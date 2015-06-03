package burglar

import (
	"appengine"
	"appengine/blobstore"
	"appengine/channel"
	"appengine/datastore"
	"appengine/image"
	"appengine/taskqueue"
	"appengine/urlfetch"
	"appengine/user"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	newappengine "google.golang.org/appengine"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

func init() {
	http.HandleFunc("/", index)
	http.HandleFunc("/start", start)
	http.HandleFunc("/fetch", fetch)
	http.HandleFunc("/reset", reset)
	http.HandleFunc("/_ah/channel/connected/", connected)
	http.HandleFunc("/cleanup", cleanup)
}

func error2(err error, c appengine.Context) bool {
	if err != nil {
		c.Errorf("%v", err.Error())
		return true
	}
	return false
}

func error3(err error, c appengine.Context, w http.ResponseWriter) bool {
	if err != nil {
		msg := err.Error()
		c.Errorf("%v", msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return true
	}
	return false
}

type Request struct {
	ClientId string
}

type Thumbnail struct {
	ThumbnailURL string
	Filename     string
}

var templates *template.Template = nil

const cookieName = "image-scrap-clientid"
const rootNode = "image-scrap-request"
const thumbnailLeaf = "image-scrap-thumbnail"
const rubbish = "rubbish"

func bucket(c appengine.Context) string {
	return appengine.AppID(c) + ".appspot.com"
}

func blobkey(filename string, c appengine.Context) (appengine.BlobKey, error) {
	return blobstore.BlobKeyForFile(c, fmt.Sprintf("/gs/%s/%s", bucket(c), filename))
}

func storagectx(c appengine.Context, r *http.Request) context.Context {
	nc := newappengine.NewContext(r)
	h := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(nc, storage.ScopeFullControl),
			Base:   &urlfetch.Transport{Context: c},
		},
	}
	return cloud.WithContext(nc, appengine.AppID(c), h)
}

func index(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieName)
	if err == nil && cookie != nil {
		c := appengine.NewContext(r)
		clientId := cookie.Value
		token, err := channel.Create(c, clientId)
		if error3(err, c, w) {
			return
		}
		gallery(token, c, w)
	} else {
		http.Redirect(w, r, "/static/start.html", http.StatusTemporaryRedirect)
	}
}

func connected(w http.ResponseWriter, r *http.Request) {
	clientId := r.FormValue("from")
	c := appengine.NewContext(r)
	cc := storagectx(c, r)
	c.Infof("connected client '%v'", clientId)
	images(clientId, c, cc)
}

func reset(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieName)
	if err == nil && cookie != nil {
		c := appengine.NewContext(r)
		cc := storagectx(c, r)
		clientId := cookie.Value
		c.Infof("removing cookie of '%v'", clientId)
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: rubbish, Expires: time.Unix(1, 0)})
		delete(clientId, c, cc)
	}
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func delete(clientId string, c appengine.Context, cc context.Context) {
	c.Infof("deleting client '%v'", clientId)
	iterate(&clientId, c, cc, "delete")
}

func gallery(token string, c appengine.Context, w http.ResponseWriter) {
	if templates == nil {
		var err error
		templates, err = template.ParseFiles("templates/gallery.html")
		if error3(err, c, w) {
			return
		}
	}
	w.Header().Set("Content-Type", "text/html")
	templates.ExecuteTemplate(w, "gallery.html", token)
}

func images(clientId string, c appengine.Context, cc context.Context) {
	c.Infof("images for '%v'", clientId)
	iterate(&clientId, c, cc, "send")
}

func cleanup(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cc := storagectx(c, r)
	c.Infof("datastore and cloudstorage cleanup")
	iterate(nil, c, cc, "delete")
}

func iterate(clientId *string, c appengine.Context, cc context.Context, op string) {
	var rkeys []*datastore.Key
	var allKeys []*datastore.Key
	var allBlobs []string
	q := datastore.NewQuery(rootNode)
	if clientId != nil { // not cleanup
		q = q.Filter("ClientId = ", *clientId)
	}
	root := q.Run(c)
	for {
		var parent Request
		key, err := root.Next(&parent)
		if err == datastore.Done {
			break
		}
		if error2(err, c) {
			return
		}
		rkeys = append(rkeys, key)
	}
	if clientId == nil { // cleanup
		allKeys = append(allKeys, rkeys...)
		rkeys = []*datastore.Key{nil}
	}
	for _, rkey := range rkeys {
		err, tkeys, blobs := iterate2(clientId, rkey, c, op)
		if op == "delete" {
			if rkey != nil {
				c.Infof("deleting request from db '%v'", rkey)
			} else {
				c.Infof("deleting all requests from db")
			}
			allKeys = append(allKeys, *tkeys...)
			allBlobs = append(allBlobs, *blobs...)
		}
		if error2(err, c) {
			break
		}
	}
	if op == "delete" {
		bkt := bucket(c)
		for _, filename := range allBlobs {
			err := storage.DeleteObject(cc, bkt, filename)
			error2(err, c)
		}
		if clientId != nil {
			allKeys = append(allKeys, rkeys...)
		}
		err := datastore.DeleteMulti(c, allKeys) // deleting parent deletes ancestors? -- No
		error2(err, c)
	}
}

func iterate2(clientId *string, rkey *datastore.Key, c appengine.Context, op string) (error, *[]*datastore.Key, *[]string) {
	var keys []*datastore.Key
	var blobs []string
	q := datastore.NewQuery(thumbnailLeaf)
	if rkey != nil {
		q = q.Ancestor(rkey)
	}
	thumbnails := q.Run(c)
	for {
		var thumbnail Thumbnail
		key, err := thumbnails.Next(&thumbnail)
		if err == datastore.Done {
			break
		}
		if error2(err, c) {
			return err, &keys, &blobs
		}
		if op == "send" {
			c.Debugf("pushing from db '%v'", thumbnail.ThumbnailURL)
			channel.Send(c, *clientId, thumbnail.ThumbnailURL)
		} else if op == "delete" {
			c.Infof("deleting thumbnail from db '%v'", key)
			blobkey, err := blobkey(thumbnail.Filename, c)
			if !error2(err, c) {
				err := image.DeleteServingURL(c, blobkey)
				error2(err, c)
			}
			keys = append(keys, key)
			blobs = append(blobs, thumbnail.Filename)
		}
	}
	return nil, &keys, &blobs
}

func start(w http.ResponseWriter, r *http.Request) {
	target := r.FormValue("target")
	c := appengine.NewContext(r)

	client := urlfetch.Client(c)
	resp, err := client.Get("http://" + target)
	if error3(err, c, w) {
		return
	}

	len := int(resp.ContentLength)
	buf := make([]byte, len)
	read, err := resp.Body.Read(buf)
	if error3(err, c, w) {
		return
	}
	if read != len {
		http.Error(w, fmt.Sprintf("Target page Content-Length is %v but read %v bytes", len, read), http.StatusInternalServerError)
		return
	}

	rx, _ := regexp.Compile("<img .*? src=\"(.*?)\"")
	images := rx.FindAllSubmatch(buf, len)
	if images == nil {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "HTTP GET returned status %v\nNo images found\n\n", resp.Status)
		w.Write(buf)
		return
	}

	var clientId string
	u := user.Current(c)
	if u != nil {
		clientId = u.Email
	} else {
		len := 16
		r := make([]byte, len)
		read, err := rand.Read(r)
		if error3(err, c, w) && read < 8 {
			return
		}
		clientId = hex.EncodeToString(r)
	}

	token, err := channel.Create(c, clientId)
	if error3(err, c, w) {
		return
	}

	isr := Request{clientId}
	key, err := datastore.Put(c, datastore.NewIncompleteKey(c, rootNode, nil), &isr)
	if error3(err, c, w) {
		return
	}

	seen := make(map[string]struct{})
	for _, image := range images {
		addr := string(image[1])
		if strings.Index(addr, "http") == 0 {
			if _, ok := seen[addr]; !ok {
				seen[addr] = struct{}{}
				task := taskqueue.NewPOSTTask("/fetch", url.Values{"clientId": {clientId}, "image": {addr}, "key": {key.Encode()}})
				_, err := taskqueue.Add(c, task, "default")
				if error3(err, c, w) {
					return
				}
			}
		}
	}

	c.Infof("setting cookie to '%v'", clientId)
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: clientId})
	gallery(token, c, w)
}

func fetch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cc := storagectx(c, r)

	clientId := r.FormValue("clientId")
	imageUrl := r.FormValue("image")
	c.Debugf("fetching '%v'", imageUrl)
	key, err := datastore.DecodeKey(r.FormValue("key"))
	if error3(err, c, w) {
		return
	}

	client := urlfetch.Client(c)
	resp, err := client.Get(imageUrl)
	if error3(err, c, w) {
		return
	}
	c.Debugf("downloaded '%v'", imageUrl)

	bkt := bucket(c)
	filename := fmt.Sprintf("%x", md5.Sum([]byte(imageUrl)))
	c.Debugf("creating blob %v/%v", bkt, filename)
	blob := storage.NewWriter(cc, bkt, filename)
	blob.ContentType = resp.Header.Get("Content-Type")
	blob.ACL = []storage.ACLRule{{storage.AllUsers, storage.RoleReader}}
	written, err := io.Copy(blob, resp.Body)
	if error3(err, c, w) {
		return
	}
	err = blob.Close()
	if error3(err, c, w) {
		return
	}
	if written < 100 {
		c.Infof("image is too small: %v bytes; deleting", written)
		storage.DeleteObject(cc, bkt, filename)
		return
	}

	blobkey, err := blobkey(filename, c)
	if error3(err, c, w) {
		return
	}

	thumbnailUrl, err := image.ServingURL(c, blobkey, &image.ServingURLOptions{Size: 100})
	if error3(err, c, w) {
		return
	}
	thumbnail := thumbnailUrl.String()

	c.Debugf("pushing to client '%v'", thumbnail)
	channel.Send(c, clientId, thumbnail)

	ist := Thumbnail{thumbnail, filename}
	_, err = datastore.Put(c, datastore.NewIncompleteKey(c, thumbnailLeaf, key), &ist)
	if error3(err, c, w) {
		return
	}
}
