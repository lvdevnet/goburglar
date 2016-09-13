package burglar

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/channel"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/image"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/urlfetch"
	"google.golang.org/appengine/user"
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

func error2(err error, c context.Context) bool {
	if err != nil {
		log.Errorf(c, "%v", err.Error())
		return true
	}
	return false
}

func error3(err error, c context.Context, w http.ResponseWriter) bool {
	if err != nil {
		msg := err.Error()
		log.Errorf(c, "%v", msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return true
	}
	return false
}

type Request struct {
	ClientId string
}

type Thumbnail struct {
	ThumbnailURL string `datastore:",noindex"`
	Filename     string `datastore:",noindex"`
}

var templates *template.Template = nil

const cookieName = "image-scrap-clientid"
const rootNode = "image-scrap-request"
const thumbnailLeaf = "image-scrap-thumbnail"
const rubbish = "rubbish"

func bucket(c context.Context) string {
	return appengine.AppID(c) + ".appspot.com"
}

func blobkey(filename string, c context.Context) (appengine.BlobKey, error) {
	return blobstore.BlobKeyForFile(c, fmt.Sprintf("/gs/%s/%s", bucket(c), filename))
}

func storagectx(c context.Context, r *http.Request) context.Context {
	nc := appengine.NewContext(r)
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
	log.Infof(c, "connected client '%v'", clientId)
	images(clientId, c, cc)
}

func reset(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cookie, err := r.Cookie(cookieName)
	if err == nil && cookie != nil {
		clientId := cookie.Value
		log.Infof(c, "removing cookie of '%v'", clientId)
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: rubbish, Expires: time.Unix(1, 0)})
		task := taskqueue.NewPOSTTask("/reset", url.Values{"clientId": {clientId}})
		_, err := taskqueue.Add(c, task, "")
		if error3(err, c, w) {
			cc := storagectx(c, r)
			delete(clientId, c, cc)
		}
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
	} else if clientId := r.FormValue("clientId"); clientId != "" {
		cc := storagectx(c, r)
		delete(clientId, c, cc)
	}
}

func delete(clientId string, c context.Context, cc context.Context) {
	log.Infof(c, "deleting client '%v'", clientId)
	iterate(&clientId, c, cc, "delete")
}

func gallery(token string, c context.Context, w http.ResponseWriter) {
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

func images(clientId string, c context.Context, cc context.Context) {
	log.Infof(c, "images for '%v'", clientId)
	iterate(&clientId, c, cc, "send")
}

func cleanup(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cc := storagectx(c, r)
	log.Infof(c, "datastore and cloudstorage cleanup")
	iterate(nil, c, cc, "delete")
}

func iterate(clientId *string, c context.Context, cc context.Context, op string) {
	var rkeys []*datastore.Key
	var allKeys []*datastore.Key
	var allBlobs []string
	q := datastore.NewQuery(rootNode).KeysOnly()
	if clientId != nil { // not cleanup
		q = q.Filter("ClientId = ", *clientId)
	}
	root := q.Run(c)
	for {
		key, err := root.Next(nil)
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
				log.Infof(c, "deleting request from db '%v'", rkey)
			} else {
				log.Infof(c, "deleting all requests from db")
			}
			allKeys = append(allKeys, *tkeys...)
			allBlobs = append(allBlobs, *blobs...)
		}
		if error2(err, c) {
			break
		}
	}
	if op == "delete" {
		bucketName := bucket(c)
		gcs, err := storage.NewClient(cc)
		if error2(err, c) {
			return
		}
		bucket := gcs.Bucket(bucketName)
		for _, objName := range allBlobs {
			obj := bucket.Object(objName)
			err := obj.Delete(cc)
			error2(err, c)
		}
		if clientId != nil {
			allKeys = append(allKeys, rkeys...)
		}
		err = datastore.DeleteMulti(c, allKeys) // deleting parent deletes ancestors? -- No
		error2(err, c)
	}
}

func iterate2(clientId *string, rkey *datastore.Key, c context.Context, op string) (error, *[]*datastore.Key, *[]string) {
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
			log.Debugf(c, "pushing from db '%v'", thumbnail.ThumbnailURL)
			channel.Send(c, *clientId, thumbnail.ThumbnailURL)
		} else if op == "delete" {
			log.Infof(c, "deleting thumbnail from db '%v'", key)
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

func startsWith(str string, search ...string) bool {
	for _, s := range search {
		if strings.Index(str, s) == 0 {
			return true
		}
	}
	return false
}

var _imgRx = regexp.MustCompile(`<img .*?src="(.*?)"`)

func start(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	target := r.FormValue("target")
	if !startsWith(target, "http:", "https:") {
		target = "http://" + target
	}
	targetUrl, err := url.Parse(target)
	if error3(err, c, w) {
		return
	}
	host := fmt.Sprintf("%s://%s", targetUrl.Scheme, targetUrl.Host)

	client := urlfetch.Client(c)
	resp, err := client.Get(target)
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

	images := _imgRx.FindAllSubmatch(buf, len)
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
		loc := string(image[1])
		if !startsWith(loc, "http:", "https:") {
			if startsWith(loc, "//") {
				loc = fmt.Sprintf("%s:%s", targetUrl.Scheme, loc)
			} else if startsWith(loc, "/") {
				loc = host + loc
			} else {
				continue // not processing relative urls
			}
		}
		if _, ok := seen[loc]; !ok {
			seen[loc] = struct{}{}
			task := taskqueue.NewPOSTTask("/fetch", url.Values{"clientId": {clientId}, "image": {loc}, "key": {key.Encode()}})
			_, err := taskqueue.Add(c, task, "")
			if error3(err, c, w) {
				return
			}
		}
	}

	log.Infof(c, "setting cookie to '%v'", clientId)
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: clientId})
	gallery(token, c, w)
}

func fetch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cc := storagectx(c, r)

	clientId := r.FormValue("clientId")
	imageUrl := r.FormValue("image")
	log.Debugf(c, "fetching '%v'", imageUrl)
	key, err := datastore.DecodeKey(r.FormValue("key"))
	if error3(err, c, w) {
		return
	}

	client := urlfetch.Client(c)
	resp, err := client.Get(imageUrl)
	if error3(err, c, w) {
		return
	}
	log.Debugf(c, "downloaded '%v'", imageUrl)

	bucketName := bucket(c)
	objName := fmt.Sprintf("%x", md5.Sum([]byte(imageUrl)))
	log.Debugf(c, "creating image GCS object %v/%v", bucketName, objName)
	gcs, err := storage.NewClient(cc)
	if error3(err, c, w) {
		return
	}
	defer gcs.Close()
	bucket := gcs.Bucket(bucketName)
	obj := bucket.Object(objName)
	blob := obj.NewWriter(cc)
	blob.ContentType = resp.Header.Get("Content-Type")
	blob.ACL = []storage.ACLRule{{storage.AllUsers, storage.RoleReader}}
	log.Debugf(c, "writting GCS object %v/%v", bucketName, objName)
	written, err := io.Copy(blob, resp.Body)
	if error3(err, c, w) {
		log.Infof(c, "image GCS object write failed, written %d bytes: deleting GCS object", written)
		err = obj.Delete(cc)
		error3(err, c, w)
		return
	}
	err = blob.Close()
	if error3(err, c, w) {
		log.Infof(c, "image GCS object close failed: deleting GCS object")
		err = obj.Delete(cc)
		error3(err, c, w)
		return
	}
	if written < 100 {
		log.Infof(c, "image is too small: %d bytes; deleting GCS object", written)
		err = obj.Delete(cc)
		error3(err, c, w)
		return
	}

	blobkey, err := blobkey(objName, c)
	if error3(err, c, w) {
		return
	}

	thumbnailUrl, err := image.ServingURL(c, blobkey, &image.ServingURLOptions{Size: 100})
	if error3(err, c, w) {
		log.Infof(c, "image service failed: deleting GCS object")
		err = obj.Delete(cc)
		error3(err, c, w)
		return
	}
	thumbnail := thumbnailUrl.String()

	log.Debugf(c, "pushing to client '%v'", thumbnail)
	channel.Send(c, clientId, thumbnail)

	ist := Thumbnail{thumbnail, objName}
	_, err = datastore.Put(c, datastore.NewIncompleteKey(c, thumbnailLeaf, key), &ist)
	if error3(err, c, w) {
		return
	}
}
