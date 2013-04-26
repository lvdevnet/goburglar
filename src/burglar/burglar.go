package burglar

import (
	"strings"
	"time"
	"io"
	"fmt"
	"regexp"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"html/template"
	"appengine"
	"appengine/user"
	"appengine/urlfetch"
	"appengine/channel"
	"appengine/datastore"
	"appengine/taskqueue"
	"appengine/blobstore"
	"appengine/image"
)

func init() {
	http.HandleFunc("/", index)
	http.HandleFunc("/start", start)
	http.HandleFunc("/fetch", fetch)
	http.HandleFunc("/reset", reset)
	http.HandleFunc("/_ah/channel/connected/", connected)
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
	Blob appengine.BlobKey // BlobKey is string
}

var templates *template.Template = nil
const cookieName = "image-scrap-clientid"
const rootNode = "image-scrap-request"
const thumbnailLeaf = "image-scrap-thumbnail"
const rubbish = "rubbish"

func index(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieName)
	if err == nil && cookie != nil {
		c := appengine.NewContext(r)
		clientId := cookie.Value
		token, err := channel.Create(c, clientId)
		if error3(err, c, w) { return }
		gallery(token, c, w)
	} else {
		http.Redirect(w, r, "/static/start.html", http.StatusTemporaryRedirect)
	}
}

func connected(w http.ResponseWriter, r *http.Request) {
	clientId := r.FormValue("from")
	c := appengine.NewContext(r)
	c.Infof("connected '%v'", clientId)
	images(clientId, c)
}

func reset(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(cookieName)
	if err == nil && cookie != nil {
		c := appengine.NewContext(r)
		clientId := cookie.Value
		c.Infof("removing cookie")
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: rubbish, Expires: time.Unix(1, 0)})
		//go delete(clientId, c)
		delete(clientId, c)
	}
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func delete(clientId string, c appengine.Context) {
	c.Infof("deleting '%v'", clientId)
	iterate(clientId, c, "delete")
}

func gallery(token string, c appengine.Context, w http.ResponseWriter) {
	if templates == nil {
		var err error
		templates, err = template.ParseFiles("templates/gallery.html")
		if error3(err, c, w) { return }
	}
	w.Header().Set("Content-Type", "text/html")
	templates.ExecuteTemplate(w, "gallery.html", token)
}

func images(clientId string, c appengine.Context) {
	c.Infof("images for '%v'", clientId)
	iterate(clientId, c, "send")
}

func iterate(clientId string, c appengine.Context, op string) {
	root := datastore.NewQuery(rootNode).Filter("ClientId = ", clientId).Run(c)
	var keys []*datastore.Key
	for {
		var parent Request
		rootKey, err := root.Next(&parent)
		if err == datastore.Done {
			break
		}
		if error2(err, c) { return }
		thumbnails := datastore.NewQuery(thumbnailLeaf).Ancestor(rootKey).Run(c)
		for {
			var thumbnail Thumbnail
			key, err := thumbnails.Next(&thumbnail)
			if err == datastore.Done {
				break
			}
			if error2(err, c) { return }
			if op == "send" {
				c.Infof("pushing from db '%v'", thumbnail.ThumbnailURL)
				channel.Send(c, clientId, thumbnail.ThumbnailURL)
			} else if op == "delete" {
				c.Infof("deleting thumbnail from db '%v'", key)
				keys = append(keys, key)
				err := image.DeleteServingURL(c, thumbnail.Blob)
				error2(err, c)
			}
		}
		if op == "delete" {
			c.Infof("deleting request from db '%v'", rootKey)
			keys = append(keys, rootKey)
		}
	}
	if op == "delete" {
		err := datastore.DeleteMulti(c, keys) // deleting parent deletes children entities?
		error2(err, c)
	}
}

func start(w http.ResponseWriter, r *http.Request) {
	target := r.FormValue("target")
	c := appengine.NewContext(r)

	client := urlfetch.Client(c)
	resp, err := client.Get("http://" + target)
	if error3(err, c, w) { return }

	len := int(resp.ContentLength)
	buf := make([]byte, len)
	read, err := resp.Body.Read(buf)
	if error3(err, c, w) { return }
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
		if error3(err, c, w) && read < 8 { return }
		clientId = hex.EncodeToString(r)
	}

	token, err := channel.Create(c, clientId)
	if error3(err, c, w) { return }

	isr := Request{clientId}
	key, err := datastore.Put(c, datastore.NewIncompleteKey(c, rootNode, nil), &isr)
	if error3(err, c, w) { return }

	for _, image := range images {
		addr := string(image[1])
		if strings.Index(addr, "http") == 0 {
			task := taskqueue.NewPOSTTask("/fetch", url.Values{"clientId": {clientId}, "image": {addr}, "key": {key.Encode()}})
			_, err := taskqueue.Add(c, task, "default")
			if error3(err, c, w) { return }
		}
	}

	c.Infof("setting cookie to '%v'", clientId)
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: clientId})
	gallery(token, c, w)
}

func fetch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	clientId := r.FormValue("clientId")
	imageUrl := r.FormValue("image")
	key, err := datastore.DecodeKey(r.FormValue("key"))
	if error3(err, c, w) { return }

	client := urlfetch.Client(c)
	resp, err := client.Get(imageUrl)
	if error3(err, c, w) { return }

	blob, err := blobstore.Create(c, resp.Header.Get("Content-Type"))
	if error3(err, c, w) { return }
	written, err := io.Copy(blob, resp.Body)
	if error3(err, c, w) { return }
	if (written < 100) {
		c.Infof("image is too small %v", written)
		return
	}
	err = blob.Close()
	if error3(err, c, w) { return }

	blobkey, err := blob.Key()
	if error3(err, c, w) { return }

	thumbnailUrl, err := image.ServingURL(c, blobkey, &image.ServingURLOptions{Size: 100})
	if error3(err, c, w) { return }
	thumbnail := thumbnailUrl.String()

	c.Infof("pushing just fetched '%v'", thumbnail)
	channel.Send(c, clientId, thumbnail)

	ist := Thumbnail{thumbnail, blobkey}
	_, err = datastore.Put(c, datastore.NewIncompleteKey(c, thumbnailLeaf, key), &ist)
	if error3(err, c, w) { return }
}
