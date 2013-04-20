package prodeagle

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"appengine/urlfetch"
	"appengine/user"
	"fmt"
	"net/http"
	"prodeagle/harvest"
	"strings"
	"time"
)

const twoDays time.Duration = time.Hour * 24 * 2

func init() {
	http.HandleFunc("/prodeagle/", dispatch)
}

func dispatch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	prodcall := r.FormValue("production_call")
	if prodcall != "" {
		harvest.Harvest(c, w, r)
		return
	}
	admin := r.FormValue("administrator")
	viewer := r.FormValue("viewer")
	if admin != "" || viewer != "" {
		login(c, w, r)
		return
	}
	c.Errorf("dispatch -unknown request %s ", r.URL)
	http.Error(w, "unknown request", http.StatusBadRequest)
	return

}

const prodeagleUrl string = "http://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v&%v=%v"
const prodeagleAuthUrl string = "http://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v"

func login(c appengine.Context, w http.ResponseWriter, r *http.Request) {

	u := user.Current(c)
	if u == nil {
		url, err := user.LoginURL(c, r.URL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", url)
		w.WriteHeader(http.StatusFound)
		return
	}
	if u.Admin {
		appId := appengine.AppID(c)
		c.Infof("appid: %v", appId)
		if strings.Contains(appId, ":") {
			appId = strings.Split(appId, ":")[1]
		}
		email := r.FormValue("administrator")
		rtype := "administrator"
		if email == "" {
			email = r.FormValue("viewer")
			rtype = "viewer"
		}
		token, err := getAuthToken(&appId, c, r)
		if err == nil {
			w.Header().Set("Location", fmt.Sprintf(prodeagleUrl, appId, token, rtype, email))
			w.WriteHeader(http.StatusFound)
		}
		return

	}
	//TODO add logout URL and note for user to login as admin
	http.Error(w, "unauthorized request", http.StatusUnauthorized)
}

const masterKeyName string = "prodeagle_master"

func getAuthToken(appId *string, c appengine.Context, r *http.Request) (token string, _ error) {
	token = "NONE"
	cache, err := memcache.Get(c, masterKeyName)
	if err == nil {
		c.Infof("getAuthToken - memcache hit")
		token = string(cache.Value)
		return
	}
	key := datastore.NewKey(c, "prodeagle", "master", 0, nil)
	err = datastore.Get(c, key, &token)
	if err == nil {
		putTokenToMemCache(token, c)
		return
	}

	client := urlfetch.Client(c)
	resp, err := client.Get(fmt.Sprintf(prodeagleUrl, appId, token))
	if err != nil {
		//http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.Infof("UrlFetch Status: %v", resp.Status)
	return
}

func putTokenToMemCache(token string, c appengine.Context) {
	cache := &memcache.Item{
		Key:        masterKeyName,
		Value:      []byte(token),
		Expiration: twoDays,
	}
	c.Infof("putTokenToMemCache - write to memcache")
	if err := memcache.Set(c, cache); err != nil {
		c.Errorf("putTokenToMemCache - memcache.Set(%#v) %s ", token, err)
		//http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
