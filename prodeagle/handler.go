package prodeagle

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"appengine/urlfetch"
	"appengine/user"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const twoDays time.Duration = time.Hour * 24 * 2

func init() {
	//TODO remove test later
	http.HandleFunc("/prodeagle/testing/batch/commit/", testComitBatchCounter)
	http.HandleFunc("/prodeagle/testing/batch/", testBatchCounter)
	http.HandleFunc("/prodeagle/testing/", testCounter)
	http.HandleFunc("/prodeagle/", Dispatch)
}

const prodeagleUrl string = "https://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v&%v=%v"
const prodeagleAuthUrl string = "https://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v"

func Dispatch(w http.ResponseWriter, r *http.Request) {

	dc := appengine.NewContext(r)
	c, _ := appengine.Namespace(dc, namespace)

	method := r.Method
	if method == "HEAD" {
		return
	}

	appId := appengine.AppID(c)
	if strings.Contains(appId, ":") {
		appId = strings.Split(appId, ":")[1]
	}

	admin := r.FormValue("administrator")
	viewer := r.FormValue("viewer")

	if admin != "" || viewer != "" {
		addUser(&appId, c, w, r)
		return
	}

	var isadmin bool
	prod := isProdeagle(&appId, c, r)
	if !prod {
		isadmin = isAdmin(c, w, r)
	}

	c.Debugf("admin: %v prod: %v", isadmin, prod)
	if isadmin || prod {
		prodcall := r.FormValue("production_call")
		sLastHarvestTime := r.FormValue("last_time")
		isProdCall := prodcall == "1"
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write(Harvest(c, sLastHarvestTime, isProdCall))
		return
	}
	c.Errorf("Dispatch -unknown request %s ", r.URL)
}

func isProdeagle(appId *string, c appengine.Context, r *http.Request) bool {
	auth := r.FormValue("auth")
	if auth != "" {
		secret := getAuth(appId, auth, c, r)
		if secret == auth {
			return true
		}
	}
	return false
}

func isAdmin(c appengine.Context, w http.ResponseWriter, r *http.Request) bool {
	u := user.Current(c)
	if u == nil {
		url, err := user.LoginURL(c, r.URL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false
		}
		w.Header().Set("Location", url)
		w.WriteHeader(http.StatusFound)
		return false
	}
	if u.Admin {
		return true
	}
	url, _ := user.LogoutURL(c, r.URL.String())
	fmt.Fprint(w, "Please login with an administrator account. <a href='%s'>Logout</a>", url)
	return false
}

func addUser(appId *string, c appengine.Context, w http.ResponseWriter, r *http.Request) {
	if isAdmin(c, w, r) {
		email := r.FormValue("administrator")
		rtype := "administrator"
		if email == "" {
			email = r.FormValue("viewer")
			rtype = "viewer"
		}
		auth := getAuth(appId, "", c, r)
		if auth == "" {
			fmt.Fprint(w, "ProdEagle hasn't set your secret yet. Please visit prodeagle.com and register your website.")
			return
		}
		w.Header().Set("Location", fmt.Sprintf(prodeagleUrl, *appId, auth, rtype, email))
		w.WriteHeader(http.StatusFound)
		return
	}
}

const authKeyId string = "prodeagle_auth"

type prodeagleAuth struct {
	Secret string
}

func getAuth(appId *string, updateAuth string, c appengine.Context, r *http.Request) string {
	prodauth := new(prodeagleAuth)
	var auth string
	cache, err := memcache.Get(c, authKeyId)
	if err == nil {
		auth = string(cache.Value)
	} else {
		key := datastore.NewKey(c, "prodeagle_key", authKeyId, 0, nil)
		derr := datastore.Get(c, key, &prodauth)
		if derr == nil {
			auth = prodauth.Secret
		}
	}
	c.Debugf("getAuth - auth: %v updateAuth: %v", auth, updateAuth)
	if updateAuth != "" && (auth == "" || auth != updateAuth) {
		client := urlfetch.Client(c)
		url := fmt.Sprintf(prodeagleAuthUrl, *appId, updateAuth)
		resp, ferr := client.Get(url)
		if ferr != nil {
			c.Errorf("getAuth error %v", ferr)
			return ""
		}
		c.Debugf("getAuth - status: %v", resp.Status)
		if resp.Status == "200 OK" {
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			c.Debugf("getAuth - body: %v", string(body))
			if string(body) == "OK" {
				prodauth.Secret = updateAuth
				storeAuth(*prodauth, c)
				return updateAuth
			}
		}
	}
	if auth != "" {
		return auth
	}
	return ""
}

func storeAuth(auth prodeagleAuth, c appengine.Context) {
	key := datastore.NewKey(c, "prodeagle_key", authKeyId, 0, nil)
	_, err := datastore.Put(c, key, &auth)
	c.Debugf("storeAuth - write to datastore")
	if err != nil {
		c.Errorf("storeAuth - datastore.Put(%#v) %s ", auth, err)
		return
	}
	cache := &memcache.Item{
		Key:        authKeyId,
		Value:      []byte(auth.Secret),
		Expiration: twoDays,
	}
	c.Debugf("storeAuth - write to memcache")
	if err := memcache.Set(c, cache); err != nil {
		c.Errorf("storeAuth - memcache.Set(%#v) %s ", auth, err)
	}
}

//TODO remove test later
func testCounter(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	name := r.FormValue("name")
	//IncrDelta(c, name, 5)
	Incr(c, name)
	fmt.Fprint(w, "counter written")
}

var b *Batch

//TODO remove test later
func testBatchCounter(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	name := r.FormValue("name")
	if nil == b {
		b = NewBatch(c)
	}
	b.Incr(name)
	fmt.Fprint(w, "batch counter written")
}

//TODO remove test later
func testComitBatchCounter(w http.ResponseWriter, r *http.Request) {
	b.Commit()
	fmt.Fprint(w, "batch counter commited written")
}
