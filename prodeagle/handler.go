/*
 prodegale library for appengine apps 

 just count what ever you wanted to get count and watch your app with prodegle
 see documentation on http://godoc.org/github.com/fmt-Println-MKO/prodeagle-go/prodeagle

 short example how to use:
 add a url for the prodeagle api:
 	http.HandleFunc("/prodeagle/", prodeagle.Dispatch)
 just count any counter like this:

 c is your appengine.Context
 name is the name of counter you want to inceremt by 1
 	prodeagle.Incr(c, name) 

 read the full example on 
 https://github.com/fmt-Println-MKO/prodeagle-go
*/
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

const prodeagleUrl string = "https://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v&%v=%v"
const prodeagleAuthUrl string = "https://prod-eagle.appspot.com/auth/?site=%v.appspot.com&auth=%v"

// if it is true, internal api will write debug logs, default is false
var IsDebugEnabled bool

// handels all prodealge request, configure your handler to use this function on request to http(s)://YOUR.APP.URL/prodeagle/
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

	//check if a new user should be added
	admin := r.FormValue("administrator")
	viewer := r.FormValue("viewer")

	if admin != "" || viewer != "" {
		addUser(&appId, c, w, r)
		return
	}

	//check if call made from prodeagle app, or a app administrator
	var isadmin bool
	prod := isProdeagle(&appId, c, r)
	if !prod {
		isadmin = isAdmin(c, w, r)
	}

	//if prodeagle or admin call, do harvest counters
	if IsDebugEnabled {
		c.Debugf("Dispatch - is Admin req: %v is ProdEagle req: %v", isadmin, prod)
	}
	if isadmin || prod {
		delCounter := r.FormValue("delete_counter")
		if delCounter != "" {
			deleteCounter(c, delCounter)
			return
		}
		prodcall := r.FormValue("production_call")
		sLastHarvestTime := r.FormValue("last_time")
		isProdCall := prodcall == "1"
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write(harvest(c, sLastHarvestTime, isProdCall))
		return
	}
	//note a unknown request
	c.Warningf("Dispatch - unknown request %s ", r.URL)
}

//a prodeagle request will send a auth parameter to verify
func isProdeagle(appId *string, c appengine.Context, r *http.Request) bool {
	auth := r.PostFormValue("auth")
	if auth != "" {
		secret := getAuth(appId, auth, c, r)
		if secret == auth {
			return true
		}
	}
	return false
}

// check if request to app is made from an app admin, if not notify the user
// method is call when a new user is added on prodeagle, prodeagle will redirect the user to app
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

//store new user authentication and redirect back to prodegale with stored authentication
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

//verify auth ot create/update an exsiting, create will only happen if a new user should be added
//after creatin auth will be verifyed by request to prodeagle
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
	if IsDebugEnabled {
		c.Debugf("getAuth - auth: %v updateAuth: %v", auth, updateAuth)
	}
	if updateAuth != "" && (auth == "" || auth != updateAuth) {
		client := urlfetch.Client(c)
		//make verify call to prodeagle
		url := fmt.Sprintf(prodeagleAuthUrl, *appId, updateAuth)
		resp, ferr := client.Get(url)
		if ferr != nil {
			c.Errorf("getAuth - error %v", ferr)
			return ""
		}
		//check verification result
		if IsDebugEnabled {
			c.Debugf("getAuth - status: %v", resp.Status)
		}
		if resp.Status == "200 OK" {
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			if IsDebugEnabled {
				c.Debugf("getAuth - body: %v", string(body))
			}
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

//store a verifed auth in datastore and memcached
func storeAuth(auth prodeagleAuth, c appengine.Context) {
	key := datastore.NewKey(c, "prodeagle_key", authKeyId, 0, nil)
	_, err := datastore.Put(c, key, &auth)
	if IsDebugEnabled {
		c.Debugf("storeAuth - write to datastore")
	}
	if err != nil {
		c.Errorf("storeAuth - datastore.Put(%#v) %s ", auth, err)
		return
	}
	cache := &memcache.Item{
		Key:        authKeyId,
		Value:      []byte(auth.Secret),
		Expiration: twoDays,
	}
	if IsDebugEnabled {
		c.Debugf("storeAuth - write to memcache")
	}
	if err := memcache.Set(c, cache); err != nil {
		c.Errorf("storeAuth - memcache.Set(%#v) %s ", auth, err)
	}
}
