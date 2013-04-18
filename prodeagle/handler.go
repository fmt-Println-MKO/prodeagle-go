package prodeagle

import (
	"appengine"
	"fmt"
	"net/http"
	"prodeagle/harvest"
)

func init() {
	http.HandleFunc("/prodeagle/", dispatch)
}

func dispatch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	prodcall := r.FormValue("production_call")
	if prodcall != "" {
		harvest.Harvest(w, r)
		return
	}
	admin := r.FormValue("administrator")
	viewer := r.FormValue("viewer")
	if admin != "" || viewer != "" {
		login(w, r)
	}
	c.Errorf("dispatch -unknown request %s ", r.URL)
	http.Error(w, "unknown request", http.StatusBadRequest)
	return

}

func login(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "implement login! ")
}
