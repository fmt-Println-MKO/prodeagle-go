package main

// Licensed: Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)
// http://creativecommons.org/licenses/by-sa/3.0/

import (
	"appengine"
	"fmt"
	"net/http"
	"prodeagle"
	"strconv"
)

func init() {
	prodeagle.IsDebugEnabled = true
	http.HandleFunc("/prodeagle/", prodeagle.Dispatch)
	http.HandleFunc("/batch/commit/", testComitBatchCounter)
	http.HandleFunc("/batch/", testBatchCounter)
	http.HandleFunc("/", testCounter)

}

func testCounter(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	name := r.FormValue("n")
	if name != "" {
		svalue := r.FormValue("v")
		var delta int64 = 1
		if svalue != "" {
			delta, _ = strconv.ParseInt(svalue, 10, 64)
		}
		if delta > 1 {
			prodeagle.IncrDelta(c, name, delta)
		} else {
			prodeagle.Incr(c, name)
		}
		fmt.Fprintf(w, "counter: %v with value: %v added", name, delta)
	} else {
		fmt.Fprint(w, "no counter given, use?n=CounterName and optional v=Delta ")
	}
}

var b *prodeagle.Batch

func testBatchCounter(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	name := r.FormValue("n")
	if name != "" {
		svalue := r.FormValue("v")
		if nil == b {
			b = prodeagle.NewBatch(c)
		}
		var delta int64 = 1
		if svalue != "" {
			delta, _ = strconv.ParseInt(svalue, 10, 64)
		}
		if delta > 1 {
			b.IncrDelta(name, delta)
		} else {
			b.Incr(name)
		}
		fmt.Fprintf(w, "batch counter: %v with value: %v added", name, delta)
	} else {
		fmt.Fprint(w, "no counter given, use?n=CounterName and optional v=Delta ")
	}
}

func testComitBatchCounter(w http.ResponseWriter, r *http.Request) {
	b.Commit()
	fmt.Fprint(w, "batch counters commited")
}
