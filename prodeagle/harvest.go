package prodeagle

import (
	"appengine"
	"appengine/memcache"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

/**
{
  "all_data_inaccurate": false, // I will explain this bellow
  "counters": {                 // a dictionary of all counters
    "foo": {                    // foo is the counter name
      "1319752200": 3           // the first number is the epoch modulo 60 and
                                // the second number is the delta of the counter
    },
    "bar": {                    // this is another counter...
      "1319752140": 2           // which was incremented at two different minutes
      "1319752200": 161
    },
  }, 
  "ms_of_data_lost": 303,       // i will explain bellow
  "time": 1319752200,           // the current time on the server modulo 60
  "version": 1.0                // the version of the protocol. so far always 1.0
}
**/

type counterHarvest struct {
	All_data_inaccurate bool                        `json:"all_data_inaccurate"`
	Ms_of_data_lost     int64                       `json:"ms_of_data_lost"`
	Time                int64                       `json:"time"`
	Version             string                      `json:"version"`
	Counters            map[string]map[string]int64 `json:"counters"`
}

const (
	max_look_back_time  int64  = 1000 * 60 * 2 //1000 * 60 * 60 = 1h
	max_clock_skew      int64  = 60
	min_slot_size       int64  = 60
	sep                 string = "_"
	version             string = "1.0"
	max_memcache_server int    = 1024
)

func Harvest(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now().Unix()
	dc := appengine.NewContext(r)
	c, _ := appengine.Namespace(dc, namespace)
	sLastHarvestTime := r.FormValue("last_time")
	lastHarvestTime := time.Now().Unix()
	if sLastHarvestTime != "" {
		lastHarvestTime, _ = strconv.ParseInt(sLastHarvestTime, 10, 64)
	}
	currentTime, _ := strconv.ParseInt(calcMinute(), 10, 64)
	counterNames := getAllCounterNames(c)
	slot := lastHarvestTime - max_look_back_time
	slot = slot - (slot % 60)
	counters := make(map[string]map[string]int64)
	for _, name := range counterNames {
		counters[name] = make(map[string]int64)
	}
	cmNames := createMemCacheNames(counterNames)

	c.Infof("sLastharvesttime is: " + sLastHarvestTime)
	c.Infof("lastharvesttime is: " + strconv.FormatInt(lastHarvestTime, 10))
	c.Infof("slot is: " + strconv.FormatInt(slot, 10))
	for slot <= currentTime {
		sslot := strconv.FormatInt(slot, 10)
		//c.Infof("sslot is: " + sslot)
		items, _ := memcache.GetMulti(c, cmNames(sslot))
		for key, item := range items {
			buf := bytes.NewBuffer(item.Value)
			cn := strings.Split(key, sep)
			counters[cn[1]][cn[0]], _ = strconv.ParseInt(buf.String(), 10, 64)
		}
		slot = slot + min_slot_size
	}
	timelost := time.Now().Unix() - startTime
	harvest := counterHarvest{Counters: counters, Ms_of_data_lost: timelost, Time: currentTime, Version: version}
	b, err := json.Marshal(harvest)
	if err != nil {
		c.Errorf("Harvest - json.Marshal(%#v) %s ", counters, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val := strconv.Itoa(readFlushChecks(c, lastHarvestTime))
	c.Infof("flushes: " + val)
	w.Write(b)
}

func writeFlushChecks(c appengine.Context, currentTime int64) {
	items := make([]*memcache.Item, max_memcache_server)
	for i := range items {
		key := strconv.Itoa(i) + sep + strconv.FormatInt(currentTime, 10)
		value := make([]byte, 20, 50)
		binary.PutVarint(value, currentTime)
		items[i] = &memcache.Item{
			Key:   key,
			Value: value,
		}
	}
	memcache.SetMulti(c, items)
}

func readFlushChecks(c appengine.Context, lastHarvestTime int64) int {
	keys := make([]string, max_memcache_server)
	for i := range keys {
		keys[i] = strconv.Itoa(i) + sep + strconv.FormatInt(lastHarvestTime, 10)
	}
	items, _ := memcache.GetMulti(c, keys)
	return len(items)
}

func flushMemCache(c appengine.Context) {
	memcache.Flush(c)
}

func createMemCacheNames(cnames []string) func(slot string) []string {
	return func(slot string) []string {
		l := len(cnames)
		slotnames := make([]string, l, l)
		for i, name := range cnames {
			slotnames[i] = slot + "_" + name

		}
		return slotnames
	}
}
