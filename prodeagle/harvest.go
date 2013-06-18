package prodeagle

import (
	"appengine"
	"appengine/memcache"
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

type counterHarvest struct {
	All_data_inaccurate bool                        `json:"all_data_inaccurate"`
	Counters            map[string]map[string]int64 `json:"counters"`
	Ms_of_data_lost     int64                       `json:"ms_of_data_lost"`
	Time                int64                       `json:"time"`
	Version             float32                     `json:"version"`
}

const (
	max_look_back_time  int64   = 3600 //1h
	max_clock_skew      int64   = 60
	min_slot_size       int64   = 60
	sep                 string  = "_"
	version             float32 = 1.0
	max_memcache_server int     = 1024
	lost_data_check     string  = "lost_data_check"
)

//harvest stored counters
//if last_time is not set try to harvest counters from last hour
func harvest(c appengine.Context, sLastHarvestTime string, prodCall bool) []byte {
	startTime := time.Now()
	//check if there could be lost counters since last harvest
	all_data_inaccurate := wasDataLost(c, true)
	lastHarvestTime := time.Now().Unix()
	if sLastHarvestTime != "" {
		lastHarvestTime, _ = strconv.ParseInt(sLastHarvestTime, 10, 64)
		lastHarvestTime = lastHarvestTime - max_look_back_time
	}
	currentTime := calcMinute(time.Now().Unix())
	counterNames := getAllCounterNames(c)
	slot := calcMinute(lastHarvestTime)
	counters := make(map[string]map[string]int64)
	for _, name := range counterNames {
		counters[name] = make(map[string]int64)
	}
	cmNames := createMemCacheNames(counterNames)
	var keys []string
	if prodCall {
		keys = make([]string, len(counters_name)*2, len(counters_name)*3)
	}
	c.Infof("lastharvesttime is: %v", lastHarvestTime)
	c.Infof("currenttime is: %v", currentTime)
	c.Infof("slot is: %v", slot)
	for slot <= currentTime {
		sslot := strconv.FormatInt(slot, 10)
		items, _ := memcache.GetMulti(c, cmNames(sslot))
		for key, item := range items {
			buf := bytes.NewBuffer(item.Value)
			if prodCall {
				keys = append(keys, key)
			}
			cn := strings.Split(key, sep)
			counters[cn[1]][cn[0]], _ = strconv.ParseInt(buf.String(), 10, 64)
		}
		slot = slot + min_slot_size
	}
	timelost := (time.Now().Sub(startTime).Nanoseconds() / time.Millisecond.Nanoseconds())
	//check if there could be lost counters during harvest, do not reset counters because there could be also lost counters for next harvest
	all_data_inaccurate = all_data_inaccurate || wasDataLost(c, false)
	harvest := counterHarvest{All_data_inaccurate: all_data_inaccurate, Counters: counters, Ms_of_data_lost: timelost, Time: currentTime, Version: version}
	b, err := json.Marshal(harvest)
	if err != nil {
		c.Errorf("Harvest - json.Marshal(%#v) %s ", counters, err)
		return nil
	}
	if prodCall {
		//delete harvested counters
		c.Infof("deleting all counters")
		memcache.DeleteMulti(c, keys)
	}
	return b
}

func wasDataLost(c appengine.Context, reset bool) bool {

	keys := make([]string, max_memcache_server)
	for i := range keys {
		keys[i] = lost_data_check + strconv.Itoa(i)
	}
	result := false
	if reset {
		for _, key := range keys {
			newValue, _ := memcache.Increment(c, key, 1, 0)
			if newValue == 1 {
				result = true
			}
		}
	} else {
		items, _ := memcache.GetMulti(c, keys)
		result = max_memcache_server != len(items)
	}
	return result
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
