package counter

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"encoding/json"
	//"strconv"
	"time"
)

type entity struct {
	Value []string
}

var counters = make([]string, 0, 50)

const (
	counters_name                     = "prodeagle_counters"
	counter_name_prefix               = "prodeagle_counter_"
	oneWeek             time.Duration = time.Hour * 24 * 7
)

func checkCounter(c appengine.Context, name string) {
	if len(counters) == 0 {
		cache, err := memcache.Get(c, counters_name)
		if err != nil && err != memcache.ErrCacheMiss {
			c.Errorf("read counter names - memcache.Get() %s ", err)

		}
		if err == nil {
			c.Infof("read counter names - memcache hit")
			err := json.Unmarshal(cache.Value, &counters)
			if err != nil {
				c.Errorf("unmarshal counters - json.Unmarshal() %s ", err)
			}
		}
		if err == memcache.ErrCacheMiss {
			var en entity
			key := datastore.NewKey(c, counters_name, "names", 0, nil)
			err := datastore.Get(c, key, &en)
			if err != nil {
				c.Errorf("load counters - datastore.Get(%#v) %s ", key, err)
			} else {
				counters = en.Value
			}
		}
	}
	for _, c := range counters {
		if c == name {
			return
		}
	}

	counters = append(counters, name)

	ba, err := json.Marshal(counters)
	if err != nil {
		c.Errorf("marshal counters - json.Marshal(%#v) %s ", counters, err)

	}

	k := datastore.NewKey(c, counters_name, "names", 0, nil)

	en := new(entity)
	en.Value = counters
	_, dserr := datastore.Put(c, k, en)
	c.Infof("put counter names to Datastore")
	if dserr != nil {
		c.Errorf("save counters - datastore.Put(%#v) %s ", en, dserr)
	}
	counterscache := &memcache.Item{
		Key:        counters_name,
		Value:      ba,
		Expiration: oneWeek,
	}
	c.Infof("put counter names to MemCache")
	if err := memcache.Set(c, counterscache); err != nil {
		c.Errorf("put counter names to MemCache - memcache.Set(%#v) %s ", counters, err)
	}
}

func Incr(c appengine.Context, name string) {
	IncrDelta(c, name, 1)
}

func IncrDelta(c appengine.Context, name string, value int64) {
	checkCounter(c, name)
	_, _ = memcache.Increment(c, counter_name_prefix+name, value, 0)
}

type Batch struct {
	counts map[string]int64
	c      appengine.Context
}

func NewBatch(c appengine.Context) *Batch {
	return &Batch{make(map[string]int64), c}
}

func (b *Batch) Incr(name string) {
	if nil != b {
		b.IncrDelta(name, 1)
	}
}

func (b *Batch) IncrDelta(name string, value int64) {
	if nil != b {
		checkCounter(b.c, name)
		//b.c.Infof("counter " + strconv.FormatInt(b.counts[name], 10))
		b.counts[name] = b.counts[name] + 1
		//b.c.Infof("counter " + strconv.FormatInt(b.counts[name], 10))
	}
}

func (b *Batch) Commit() {
	if nil != b {
		for n, v := range b.counts {
			//b.c.Infof("batch counter " + n + " - " + strconv.FormatInt(v, 10))
			_, _ = memcache.Increment(b.c, counter_name_prefix+n, v, 0)
		}
		b.counts = make(map[string]int64)
	}
}
