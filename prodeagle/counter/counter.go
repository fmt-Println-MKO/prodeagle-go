package counter

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"strconv"
	"time"
)

type counterNameShard struct {
	Names     []string
	Timestamp time.Time
}

var allcounters = make([]string, 0, 50)
var lastUpdate time.Time

var lastShard int64 = 0

const (
	counters_name               = "CounterNamesShard"
	oneWeek       time.Duration = time.Hour * 24 * 7
	sixtySeconds  time.Duration = time.Second * -60
	add_success                 = 1
	add_full                    = 2
	add_fail                    = 3
	namespace                   = "prodeagle"
)

func contains(slice []string) func(elm string) bool {
	return func(elm string) bool {
		for _, e := range slice {
			if e == elm {
				return true
			}
		}
		return false
	}
}

func maxInt(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func getAllCounterNames(c appengine.Context) []string {
	counternames := make([]string, 0, 50)
	_, err := memcache.JSON.Get(c, counters_name, &counternames)
	if err != nil && err != memcache.ErrCacheMiss {
		c.Errorf("read counter names - memcache.Get() %s ", err)
	}
	c.Infof("read names from cache %#v", counternames)

	if err == memcache.ErrCacheMiss {
		q := datastore.NewQuery(counters_name)
		if !lastUpdate.IsZero() {
			q = q.Filter("Timestamp >=", lastUpdate.Add(sixtySeconds))
		}
		for t := q.Run(c); ; {
			var cns counterNameShard
			k, err := t.Next(&cns)
			c.Infof("read cns %#v", cns)
			if err == datastore.Done {
				break
			}
			if err != nil {
				c.Errorf("load counters - datastore.QueryRun(%#v) %s ", q, err)
			}
			counternames = append(counternames, cns.Names...)
			lastShard = maxInt(lastShard, k.IntID())
			lastUpdate = time.Now()
		}
	}
	c.Infof("read all names  %#v", counternames)
	return counternames
}

func createCounterNamesShardIfNew(c appengine.Context, shard int64) {
	key := datastore.NewKey(c, counters_name, "", shard, nil)
	cns := new(counterNameShard)

	err := datastore.Get(c, key, cns)
	if err == datastore.ErrNoSuchEntity {
		cns.Timestamp = time.Now()
		_, err := datastore.Put(c, key, cns)
		c.Infof("init new counter names to Datastore")
		if err != nil {
			c.Errorf("init new counters - datastore.Put(%#v) %s ", cns, err)
		}
	}
}

func addCounterNames(c appengine.Context, names []string) int {

	key := datastore.NewKey(c, counters_name, "", lastShard, nil)
	var cns counterNameShard
	counters := make([]string, 0, 50)
	result := add_fail
	err := datastore.RunInTransaction(c, func(c appengine.Context) error {
		err := datastore.Get(c, key, &cns)
		if err != nil && err != datastore.ErrNoSuchEntity {
			c.Errorf("load counters - datastore.Get(%#v) %s ", key, err)
			return err
		} else {
			counters = cns.Names
			contain := contains(counters)
			for _, n := range names {
				if !contain(n) {
					counters = append(counters, n)
				}
			}
			cns.Names = counters
			cns.Timestamp = time.Now()
			_, err = datastore.Put(c, key, &cns)
			c.Infof("put counter names to Datastore")
			if err != nil {
				if len(names) == len(cns.Names) {
					c.Errorf("save counters - datastore.Put(%#v) %s ", cns, err)
					return err
				}
				result = add_full
				return err
			}
		}
		result = add_success
		return err
	}, nil)
	if err != nil {
		if result == add_full {
			lastShard++
			createCounterNamesShardIfNew(c, lastShard)
			addCounterNames(c, names)
		} else {
			c.Errorf("Transaction failed: %v , will try to write counter names next time", err)
			return result
		}

	} else {
		c.Infof("counter names successfull written")
	}

	counterscache := &memcache.Item{
		Key:        counters_name,
		Object:     counters,
		Expiration: oneWeek,
	}
	c.Infof("put counter names to MemCache")
	if err := memcache.JSON.Set(c, counterscache); err != nil {
		c.Errorf("put counter names to MemCache - memcache.Set(%#v) %s ", counters, err)
	}
	return result
}

func calcMinute() string {
	epoch := time.Now().Unix()
	minute := epoch - (epoch % 60)
	return strconv.FormatInt(minute, 10)
}

func Incr(c appengine.Context, name string) {
	IncrDelta(c, name, 1)
}

func IncrDelta(c appengine.Context, name string, value int64) {
	incrBatch(c, map[string]int64{name: value})
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
		b.counts[name] = b.counts[name] + 1
	}
}

func (b *Batch) Commit() {
	if nil != b {
		incrBatch(b.c, b.counts)
		b.counts = make(map[string]int64)
	}
}

func incrBatch(dc appengine.Context, counters map[string]int64) {
	c, _ := appengine.Namespace(dc, namespace)
	minute := calcMinute()
	newCounters := make([]string, 0, 10)
	for n, v := range counters {
		newValue, _ := memcache.Increment(c, minute+"_"+n, v, 0)
		c.Infof("written counter %v with: %v", minute+"_"+n, v)
		if v == int64(newValue) {
			c.Infof("new counter %#v", n)
			newCounters = append(newCounters, n)
		}
	}
	if len(newCounters) > 0 {
		allcounters = getAllCounterNames(c)

		var newCounterNames = make([]string, 0, 5)

		contain := contains(allcounters)
		for _, n := range newCounters {
			c.Infof("check new counter %#v , %#v", n, allcounters)
			if !contain(n) {
				c.Infof("check new counter %#v , %#v", n, allcounters)
				newCounterNames = append(newCounterNames, n)
			}
		}

		if len(newCounterNames) > 0 {
			if lastShard == 0 {
				createCounterNamesShardIfNew(c, 1)
				lastShard = 1
			}
			c.Infof("adding new counters %#v ", newCounterNames)
			addCounterNames(c, newCounterNames)
		}
	}
}
