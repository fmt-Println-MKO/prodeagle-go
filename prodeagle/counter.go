package prodeagle

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

//checkinhg if elm is in slice
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

//returning the bigger int64 value
func maxInt(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

//getting all Counter names, also from past
//counter names are cached into memcache and stored in datastore
//datastore is main memory over all instances of app (appengine could start couple of instanzes)
func getAllCounterNames(c appengine.Context) []string {
	counternames := make([]string, 0, 50)
	_, err := memcache.JSON.Get(c, counters_name, &counternames)
	if err != nil && err != memcache.ErrCacheMiss {
		c.Errorf("read counter names - memcache.Get() %s ", err)
	}
	c.Infof("read names from cache %#v", counternames)

	if err == memcache.ErrCacheMiss {
		q := datastore.NewQuery(counters_name)
		//if it was already read from DS, just load the changes since last read per app instance
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
			//only update lastUpdate timestamp if there was at least 1 entry
			lastUpdate = time.Now()
		}
	}
	c.Infof("read all names  %#v", counternames)
	return counternames
}

//check if entry for counter names exisits, if not create a fresh new
//counter names are sharded because 
// there is a limit in max counter names (maxsize per field) on appengine
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

// adding new counter names to current CounterNamesShard 
func addCounterNames(c appengine.Context, names []string) int {

	key := datastore.NewKey(c, counters_name, "", lastShard, nil)
	var cns counterNameShard
	counters := make([]string, 0, 50)
	result := add_fail
	//add new names in transaction because there could be more then one instance who would like to store the same shard with different names
	err := datastore.RunInTransaction(c, func(c appengine.Context) error {
		//read current shard to recheck of name is still new, (could be added by an other instance in the meanwhile)
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
				// if lenght are equal it looks like a counting start, in case of errors there are no counters written, so no harvest possible
				if len(names) == len(cns.Names) {
					c.Errorf("save counters - datastore.Put(%#v) %s ", cns, err)
					return err
				}
				// if there where allready counter names written before, maybe we reached the limit of appengine, in this case write new names to new CounterNamesShard
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

	// add new counter names to cache for faster read for next time check if there are new once
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

// calc current minute
func calcMinute() string {
	epoch := time.Now().Unix()
	minute := epoch - (epoch % 60)
	return strconv.FormatInt(minute, 10)
}

//increments the counter *name" by one
func Incr(c appengine.Context, name string) {
	IncrDelta(c, name, 1)
}

//increments the counter *name" by given value
func IncrDelta(c appengine.Context, name string, value int64) {
	incrBatch(c, map[string]int64{name: value})
}

//a counting Batch to count many counters in batchmode (not one by one)
type Batch struct {
	counts map[string]int64
	c      appengine.Context
}

//create a new Batch for counting
func NewBatch(c appengine.Context) *Batch {
	return &Batch{make(map[string]int64), c}
}

//increment a counter *name* by one in Batchmode
func (b *Batch) Incr(name string) {
	if nil != b {
		b.IncrDelta(name, 1)
	}
}

//increment a counter *name* by given value in Batchmode
func (b *Batch) IncrDelta(name string, value int64) {
	if nil != b {
		b.counts[name] = b.counts[name] + 1
	}
}

//commit all counters counted in this batch and reset the batch to empty
func (b *Batch) Commit() {
	if nil != b {
		incrBatch(b.c, b.counts)
		b.counts = make(map[string]int64)
	}
}

//increments given counters by given values
//checks if new counters where added, if so, add them to CounterNames
//for faster reading /writing counter values are stored in memcached
func incrBatch(dc appengine.Context, counters map[string]int64) {
	c, _ := appengine.Namespace(dc, namespace)
	minute := calcMinute()
	newCounters := make([]string, 0, 10)
	for n, v := range counters {
		newValue, _ := memcache.Increment(c, minute+"_"+n, v, 0)
		c.Infof("written counter %v with: %v", minute+"_"+n, v)
		//if given counter delta is the same value as incremented value its possible a new counter
		if v == int64(newValue) {
			c.Infof("new counter %#v", n)
			newCounters = append(newCounters, n)
		}
	}
	if len(newCounters) > 0 {
		allcounters = getAllCounterNames(c)

		var newCounterNames = make([]string, 0, 5)

		// check which counters are realy new
		contain := contains(allcounters)
		for _, n := range newCounters {
			c.Infof("check new counter %#v , %#v", n, allcounters)
			if !contain(n) {
				c.Infof("check new counter %#v , %#v", n, allcounters)
				newCounterNames = append(newCounterNames, n)
			}
		}
		//if there where new counters, check if there is already a ConterNamesShard, of not create new one. 
		//add new counter names to Datastore
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
