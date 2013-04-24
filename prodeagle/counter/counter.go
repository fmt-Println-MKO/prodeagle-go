package counter

import ()

var counters := make([]string,20,50)

func isNewCounter(name string) bool {
	for c :=range slice {
		if c == name {
			return true
		}
	}
	return false
}

func Incr(name string) {
	Incr(name, 1)
}

func Incr(name string, int value) {
	if isNewCounter(name) {
		append(counter, name)
	}
}

type Batch struct {
	counts := map[string] int
}

func (b *Batch) Incr(name string) {
	b.Incr(name,1)
}

func (b *Batch) Incr(name string, int value) {
	counts[name]=counts[name]++
}

func (b *Batch) Commit() {

}
