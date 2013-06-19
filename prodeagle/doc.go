// prodeagle-go von Matthias Koch steht unter einer Creative Commons Namensnennung - Weitergabe unter gleichen Bedingungen 3.0 Unported Lizenz.
// Beruht auf einem Inhalt unter https://github.com/fmt-Println-MKO/prodeagle-go.
// Licensed: Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)
// http://creativecommons.org/licenses/by-sa/3.0/

/*
prodegale api library for appengine apps 

just count what ever you wanted to get count and watch your app with http://www.prodeagle.com/
api documentation: http://godoc.org/github.com/fmt-Println-MKO/prodeagle-go/prodeagle

get the api 
	go get github.com/fmt-Println-MKO/prodeagle-go/prodeagle

import it into your code
	import "github.com/fmt-Println-MKO/prodeagle-go/prodeagle"

short example how to use:
add a url for the prodeagle api:
	http.HandleFunc("/prodeagle/", prodeagle.Dispatch)
just count any counter like this:

c is your appengine.Context
name is string with name of counter you want to increment by 1
	prodeagle.Incr(c, name) 

count by more then one:
delta is an int64 and value is the number the counter should be incremented
	prodeagle.IncrDelta(c, name, delta) 

counting in batchmode
batchmode means your counter stats are not imedatly written, 
they will just be written when you commit them. in between this counters are not included during harvest from prodeagle

creating a new Batch Counter
	var b *prodeagle.Batch
	if nil == b {
		b = prodeagle.NewBatch(c)
	}

incrementing a counter by 1 in batch
	b.Incr(name)

incrementing a counter by given value in batch
	b.IncrDelta(name,delta)

committing batch counter, so that prodeagle harvest can get them
	b.Commit()

thats all
*/
package prodeagle
