package badger

import (
	"sync"

	"github.com/dgraph-io/badger/y"
)

type KVItem struct {
	wg         sync.WaitGroup
	key        []byte
	vptr       []byte
	meta       byte
	val        []byte
	casCounter uint16
	slice      *y.Slice
	next       *KVItem
}

// Key returns the key. If nil, the iteration is done and you should break out of channel loop.
func (item *KVItem) Key() []byte {
	return item.key
}

// Value returns the value, generally fetched from the value log. This can block while
// the fetch workers populate the value.
func (item *KVItem) Value() []byte {
	item.wg.Wait()
	return item.val
}

type list struct {
	head *KVItem
	tail *KVItem
}

func (l *list) push(i *KVItem) {
	i.next = nil
	if l.tail == nil {
		l.head = i
		l.tail = i
		return
	}
	l.tail.next = i
	l.tail = i
}

func (l *list) pop() *KVItem {
	if l.head == nil {
		return nil
	}
	i := l.head
	if l.head == l.tail {
		l.tail = nil
		l.head = nil
	} else {
		l.head = i.next
	}
	i.next = nil
	return i
}

type IteratorOptions struct {
	PrefetchSize int
	FetchValues  bool
	Reversed     bool
}

type Iterator struct {
	kv   *KV
	iitr y.Iterator

	opt   IteratorOptions
	item  *KVItem
	data  list
	waste list
}

func (it *Iterator) newItem() *KVItem {
	item := it.waste.pop()
	if item == nil {
		item = &KVItem{slice: new(y.Slice)}
	}
	return item
}

func (it *Iterator) fetchOneValue(item *KVItem) {
	item.val = it.kv.decodeValue(item.vptr, item.meta, item.slice)
	item.wg.Done()
}

func (it *Iterator) Item() *KVItem {
	return it.item
}

func (it *Iterator) Valid() bool {
	return it.item != nil
}

func (it *Iterator) Close() {
	it.iitr.Close()
}

func (it *Iterator) Next() {
	// Reuse current item
	it.item.wg.Wait() // Just cleaner to wait before pushing to avoid doing ref counting.
	it.waste.push(it.item)

	// Set next item to current
	it.item = it.data.pop()

	// Advance internal iterator
	if it.iitr.Valid() {
		it.iitr.Next()
	}
	if !it.iitr.Valid() {
		return
	}
	item := it.newItem()
	it.fill(item)
	it.data.push(item)
}

func (it *Iterator) fill(item *KVItem) {
	vs := it.iitr.Value()
	item.meta = vs.Meta
	item.casCounter = vs.CASCounter
	item.key = y.Safecopy(item.key, it.iitr.Key())
	item.vptr = y.Safecopy(item.vptr, vs.Value)
	if it.opt.FetchValues {
		item.wg.Add(1)
		go it.fetchOneValue(item)
	}
}

func (it *Iterator) prefetch() {
	i := it.iitr
	var count int
	it.item = nil
	for ; i.Valid(); i.Next() {
		count++

		item := it.newItem()
		it.fill(item)
		if it.item == nil {
			it.item = item
		} else {
			it.data.push(item)
		}
		if count == it.opt.PrefetchSize {
			break
		}
	}
}

func (it *Iterator) Seek(key []byte) {
	for i := it.data.pop(); i != nil; {
		i.wg.Wait()
		it.waste.push(i)
	}
	it.iitr.Seek(key)
	it.prefetch()
}

func (it *Iterator) Rewind() {
	i := it.data.pop()
	for i != nil {
		i.wg.Wait() // Just cleaner to wait before pushing. No ref count is needed.
		it.waste.push(i)
		i = it.data.pop()
	}

	it.iitr.Rewind()
	it.prefetch()
}

func (s *KV) NewIterator(opt IteratorOptions) *Iterator {
	tables, decr := s.getMemTables()
	defer decr()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].NewUniIterator(opt.Reversed))
	}
	iters = s.lc.appendIterators(iters, opt.Reversed) // This will increment references.
	res := &Iterator{
		kv:   s,
		iitr: y.NewMergeIterator(iters, opt.Reversed),
		opt:  opt,
	}
	return res
}
