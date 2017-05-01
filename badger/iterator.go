package badger

import (
	"context"
	"sync"
	"sync/atomic"

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
	ref        int32
}

// Key returns the key. If nil, the iteration is done and you should break out of channel loop.
func (kv *KVItem) Key() []byte {
	return kv.key
}

// Value returns the value, generally fetched from the value log. This can block while
// the fetch workers populate the value.
func (kv *KVItem) Value() []byte {
	kv.wg.Wait()
	return kv.val
}

type iteratorOp struct {
	seekExtreme int    // 0 = FIRST, 1 = LAST
	seekTo      []byte // specific key
	direction   int    // 0 = FWD, 1 = REV
	close       bool
}

func (op iteratorOp) Set(i y.Iterator) {
	if op.seekTo != nil {
		i.Seek(op.seekTo)
		return
	}
	if op.seekExtreme == 0 {
		i.Rewind() // Either a seek to first or end.
		return
	}
	y.Fatalf("Unhandled seek operation.")
}

type Iterator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ch       chan *KVItem
	fetchCh  chan *KVItem
	kv       *KV
	iitr     y.Iterator
	seekCh   chan iteratorOp
	reversed bool

	recycler chan *KVItem
}

// func debugItem(item *KVItem, msg string) {
// 	fmt.Printf("[%x] %s\n", item.uuid, msg)
// }

func (itr *Iterator) newItem() (item *KVItem) {
LOOP:
	for {
		select {
		case item = <-itr.recycler:
			if atomic.LoadInt32(&item.ref) > 0 {
				itr.doRecycle(item) // Don't change the ref count.
				continue LOOP
			}

			item.wg = sync.WaitGroup{}
			item.meta = 0
			item.casCounter = 0
			break LOOP
		default:
			item = &KVItem{slice: new(y.Slice)}
			break LOOP
		}
	}
	item.wg.Add(1)
	atomic.AddInt32(&item.ref, 1)
	return item
}

func (itr *Iterator) Ch() <-chan *KVItem {
	return itr.ch
}

func (itr *Iterator) doRecycle(item *KVItem) {
	select {
	case itr.recycler <- item:
	default:
		// Don't do anything, Go GC will take care of this.
	}
}

// Recycle would reuse the memory allocated for KVItem.
func (itr *Iterator) Recycle(item *KVItem) {
	atomic.AddInt32(&item.ref, -1)
	itr.doRecycle(item)
}

func (itr *Iterator) clearCh() {
	for {
		select {
		case item := <-itr.ch:
			itr.Recycle(item)
		case item := <-itr.fetchCh:
			atomic.AddInt32(&item.ref, -1)
			// These will continue until channels are empty.
			//
			// Best not to recycle items from here to avoid this race cond:
			// - Item is already being processed by fetchValue
			// - We pick this item from itr.Ch, and push it to recycler.
			// - Recycler recirculates this item, which then goes back into fetchCh.
			// - previous fetchValue finishes, calles Done().
			// - fetchCh -> fetchValue -> also calls Done on the same item.
			// Avoid this by just not recycling these items.
		default:
			return
		}
	}
}

func (itr *Iterator) prefetch() {
	safecopy := func(a []byte, src []byte) []byte {
		if cap(a) < len(src) {
			a = make([]byte, len(src))
		}
		a = a[:len(src)]
		copy(a, src)
		return a
	}

	i := itr.iitr
	var op iteratorOp
TOP:
	select {
	case op = <-itr.seekCh:
		y.Trace(itr.ctx, "Got op: %+v", op)
		itr.clearCh()
	case <-itr.ctx.Done():
		return
	}

	for !op.close {
		op.Set(i)
	INTERNAL:
		for ; i.Valid(); i.Next() {
			vs := i.Value()
			if (vs.Meta & BitDelete) != 0 {
				// Tombstone encountered.
				continue
			}

			item := itr.newItem()
			item.meta = vs.Meta
			item.casCounter = vs.CASCounter
			item.key = safecopy(item.key, i.Key())
			item.vptr = safecopy(item.vptr, vs.Value)

			select {
			case op = <-itr.seekCh:
				y.Trace(itr.ctx, "Got op: %+v", op)
				itr.clearCh()
				break INTERNAL
			case itr.ch <- item: // We must have incremented sync.WaitGroup before pushing to ch.
				if itr.fetchCh != nil {
					atomic.AddInt32(&item.ref, 1)
					itr.fetchCh <- item
				} else {
					item.wg.Done()
				}
			case <-itr.ctx.Done():
				return
			}
		}
		if !i.Valid() {
			itr.ch <- &KVItem{key: nil}
			// Reached end of iterator.
			goto TOP
		}
	}
}

func (itr *Iterator) Rewind() {
	itr.seekCh <- iteratorOp{seekExtreme: 0}
}

func (itr *Iterator) Seek(key []byte) {
	itr.seekCh <- iteratorOp{seekTo: key}
}

func (itr *Iterator) fetchValue() {
	for {
		select {
		case item := <-itr.fetchCh:
			item.val = itr.kv.decodeValue(item.vptr, item.meta, item.slice)
			item.wg.Done()
			atomic.AddInt32(&item.ref, -1)
		case <-itr.ctx.Done():
			return
		}
	}
}

func (itr *Iterator) Close() {
	itr.seekCh <- iteratorOp{close: true}
	itr.cancel()
	itr.iitr.Close()
}

// NewIterator returns a store wide iterator. You can control how many key-value pairs would be
// prefetched by setting the prefetchSize. Most values would need to be retrieved from the value
// log. You can control how many goroutines would be doing random seeks in value log to fill
// the values by passing numWorkers.
// If you set numWorkers to zero, the values won't be retrieved.
// Note: This acquires references to underlying tables. Remember to close the returned iterator.
func (s *KV) NewIterator(
	ctx context.Context, prefetchSize, numWorkers int, reversed bool) *Iterator {
	// The order we add these iterators is important.
	// Imagine you add level0 first, then add imm. In between, the initial imm might be moved into
	// level0, and be completely missed. On the other hand, if you add imm first and it got moved
	// to level 0, you would just have that data appear twice which is fine.
	if numWorkers < 0 {
		return nil
	}

	tables, decr := s.getMemTables()
	defer decr()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].NewUniIterator(reversed))
	}
	iters = s.lc.appendIterators(iters, reversed) // This will increment references.

	itr := &Iterator{
		kv:       s,
		iitr:     y.NewMergeIterator(iters, reversed),
		ch:       make(chan *KVItem, prefetchSize),
		seekCh:   make(chan iteratorOp), // unbuffered channel
		recycler: make(chan *KVItem, 2*prefetchSize),
	}
	itr.ctx, itr.cancel = context.WithCancel(ctx)
	if numWorkers > 0 {
		itr.fetchCh = make(chan *KVItem, prefetchSize)
		for i := 0; i < numWorkers; i++ {
			go itr.fetchValue()
		}
	}
	go itr.prefetch()
	return itr
}
