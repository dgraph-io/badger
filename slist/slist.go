package slist

/*
Adapted from java.util.concurrent.ConcurrentSkipListMap. This is a lock-free skiplist.

===============
KEY DIFFERENCES
===============
We are able to simplify the code substantially because of the following differences.

(1) We do not support deletes because our application requires us to distinguish between a
    deleted key VS a key that has never been seen. To do that, the application should use a
    special value (unsafe.Pointer) to indicate a deleted value.
(2) No custom comparator.
(3) We pre-initialize all levels. We do not have to CAS the top head node or keep level in each
    head node.

============
HOW IT WORKS
============
There are two main structs: baseNode and indexNode.

A baseNode is in the base level. An indexNode is in higher levels.

Every indexNode points to a baseNode. Only the baseNode stores the value.

Every level has one header dummy node. On the base level, there is one header baseNode.
On index levels, the header nodes have baseNode equal to this header baseNode.

We refer to first index level as level 0. The last index level is level "kNumIndexLevels-1".
We refer to the base level of baseNodes as the "base level", not level 0.

The number of nodes in level 0 is equal to the number of nodes in base level.
*/

import (
	"bytes"
	//	"log"
	"math/rand"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/y"
)

const (
	kNumIndexLevels     = 30  // Number of index levels.
	kProbHeightIncrease = 0.5 // With this probability height increases by 1.
)

type baseNode struct {
	key   []byte
	value unsafe.Pointer
	next  unsafe.Pointer // *baseNode
}

type indexNode struct {
	base  *baseNode      // Immutable.
	down  *indexNode     // Immutable.
	right unsafe.Pointer // *indexNode
}

type Skiplist struct {
	heads     []*indexNode // Size is kNumIndexLevels.
	numLevels int32        // Top head node is heads[numLevels-1] and 1<=numLevels<=kNumIndexLevels.
}

// If baseNode.value == kBaseNodeHeader, then this baseNode is a header node.
var (
	kBaseNodeHeader = unsafe.Pointer(new(int))
)

// NewSkiplist returns a new Skiplist object.
func NewSkiplist() *Skiplist {
	s := &Skiplist{
		heads:     make([]*indexNode, kNumIndexLevels),
		numLevels: 1, // Initially there is only one level of indexNodes.
	}
	b := &baseNode{value: kBaseNodeHeader}
	for i := 0; i < kNumIndexLevels; i++ {
		s.heads[i] = &indexNode{base: b}
		// s.heads[0].down is left as nil. This is level 0.
		if i > 0 {
			s.heads[i].down = s.heads[i-1]
		}
	}
	return s
}

func (s *baseNode) isHead() bool  { return s.value == kBaseNodeHeader }
func (s *indexNode) isHead() bool { return s.base.isHead() }

func (s *baseNode) casValue(old, val unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&s.value, old, val)
}
func (s *baseNode) casNext(old, val *baseNode) bool {
	return atomic.CompareAndSwapPointer(&s.next, unsafe.Pointer(old), unsafe.Pointer(val))
}
func (s *baseNode) getNext() *baseNode { return (*baseNode)(s.next) }
func (s *indexNode) casRight(old, val *indexNode) bool {
	return atomic.CompareAndSwapPointer(&s.right, unsafe.Pointer(old), unsafe.Pointer(val))
}
func (s *indexNode) getRight() *indexNode { return (*indexNode)(s.right) }

// findPred finds a baseNode that has key strictly less than given key. This routine is very
// simple. There's no CAS here or restarts.
func (s *Skiplist) findPred(key []byte) *baseNode {
	y.AssertTrue(key != nil)
	q := s.heads[s.numLevels-1] // Top head node.
	r := q.getRight()
	for {
		// Iterate towards right as long as r's key < key. Otherwise, you want to descend from q.
		if r != nil && bytes.Compare(key, r.base.key) > 0 {
			q = r
			r = r.getRight()
			continue
		}
		d := q.down
		if d == nil {
			// We are at index level 0. Just return the base node.
			return q.base
		}
		q = d // Descend one level.
		r = q.getRight()
	}
}

// findBaseNode finds a baseNode that has key equal to given key.
func (s *Skiplist) findBaseNode(key []byte) *baseNode {
restart:
	b := s.findPred(key)
	n := b.getNext()
	for {
		if n == nil {
			return nil
		}
		f := n.getNext()
		if n != b.getNext() {
			goto restart // Inconsistent read. Restart.
		}
		cmp := bytes.Compare(key, n.key)
		if cmp == 0 {
			return n
		} else if cmp < 0 {
			return nil
		}
		// Somehow n.key is < given key, despite what s.findPred is supposed to give.
		// Redo with b being its next node.
		b = n
		n = f
	}
}

// Get returns the value of given key.
func (s *Skiplist) Get(key []byte) unsafe.Pointer {
	n := s.findBaseNode(key)
	if n == nil {
		return nil
	}
	return n.value
}

// randomNumLevels returns number of levels. Note 1 <= output <= kNumIndexLevels.
func randomNumLevels() int {
	numLevels := int(1)
	for rand.Float32() < kProbHeightIncrease && numLevels < kNumIndexLevels {
		numLevels++
	}
	return numLevels
}

// addIndex adds index nodes from numLevels-1 down to level 0. We assume idx points to a list of
// indexNodes. This list of new index nodes is of size numLevels.
func (s *Skiplist) addIndex(idx *indexNode, numLevels int) {
	insertionLevel := numLevels - 1 // Level number where we want to insert idx.
	key := idx.base.key
restart:
	j := int(s.numLevels - 1) // Start from top level of skiplist.
	h := s.heads[j]
	q := h
	r := q.getRight()
	for {
		if r != nil {
			n := r.base
			cmp := bytes.Compare(key, n.key)
			if cmp > 0 {
				// Iterate right just like in findPred.
				q = r
				r = r.getRight()
				continue
			}
		}
		// Either r==nil or we find r's key <= given key (like in findPred).
		if j == insertionLevel {
			// Tries to insert index node between q and r.
			y.AssertTrue(idx != nil)
			if !q.casRight(r, idx) {
				goto restart // Restart for this insertionLevel.
			}
			if insertionLevel == 0 {
				return // We are just done with index level 0. So we are done.
			}
			insertionLevel-- // Next level we want to insert is
			idx = idx.down
		}
		// Descend one level.
		j--
		q = q.down
		r = q.getRight()
	}
}

// insertIndex inserts indexNodes for baseNode z, with numLevels.
func (s *Skiplist) insertIndex(z *baseNode, numLevels int) {
	// Create indexNodes.
	idxs := make([]*indexNode, numLevels)
	for i := 0; i < numLevels; i++ {
		idxs[i] = &indexNode{base: z}
		if i > 0 {
			idxs[i].down = idxs[i-1]
		}
	}
	for {
		oldNumLevels := s.numLevels
		if numLevels <= int(oldNumLevels) {
			// No need to increase s.numLevels.
			break
		}
		if atomic.CompareAndSwapInt32(&s.numLevels, oldNumLevels, int32(numLevels)) {
			break
		}
	}
	s.addIndex(idxs[numLevels-1], numLevels)
}

// Put adds element if not present, or replaces value if present and onlyIfAbsent=false.
// Returns the old value of nil if newly inserted.
func (s *Skiplist) Put(key []byte, value unsafe.Pointer, onlyIfAbsent bool) unsafe.Pointer {
	y.AssertTrue(key != nil)
restart:
	b := s.findPred(key)
	n := b.getNext()
	for {
		if n != nil {
			f := n.getNext()
			if n != b.getNext() {
				goto restart // Inconsistent read.
			}
			v := n.value
			cmp := bytes.Compare(key, n.key)
			if cmp > 0 {
				// Somehow, n < key which is unexpected from findPred. Need to "restart" moving right.
				b = n
				n = f
				continue
			}
			if cmp == 0 {
				// We found the key.
				if onlyIfAbsent || n.casValue(v, value) {
					return v // Returns old value.
				}
				goto restart // onlyIfAbsent=false and we try to replace the value but failed. Restart.
			}
			// cmp < 0: fall through. findPred returns an expected result.
		}
		// Either n==nil OR n.key > key. We need to insert a new baseNode between b and n.
		z := &baseNode{
			key:   key,
			value: value,
			next:  unsafe.Pointer(n),
		}
		if !b.casNext(n, z) {
			goto restart // b.next is no longer n. We need to restart.
		}
		// Insert index nodes.
		s.insertIndex(z, randomNumLevels())
		return unsafe.Pointer(nil)
	}
}
