/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We make some simplifications to findLt and findGeq.
*/

package skl

import (
	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/y"
)

const (
	kMaxHeight      = 20
	kHeightIncrease = math.MaxUint32 / 3
)

type node struct {
	key   []byte           // Immutable. Left nil for head node.
	value unsafe.Pointer   // CAS.
	next  []unsafe.Pointer // []*node. Size is <=kMaxNumLevels. CAS.
}

type Skiplist struct {
	height int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	head   *node
}

func newNode(key []byte, value unsafe.Pointer, height int) *node {
	return &node{
		key:   key,
		next:  make([]unsafe.Pointer, height),
		value: value,
	}
}

func NewSkiplist() *Skiplist {
	head := newNode(nil, nil, kMaxHeight)
	return &Skiplist{
		height: 1,
		head:   head,
	}
}

func (s *node) setValue(val unsafe.Pointer, onlyIfAbsent bool) unsafe.Pointer {
	if onlyIfAbsent {
		// Not going to overwrite. Just return old value.
		return s.value
	}
	for {
		old := s.value
		if atomic.CompareAndSwapPointer(&s.value, old, val) {
			return old
		}
	}
}

func (s *node) getNext(h int) *node { return (*node)(s.next[h]) }

func (s *node) casNext(h int, old, val *node) bool {
	return atomic.CompareAndSwapPointer(&s.next[h], unsafe.Pointer(old), unsafe.Pointer(val))
}

// Returns true if key is strictly > n.key.
// If n is nil, this is an "end" marker and we return false.
//func (s *Skiplist) keyIsAfterNode(key []byte, n *node) bool {
//	y.AssertTrue(n != s.head)
//	return n != nil && bytes.Compare(key, n.key) > 0
//}

func randomHeight() int {
	h := 1
	for h < kMaxHeight && rand.Uint32() <= kHeightIncrease {
		h++
	}
	return h
}

// findGeq finds leftmost node such that node.key >= key. Returns true if we have equality.
// Note: It looks like we could reduce duplication by implementing this function as
// findLt(key)->getNext(0), but we wouldn't be able to exit early on equality and the result
// might not even be correct. A concurrent insert might occur between findLt and the insert,
// before we are done with that getNext(0) call. That would require a restart which will slow
// us down a lot.
func (s *Skiplist) findGeq(key []byte) (*node, bool) {
	x := s.head
	level := int(s.height - 1)
	for {
		// Assume x.key < key.
		next := x.getNext(level)
		if next == nil {
			// We hit the end of the list. If on base level, we are done. Otherwise, descend.
			if level == 0 {
				return nil, false
			}
			level--
			continue
		}
		cmp := bytes.Compare(key, next.key)
		if cmp == 0 {
			// We have equality. Return right away. No need to descend.
			return next, true
		} else if cmp > 0 {
			// key > next.key. We should continue to move right.
			x = next
			continue
		}
		// Assume key < next.key. We cannot move right anymore.
		if level == 0 {
			// Base level reached. We have the answer.
			return next, false
		}
		level-- // Descend and continue looking.
	}
}

// findLt finds rightmost node such that node.key < key. This is a strict inequality.
func (s *Skiplist) findLt(key []byte) *node {
	x := s.head
	level := int(s.height - 1)
	for {
		// Assume x.key < key.
		next := x.getNext(level)
		if next == nil {
			// We hit the end of the list. If on base level, we are done. Otherwise, descend.
			if level == 0 {
				if x == s.head {
					// Be careful about returning x. It could be the head node.
					// Be sure to include test cases where we query an empty list!
					return nil
				}
				return x
			}
			level-- // Not on base level and can descend.
			continue
		}
		if bytes.Compare(key, next.key) > 0 {
			// key > next.key. We can continue moving right on this level.
			x = next
			continue
		}
		// key <= next.key. We cannot move right anymore. Try moving down.
		if level == 0 {
			return x
		}
		level--
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := before.getNext(level)
		if next == nil {
			return before, next
		}
		cmp := bytes.Compare(key, next.key)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

// Put inserts the key-value pair. Two cases.
// If key already exists, we return the previous value. We will not overwrite value if
// onlyIfAbsent is true. Otherwise, we will try to overwrite the value.
func (s *Skiplist) Put(key []byte, val unsafe.Pointer, onlyIfAbsent bool) unsafe.Pointer {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.

	listHeight := s.height
	var prev [kMaxHeight + 1]*node
	var next [kMaxHeight + 1]*node
	prev[listHeight] = s.head
	next[listHeight] = nil
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			return prev[i].setValue(val, onlyIfAbsent)
		}
	}

	// We do need to create a new node.
	height := randomHeight()
	x := newNode(key, val, height)

	// Try to increase s.height via CAS.
	listHeight = s.height
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.height
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				y.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				y.AssertTrue(prev[i] != next[i])
			}
			x.next[i] = unsafe.Pointer(next[i])
			if prev[i].casNext(i, next[i], x) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				y.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				return prev[i].setValue(val, onlyIfAbsent)
			}
		}
	}
	return nil
}

func (s *Skiplist) findLast() *node {
	n := s.head
	level := int(s.height) - 1
	for {
		next := n.getNext(level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			return n
		}
		level--
	}
}

func (s *Skiplist) Get(key []byte) unsafe.Pointer {
	n, found := s.findGeq(key)
	if !found {
		return nil
	}
	return n.value
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *Skiplist
	n    *node
}

// Iterator returns a new iterator for our skiplist.
func (s *Skiplist) Iterator() *Iterator {
	return &Iterator{list: s}
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() []byte {
	y.AssertTrue(s.Valid())
	return s.n.key
}

// Next advances to the next position.
func (s *Iterator) Next() {
	y.AssertTrue(s.Valid())
	s.n = s.n.getNext(0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	y.AssertTrue(s.Valid())
	s.n = s.list.findLt(s.n.key)
	if s.n == s.list.head {
		s.n = nil
	}
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.n, _ = s.list.findGeq(target)
}

// SeekForPrev retreats to the last entry with a key <= target.
//func (s *Iterator) SeekForPrev(target []byte) {
//	s.Seek(target)
//	if !s.Valid() {
//		s.SeekToLast()
//	}
//	for s.Valid() && s.list.cmp.Compare(target, s.node.key) < 0 {
//		s.Prev()
//	}
//}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.head.getNext(0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
	if s.n == s.list.head {
		s.n = nil
	}
}
