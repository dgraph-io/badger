// Adapted from https://github.com/facebook/rocksdb/blob/master/db/skiplist.h
package skiplist

import (
	"math/rand"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/dgraph/x"
)

type Node struct {
	key string

	// next[i] is the next node on level i.
	//	next []*Node
	next []unsafe.Pointer
}

// NewNode returns new node with given key and height.
func NewNode(key string, height int) *Node {
	return &Node{
		key:  key,
		next: make([]unsafe.Pointer, height),
	}
}

// Next returns the next node for a level.
func (s *Node) Next(n int) *Node {
	x.AssertTrue(n >= 0)
	return (*Node)(atomic.LoadPointer(&s.next[n]))
}

// SetNext sets node.Next[level].
func (s *Node) SetNext(n int, node *Node) {
	x.AssertTrue(n >= 0)
	atomic.StorePointer(&s.next[n], unsafe.Pointer(node))
}

type Skiplist struct {
	kMaxHeight    int
	kBranching    int
	kInvBranching float32 // 1 / kBranching
	head          *Node

	// maxHeight modified only by Insert(). Read racily by readers but stale
	// values are ok.
	maxHeight int // Height for entire list.

	// Used for optimizing sequential insert patterns.  Tricky.  prev_[i] for
	// i up to max_height_ is the predecessor of prev_[0] and prev_height_
	// is the height of prev_[0].  prev_[0] can only be equal to head before
	// insertion, in which case max_height_ and prev_height_ are 1.
	prev       []*Node
	prevHeight int
}

// randomHeight returns a random height for a new node.
func (s *Skiplist) randomHeight() int {
	height := 1
	// Increase height with probability 1 in kBranching
	for height < s.kMaxHeight && rand.Float32() < s.kInvBranching {
		height++
	}
	x.AssertTruef(height > 0 && height <= s.kMaxHeight, "%d", height)
	return height
}

// keyIsAfterNode returns true if key is greater than the data stored in "n".
func keyIsAfterNode(key string, n *Node) bool {
	return (n != nil) && (n.key < key)
}

// findGreaterOrEqual returns the earliest node with a key >= key.
// Return nullptr if there is no such node.
func (s *Skiplist) findGreaterOrEqual(key string) *Node {
	// Note: It looks like we could reduce duplication by implementing
	// this function as FindLessThan(key)->Next(0), but we wouldn't be able
	// to exit early on equality and the result wouldn't even be correct.
	// A concurrent insert might occur after FindLessThan(key) but before
	// we get a chance to call Next(0).
	n := s.head
	level := s.maxHeight - 1
	var lastBigger *Node
	for {
		next := n.Next(level)
		// Make sure the lists are sorted
		x.AssertTrue(n == s.head || next == nil || keyIsAfterNode(next.key, n))
		// Make sure we haven't overshot during our search
		x.AssertTrue(n == s.head || keyIsAfterNode(key, n))
		var cmp int
		if next == nil || next == lastBigger {
			cmp = 1
		} else {
			cmp = strings.Compare(next.key, key)
		}
		if cmp == 0 || (cmp > 0 && level == 0) {
			return next
		}
		if cmp < 0 {
			n = next
		} else {
			lastBigger = next
			level--
		}
	}
}

// findLessThan returns the latest node with a key < key.
// Return head_ if there is no such node.
// Fills prev[level] with pointer to previous node at "level" for every
// level in [0..max_height_-1], if prev is non-null.
func (s *Skiplist) findLessThan(key string, prev []*Node) *Node {
	n := s.head
	level := s.maxHeight - 1
	// KeyIsAfter(key, lastNotAfter) is definitely false
	var lastNotAfter *Node
	for {
		next := n.Next(level)
		x.AssertTrue(n == s.head || next == nil || keyIsAfterNode(next.key, n))
		x.AssertTrue(n == s.head || keyIsAfterNode(key, n))
		if next != lastNotAfter && keyIsAfterNode(key, next) {
			// Keep searching in this list
			n = next
			continue
		}
		if prev != nil {
			prev[level] = n
		}
		if level == 0 {
			return n
		}
		// Switch to next list, reuse KeyIUsAfterNode() result.
		lastNotAfter = next
		level--
	}
}

// findLast returns the last node in the list.
// Return head_ if list is empty.
func (s *Skiplist) findLast() *Node {
	n := s.head
	level := s.maxHeight - 1
	for {
		next := n.Next(level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			return n
		}
		// Switch to next list.
		level--
	}
}

// EstimateCount estimates number of items strictly smaller than key.
func (s *Skiplist) EstimateCount(key string) int {
	var count int
	n := s.head
	level := s.maxHeight - 1
	for {
		x.AssertTrue(n == s.head || n.key < key)
		next := n.Next(level)
		if next != nil && next.key < key {
			n = next
			count++
			continue
		}
		if level == 0 {
			return count
		}
		// Switch to next list.
		count *= s.kBranching
		level--
	}
}

// NewSkiplist creates a new skiplist.
func NewSkiplist(maxHeight, branchingFactor int) *Skiplist {
	x.AssertTrue(maxHeight > 0)
	x.AssertTrue(branchingFactor > 1)
	s := &Skiplist{
		kMaxHeight:    maxHeight,
		kBranching:    branchingFactor,
		kInvBranching: 1.0 / float32(branchingFactor),
		head:          NewNode("", maxHeight),
		maxHeight:     1,
		prevHeight:    1,
	}
	// Allocate the prev_ Node* array, directly from the passed-in allocator.
	// prev_ does not need to be freed, as its life cycle is tied up with
	// the allocator as a whole.
	s.prev = make([]*Node, maxHeight)
	for i := 0; i < maxHeight; i++ {
		s.prev[i] = s.head
	}
	return s
}

// Insert inserts a new key into our skiplist.
func (s *Skiplist) Insert(key string) {
	// Fast path for sequential insertion.
	if !keyIsAfterNode(key, s.prev[0]) &&
		(s.prev[0] == s.head || keyIsAfterNode(key, s.prev[0])) {
		x.AssertTrue(s.prev[0] != s.head || (s.prevHeight == 1 && s.maxHeight == 1))

		// Outside of this method prev_[1..max_height_] is the predecessor
		// of prev_[0], and prev_height_ refers to prev_[0].  Inside Insert
		// prev_[0..max_height - 1] is the predecessor of key.  Switch from
		// the external state to the internal
		for i := 1; i < s.prevHeight; i++ {
			s.prev[i] = s.prev[0]
		}
	} else {
		// TODO(opt): we could use a NoBarrier predecessor search as an
		// optimization for architectures where memory_order_acquire needs
		// a synchronization instruction.  Doesn't matter on x86
		s.findLessThan(key, s.prev)
	}

	// Our data structure does not allow duplicate insertion.
	x.AssertTrue(s.prev[0].Next(0) == nil || key != s.prev[0].Next(0).key)

	height := s.randomHeight()
	if height > s.maxHeight {
		for i := s.maxHeight; i < height; i++ {
			s.prev[i] = s.head
		}
		// It is ok to mutate max_height_ without any synchronization
		// with concurrent readers.  A concurrent reader that observes
		// the new value of max_height_ will see either the old value of
		// new level pointers from head_ (nullptr), or a new value set in
		// the loop below.  In the former case the reader will
		// immediately drop to the next level since nullptr sorts after all
		// keys.  In the latter case the reader will use the new node.
		s.maxHeight = height
	}

	n := NewNode(key, height)
	for i := 0; i < height; i++ {
		// NoBarrier_SetNext() suffices since we will add a barrier when
		// we publish a pointer to "x" in prev[i].
		n.SetNext(i, s.prev[i].Next(i))
		s.prev[i].SetNext(i, n)
	}
	s.prev[0] = n
	s.prevHeight = height
}

// Contains returns whether skiplist contains given key.
func (s *Skiplist) Contains(key string) bool {
	n := s.findGreaterOrEqual(key)
	return n != nil && key == n.key
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *Skiplist
	node *Node
}

// Iterator returns a new iterator for our skiplist.
func (s *Skiplist) Iterator() *Iterator {
	return &Iterator{list: s}
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.node != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() string {
	x.AssertTrue(s.Valid())
	return s.node.key
}

// Next advances to the next position.
func (s *Iterator) Next() {
	x.AssertTrue(s.Valid())
	s.node = s.node.Next(0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	// Instead of using explicit "prev" links, we just search for the
	// last node that falls before key.
	x.AssertTrue(s.Valid())
	s.node = s.list.findLessThan(s.node.key, nil)
	if s.node == s.list.head {
		s.node = nil
	}
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target string) {
	s.node = s.list.findGreaterOrEqual(target)
}

// SeekForPrev retreats to the last entry with a key <= target.
func (s *Iterator) SeekForPrev(target string) {
	s.Seek(target)
	if !s.Valid() {
		s.SeekToLast()
	}
	for s.Valid() && target < s.node.key {
		s.Prev()
	}
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.node = s.list.head.Next(0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.node = s.list.findLast()
	if s.node == s.list.head {
		s.node = nil
	}
}
