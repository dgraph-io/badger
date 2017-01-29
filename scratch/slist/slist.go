// Improvement over skiplist.go.
package slist

import (
	"sync/atomic"
	"unsafe"
)

var (
	baseHeader = unsafe.Pointer(new(int))
)

type Node struct {
	key   string
	next  unsafe.Pointer // *Node
	value unsafe.Pointer
}

func NewNode(next *Node) *Node {
	s := &Node{next: unsafe.Pointer(next)}
	s.value = unsafe.Pointer(s)
	return s
}

func (s *Node) casValue(cmp, val unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&s.value, cmp, val)
}

// casNext compares and swaps node.next.
func (s *Node) casNext(cmp, val *Node) bool {
	return atomic.CompareAndSwapPointer(&s.next, unsafe.Pointer(cmp),
		unsafe.Pointer(val))
}

func (s *Node) isMarker() bool { return s.value == unsafe.Pointer(s) }

func (s *Node) isBaseHeader() bool { return s.value == baseHeader }

func (s *Node) appendMarker(f *Node) bool {
	return s.casNext(f, NewNode(f))
}

func (s *Node) helpDelete(b, f *Node) {
	if unsafe.Pointer(f) == s.next && unsafe.Pointer(s) == b.next {
		if f == nil || f.value != unsafe.Pointer(f) { // not already marked
			s.appendMarker(f)
		} else {
			b.casNext(s, (*Node)(f.next))
		}
	}
}

func (s *Node) getValidValue() unsafe.Pointer {
	v := s.value
	if v == unsafe.Pointer(s) || v == baseHeader {
		return nil
	}
	return s.value
}

type Index struct {
	node  *Node
	down  *Index
	right unsafe.Pointer // *Index
}

func NewIndex(node *Node, down, right *Index) *Index {
	return &Index{
		node:  node,
		down:  down,
		right: unsafe.Pointer(right),
	}
}

func (s *Index) casRight(cmp, val *Index) bool {
	return atomic.CompareAndSwapPointer(&s.right, unsafe.Pointer(cmp),
		unsafe.Pointer(val))
}

func (s *Index) indexesDeletedNode() bool { return s.node.value == nil }

func (s *Index) link(succ, newSucc *Index) bool {
	n := s.node
	newSucc.right = unsafe.Pointer(succ)
	return n.value != nil && s.casRight(succ, newSucc)
}

func (s *Index) unlink(succ *Index) bool {
	return !s.indexesDeletedNode() && s.casRight(succ, (*Index)(succ.right))
}

type HeadIndex struct {
	Index
	level int
}

func NewHeadIndex(node *Node, down, right *Index, level int) *HeadIndex {
	return &HeadIndex{
		Index: Index{
			node:  node,
			down:  down,
			right: unsafe.Pointer(right),
		},
		level: level,
	}
}

func inHalfOpenRange(key, least, fence string) bool {
	return key >= least && key < fence
}

func inOpenRange(key, least, fence string) bool {
	return key >= least && key <= fence
}

// Work in progress.
// http://fuseyism.com/classpath/doc/java/util/concurrent/ConcurrentSkipListMap-source.html
