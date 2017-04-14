package y

import (
	"bytes"
	"container/heap"
	//	"fmt"
)

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	//	Prev()
	SeekToFirst()
	//	SeekToLast()
	Seek(key []byte)
	//	SeekForPrev(key []byte)
	KeyValue() ([]byte, []byte)
	Valid() bool

	Name() string // Mainly for debug or testing.

	// All iterators should be closed so that file garbage collection works.
	Close()
}

// mergeHeap is an internal structure to remember which iterator has the smallest element.
type mergeHeap struct {
	it  *MergeIterator
	idx []int
}

func (s *mergeHeap) Len() int { return len(s.idx) }

func (s *mergeHeap) Less(i, j int) bool {
	idx1, idx2 := s.idx[i], s.idx[j]
	cmp := bytes.Compare(s.it.keys[idx1], s.it.keys[idx2])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	// The keys are equal. In this case, lower indices take precedence. This is important.
	return idx1 < idx2
}

func (s *mergeHeap) Swap(i, j int) {
	s.idx[i], s.idx[j] = s.idx[j], s.idx[i]
}

func (s *mergeHeap) Push(x interface{}) {
	s.idx = append(s.idx, x.(int))
}

func (s *mergeHeap) Pop() interface{} {
	n := len(s.idx)
	out := s.idx[n-1]
	s.idx = s.idx[:n-1]
	return out
}

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	iters []Iterator
	keys  [][]byte
	h     *mergeHeap
}

// NewMergeIterator returns a new MergeIterator from a list of Iterators.
func NewMergeIterator(iters []Iterator) *MergeIterator {
	return &MergeIterator{
		iters: iters,
		keys:  make([][]byte, len(iters)),
	}
}

func (s *MergeIterator) Name() string { return "MergeIterator" }

// Valid returns whether the MergeIterator is at a valid element.
func (s *MergeIterator) Valid() bool {
	if s == nil {
		return false
	}
	for _, it := range s.iters {
		if it.Valid() {
			return true
		}
	}
	return false
}

// KeyValue returns the current key-value pair.
func (s *MergeIterator) KeyValue() ([]byte, []byte) {
	if len(s.h.idx) == 0 {
		return nil, nil
	}
	return s.iters[s.h.idx[0]].KeyValue()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	AssertTrue(s.Valid())
	k, _ := s.KeyValue()
	oldKey := make([]byte, len(k))
	// TODO: Remove this?
	AssertTrue(len(k) == copy(oldKey, k))
	for {
		idx := s.h.idx[0] // Which iterator.
		it := s.iters[idx]
		heap.Pop(s.h)
		AssertTrue(it.Valid())
		it.Next()
		if it.Valid() {
			// Need to push back the idx and update keys.
			s.keys[idx], _ = it.KeyValue()
			heap.Push(s.h, idx) // Consider using Fix instead of Pop, Push.
		}
		if !s.Valid() {
			break
		}
		// Check the new key. If it is equal to the old key, we continue popping.
		newKey, _ := s.KeyValue()
		if !bytes.Equal(newKey, oldKey) {
			break
		}
		// If equal, we need to continue popping elements.
	}
}

// SeekToFirst seeks to first element.
func (s *MergeIterator) SeekToFirst() {
	for _, it := range s.iters {
		it.SeekToFirst()
	}
	s.initHeap()
}

// Seek brings us to element with key >= given key.
func (s *MergeIterator) Seek(key []byte) {
	for _, it := range s.iters {
		it.Seek(key)
	}
	s.initHeap()
}

// initHeap checks all iterators and initializes our heap and array of keys.
func (s *MergeIterator) initHeap() {
	s.h = &mergeHeap{it: s}
	for i, it := range s.iters {
		s.keys[i] = nil
		if !it.Valid() {
			continue
		}
		s.keys[i], _ = it.KeyValue()
		heap.Push(s.h, i)
	}
}

func (s *MergeIterator) Close() {
	for _, it := range s.iters {
		it.Close()
	}
}

// ConcatIterator iterates over some tables in the given order.
// NOTE: ConcatIterator owns the array of iterators and is responsible for closing them.
// TODO: Remove this.
type ConcatIterator struct {
	idx   int // Which iterator is active now.
	iters []Iterator
}

func NewConcatIterator(iters []Iterator) *ConcatIterator {
	return &ConcatIterator{
		iters: iters,
		idx:   -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) SeekToFirst() {
	if len(s.iters) == 0 {
		return
	}
	s.idx = 0
	s.iters[0].SeekToFirst()
}

func (s *ConcatIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.iters) && s.iters[s.idx].Valid()
}

func (s *ConcatIterator) Name() string { return "ConcatIterator" }

// KeyValue returns key, value at current position.
func (s *ConcatIterator) KeyValue() ([]byte, []byte) {
	AssertTrue(s.Valid())
	return s.iters[s.idx].KeyValue()
}

func (s *ConcatIterator) Seek(key []byte) {
	// CURRENTLY NOT IMPLEMENTED.
	Fatalf("ConcatIterator.Seek is currently not implemented")
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.iters[s.idx].Next()
	if s.iters[s.idx].Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		s.idx++
		if s.idx >= len(s.iters) {
			// End of list. Valid will become false.
			return
		}
		s.iters[s.idx].SeekToFirst()
		if s.iters[s.idx].Valid() {
			break
		}
	}
	return
}

func (s *ConcatIterator) Close() {
	for _, it := range s.iters {
		it.Close()
	}
}
