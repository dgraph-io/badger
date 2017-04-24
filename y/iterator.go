/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"bytes"
	"container/heap"
	//	"fmt"
)

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ([]byte, byte)
	Valid() bool
	Name() string // Mainly for debug or testing.

	// All iterators should be closed so that file garbage collection works.
	Close()
}

// mergeHeap is an internal structure to remember which iterator has the smallest element.
type mergeHeap struct {
	it       *MergeIterator
	idx      []int
	reversed bool
}

func (s *mergeHeap) Len() int { return len(s.idx) }

func (s *mergeHeap) Less(i, j int) bool {
	idx1, idx2 := s.idx[i], s.idx[j]
	cmp := bytes.Compare(s.it.keys[idx1], s.it.keys[idx2])
	if cmp < 0 {
		return !s.reversed
	}
	if cmp > 0 {
		return s.reversed
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
	iters    []Iterator
	keys     [][]byte
	h        *mergeHeap
	reversed bool
}

// NewMergeIterator returns a new MergeIterator from a list of Iterators.
func NewMergeIterator(iters []Iterator, reversed bool) *MergeIterator {
	return &MergeIterator{
		iters:    iters,
		keys:     make([][]byte, len(iters)),
		reversed: reversed,
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

func (s *MergeIterator) Key() []byte {
	if len(s.h.idx) == 0 {
		return nil
	}
	return s.iters[s.h.idx[0]].Key()
}

func (s *MergeIterator) Value() ([]byte, byte) {
	if len(s.h.idx) == 0 {
		return nil, 0
	}
	return s.iters[s.h.idx[0]].Value()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	AssertTrue(s.Valid())
	k := s.Key()
	oldKey := make([]byte, len(k))
	// This copy is necessary because the underlying iterator is going to not make
	// copies and the same key buffer may be reused.
	AssertTrue(len(k) == copy(oldKey, k))
	for {
		idx := s.h.idx[0] // Which iterator.
		it := s.iters[idx]
		heap.Pop(s.h)
		AssertTrue(it.Valid())
		it.Next()
		if it.Valid() {
			// Need to push back the idx and update keys.
			s.keys[idx] = it.Key()
			heap.Push(s.h, idx) // Consider using Fix instead of Pop, Push.
		}
		if !s.Valid() {
			break
		}
		// Check the new key. If it is equal to the old key, we continue popping.
		newKey := s.Key()
		if !bytes.Equal(newKey, oldKey) {
			break
		}
		// If equal, we need to continue popping elements.
	}
}

// Rewind seeks to first element (or last element for reverse iterator).
func (s *MergeIterator) Rewind() {
	for _, it := range s.iters {
		it.Rewind()
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
// Whenever we reverse direction, we need to run this.
func (s *MergeIterator) initHeap() {
	s.h = &mergeHeap{
		it:       s,
		reversed: s.reversed,
	}
	for i, it := range s.iters {
		s.keys[i] = nil
		if !it.Valid() {
			continue
		}
		s.keys[i] = it.Key()
		heap.Push(s.h, i)
	}
}

func (s *MergeIterator) Close() {
	for _, it := range s.iters {
		it.Close()
	}
}
