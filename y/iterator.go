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
	"encoding/binary"

	"github.com/pkg/errors"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
	Value     []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 2 // meta, usermeta.
	if v.ExpiresAt == 0 {
		return uint32(sz + 1)
	}

	enc := sizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[2:])
	v.Value = b[2+sz:]
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = v.UserMeta
	sz := binary.PutUvarint(b[2:], v.ExpiresAt)
	copy(b[2+sz:], v.Value)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	buf.WriteByte(v.UserMeta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)
	buf.Write(enc[:sz])
	buf.Write(v.Value)
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}

type elem struct {
	itr      Iterator
	nice     int
	reversed bool
}

type elemHeap []*elem

func (eh elemHeap) Len() int            { return len(eh) }
func (eh elemHeap) Swap(i, j int)       { eh[i], eh[j] = eh[j], eh[i] }
func (eh *elemHeap) Push(x interface{}) { *eh = append(*eh, x.(*elem)) }
func (eh *elemHeap) Pop() interface{} {
	// Remove the last element, because Go has already swapped 0th elem <-> last.
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}
func (eh elemHeap) Less(i, j int) bool {
	cmp := CompareKeys(eh[i].itr.Key(), eh[j].itr.Key())
	if cmp < 0 {
		return !eh[i].reversed
	}
	if cmp > 0 {
		return eh[i].reversed
	}
	// The keys are equal. In this case, lower nice take precedence. This is important.
	return eh[i].nice < eh[j].nice
}

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	h        elemHeap
	curKey   []byte
	reversed bool

	all []Iterator
}

// NewMergeIterator returns a new MergeIterator from a list of Iterators.
func NewMergeIterator(iters []Iterator, reversed bool) *MergeIterator {
	m := &MergeIterator{all: iters, reversed: reversed}
	m.h = make(elemHeap, 0, len(iters))
	m.initHeap()
	return m
}

func (s *MergeIterator) storeKey(smallest Iterator) {
	if cap(s.curKey) < len(smallest.Key()) {
		s.curKey = make([]byte, 2*len(smallest.Key()))
	}
	s.curKey = s.curKey[:len(smallest.Key())]
	copy(s.curKey, smallest.Key())
}

// initHeap checks all iterators and initializes our heap and array of keys.
// Whenever we reverse direction, we need to run this.
func (s *MergeIterator) initHeap() {
	s.h = s.h[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}
		e := &elem{itr: itr, nice: idx, reversed: s.reversed}
		s.h = append(s.h, e)
	}
	heap.Init(&s.h)
	for len(s.h) > 0 {
		it := s.h[0].itr
		if it == nil || !it.Valid() {
			heap.Pop(&s.h)
			continue
		}
		s.storeKey(s.h[0].itr)
		break
	}
}

// Valid returns whether the MergeIterator is at a valid element.
func (s *MergeIterator) Valid() bool {
	if s == nil {
		return false
	}
	if len(s.h) == 0 {
		return false
	}
	return s.h[0].itr.Valid()
}

// Key returns the key associated with the current iterator
func (s *MergeIterator) Key() []byte {
	if len(s.h) == 0 {
		return nil
	}
	return s.h[0].itr.Key()
}

// Value returns the value associated with the iterator.
func (s *MergeIterator) Value() ValueStruct {
	if len(s.h) == 0 {
		return ValueStruct{}
	}
	return s.h[0].itr.Value()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	if len(s.h) == 0 {
		return
	}

	smallest := s.h[0].itr
	smallest.Next()

	for len(s.h) > 0 {
		smallest = s.h[0].itr
		if !smallest.Valid() {
			heap.Pop(&s.h)
			continue
		}

		heap.Fix(&s.h, 0)
		smallest = s.h[0].itr
		if smallest.Valid() {
			if !bytes.Equal(smallest.Key(), s.curKey) {
				break
			}
			smallest.Next()
		}
	}
	if !smallest.Valid() {
		return
	}
	s.storeKey(smallest)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (s *MergeIterator) Rewind() {
	for _, itr := range s.all {
		itr.Rewind()
	}
	s.initHeap()
}

// Seek brings us to element with key >= given key.
func (s *MergeIterator) Seek(key []byte) {
	for _, itr := range s.all {
		itr.Seek(key)
	}
	s.initHeap()
}

// Close implements y.Iterator
func (s *MergeIterator) Close() error {
	for _, itr := range s.all {
		if err := itr.Close(); err != nil {
			return errors.Wrap(err, "MergeIterator")
		}
	}
	return nil
}

// FastMergeIterator is a specialized MergeIterator that works better than the merge Iterator for
// small number of iterators. We found out that if we have less than 18 iterators in merge iterator,
// the fast merge iterator performs better.
type FastMergeIterator struct {
	second  Iterator
	smaller Iterator
	bigger  Iterator
	reverse bool
}

func (mt *FastMergeIterator) fix() {
	if !mt.bigger.Valid() {
		return
	}
	var cmp int
	for mt.smaller.Valid() {
		cmp = CompareKeys(mt.smaller.Key(), mt.bigger.Key())
		if cmp == 0 {
			mt.second.Next()
			if !mt.second.Valid() {
				return
			}
			continue
		}
		if mt.reverse {
			if cmp < 0 {
				mt.smaller, mt.bigger = mt.bigger, mt.smaller
			}
		} else {
			if cmp > 0 {
				mt.smaller, mt.bigger = mt.bigger, mt.smaller
			}
		}
		return
	}
	mt.smaller, mt.bigger = mt.bigger, mt.smaller
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *FastMergeIterator) Next() {
	mt.smaller.Next()
	mt.fix()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *FastMergeIterator) Rewind() {
	mt.smaller.Rewind()
	mt.bigger.Rewind()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *FastMergeIterator) Seek(key []byte) {
	mt.smaller.Seek(key)
	mt.bigger.Seek(key)
	mt.fix()
}

// Valid returns whether the FastMergeIterator is at a valid element.
func (mt *FastMergeIterator) Valid() bool {
	return mt.smaller.Valid()
}

// Key returns the key associated with the current iterator
func (mt *FastMergeIterator) Key() []byte {
	return mt.smaller.Key()
}

// Value returns the value associated with the iterator.
func (mt *FastMergeIterator) Value() ValueStruct {
	return mt.smaller.Value()
}

// Close implements y.Iterator
func (mt *FastMergeIterator) Close() error {
	err1 := mt.smaller.Close()
	err2 := mt.bigger.Close()
	if err1 != nil {
		return errors.Wrap(err1, "FastMergeIterator")
	}
	return errors.Wrap(err2, "FastMergeIterator")
}

// NewFastMergeIterator creates a merge iterator
func NewFastMergeIterator(iters []Iterator, reverse bool) Iterator {
	if len(iters) == 1 {
		return iters[0]
	} else if len(iters) == 2 {
		return &FastMergeIterator{
			smaller: iters[0],
			bigger:  iters[1],
			second:  iters[1],
			reverse: reverse}
	}
	mid := len(iters) / 2
	return NewFastMergeIterator(
		[]Iterator{
			NewFastMergeIterator(iters[:mid], reverse),
			NewFastMergeIterator(iters[mid:], reverse),
		}, reverse)
}

// GetMergeIterator returns a new merge Iterator based on the number of iters provided.
func GetMergeIterator(iters []Iterator, reverse bool) Iterator {
	// We found out that if there are less than 18 tables in the merge iterator,
	// the "FastMergeIterator" is faster than the "MergeIterator".
	if len(iters) < 18 {
		return NewFastMergeIterator(iters, reverse)
	}
	return NewMergeIterator(iters, reverse)
}
