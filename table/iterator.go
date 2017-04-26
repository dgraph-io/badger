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

package table

import (
	"bytes"
	"io"
	"math"
	"sort"

	"github.com/dgraph-io/badger/y"
)

type BlockIterator struct {
	data    []byte
	pos     int
	err     error
	baseKey []byte

	key  []byte
	val  []byte
	init bool

	last header // The last header we saw.
}

func (itr *BlockIterator) Reset() {
	itr.pos = 0
	itr.err = nil
	itr.baseKey = []byte{}
	itr.key = []byte{}
	itr.val = []byte{}
	itr.init = false
	itr.last = header{}
}

func (itr *BlockIterator) Init() {
	if !itr.init {
		itr.Next()
	}
}

func (itr *BlockIterator) Valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *BlockIterator) Error() error {
	return itr.err
}

func (itr *BlockIterator) Close() {}

var (
	ORIGIN  = 0
	CURRENT = 1
)

// Seek brings us to the first block element that is >= input key.
func (itr *BlockIterator) Seek(key []byte, whence int) {
	itr.err = nil

	switch whence {
	case ORIGIN:
		itr.Reset()
	case CURRENT:
	}

	var done bool
	for itr.Init(); itr.Valid(); itr.Next() {
		k := itr.Key()
		if bytes.Compare(k, key) >= 0 {
			// We are done as k is >= key.
			done = true
			break
		}
	}
	if !done {
		itr.err = io.EOF
	}
}

func (itr *BlockIterator) SeekToFirst() {
	itr.err = nil
	itr.Init()
}

// SeekToLast brings us to the last element. Valid should return true.
func (itr *BlockIterator) SeekToLast() {
	itr.err = nil
	for itr.Init(); itr.Valid(); itr.Next() {
	}
	itr.Prev()
}

// parseKV would allocate a new byte slice for key and for value.
func (itr *BlockIterator) parseKV(h header) {
	if cap(itr.key) < h.plen+h.klen {
		itr.key = make([]byte, 2*(h.plen+h.klen))
	}
	itr.key = itr.key[:h.plen+h.klen]
	copy(itr.key, itr.baseKey[:h.plen])
	copy(itr.key[h.plen:], itr.data[itr.pos:itr.pos+h.klen])
	itr.pos += h.klen

	if itr.pos+h.vlen > len(itr.data) {
		itr.err = y.Errorf("Value exceeded size of block: %d %d %d %d %v", itr.pos, h.klen, h.vlen, len(itr.data), h)
		return
	}

	itr.val = itr.data[itr.pos : itr.pos+h.vlen]
	itr.pos += h.vlen
}

func (itr *BlockIterator) Next() {
	itr.init = true
	itr.err = nil
	if itr.pos >= len(itr.data) {
		itr.err = io.EOF
		return
	}

	var h header
	itr.pos += h.Decode(itr.data[itr.pos:])
	itr.last = h // Store the last header.

	if h.klen == 0 && h.plen == 0 {
		// Last entry in the table.
		itr.err = io.EOF
		return
	}

	// Populate baseKey if it isn't set yet. This would only happen for the first Next.
	if len(itr.baseKey) == 0 {
		// This should be the first Next() for this block. Hence, prefix length should be zero.
		y.AssertTrue(h.plen == 0)
		itr.baseKey = itr.data[itr.pos : itr.pos+h.klen]
	}
	itr.parseKV(h)
}

func (itr *BlockIterator) Prev() {
	if !itr.init {
		return
	}
	itr.err = nil
	if itr.last.prev == math.MaxUint32 {
		// This is the first element of the block!
		itr.err = io.EOF
		itr.pos = 0
		return
	}

	// Move back using current header's prev.
	itr.pos = itr.last.prev

	var h header
	y.AssertTruef(itr.pos >= 0 && itr.pos < len(itr.data), "%d %d", itr.pos, len(itr.data))
	itr.pos += h.Decode(itr.data[itr.pos:])
	itr.parseKV(h)
	itr.last = h
}

func (itr *BlockIterator) Key() []byte {
	if itr.err != nil {
		return nil
	}
	return itr.key
}

func (itr *BlockIterator) Value() []byte {
	if itr.err != nil {
		return nil
	}
	return itr.val
}

type TableIterator struct {
	t    *Table
	bpos int
	bi   *BlockIterator
	err  error
	init bool
}

func (t *Table) NewIterator() *TableIterator {
	t.IncrRef() // Important.
	return &TableIterator{t: t}
}

func (itr *TableIterator) Close() {
	itr.t.DecrRef()
}

func (itr *TableIterator) Reset() {
	itr.bpos = 0
	itr.err = nil
}

func (itr *TableIterator) Valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *TableIterator) Error() error {
	return itr.err
}

func (itr *TableIterator) Init() {
	if !itr.init {
		itr.Next()
	}
}

func (itr *TableIterator) SeekToFirst() {
	numBlocks := len(itr.t.blockIndex)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.SeekToFirst()
	itr.err = itr.bi.Error()
}

func (itr *TableIterator) SeekToLast() {
	numBlocks := len(itr.t.blockIndex)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = numBlocks - 1
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.SeekToLast()
	itr.err = itr.bi.Error()
}

func (itr *TableIterator) seekHelper(blockIdx int, key []byte) {
	y.AssertTrue(blockIdx >= 0)
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.Seek(key, ORIGIN)
	itr.err = itr.bi.Error()
}

// seek brings us to a key that is >= input key.
func (itr *TableIterator) seek(key []byte, whence int) {
	itr.err = nil
	switch whence {
	case ORIGIN:
		itr.Reset()
	case CURRENT:
	}

	idx := sort.Search(len(itr.t.blockIndex), func(idx int) bool {
		ko := itr.t.blockIndex[idx]
		return bytes.Compare(ko.key, key) > 0
	})
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		itr.seekHelper(0, key)
		return
	}

	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	itr.seekHelper(idx-1, key)
	if itr.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(itr.t.blockIndex) {
			// If idx == len(itr.t.blockIndex), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekHelper(idx, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

// Seek will reset iterator and seek to >= key.
func (itr *TableIterator) Seek(key []byte) {
	itr.seek(key, ORIGIN)
}

// SeekForPrev will reset iterator and seek to <= key.
func (itr *TableIterator) SeekForPrev(key []byte) {
	// TODO: Optimize this. We shouldn't have to take a Prev step.
	itr.seek(key, ORIGIN)
	if !bytes.Equal(itr.Key(), key) {
		itr.Prev()
	}
}

func (itr *TableIterator) Next() {
	itr.err = nil

	if itr.bpos >= len(itr.t.blockIndex) {
		itr.err = io.EOF
		return
	}

	if itr.bi == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi = block.NewIterator()
		itr.bi.SeekToFirst()
		return
	}

	itr.bi.Next()
	if !itr.bi.Valid() {
		itr.bpos++
		itr.bi = nil
		itr.Next()
		return
	}
}

func (itr *TableIterator) Prev() {
	itr.err = nil
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	if itr.bi == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi = block.NewIterator()
		itr.bi.SeekToLast()
		return
	}

	itr.bi.Prev()
	if !itr.bi.Valid() {
		itr.bpos--
		itr.bi = nil
		itr.Prev()
		return
	}
}

func (itr *TableIterator) Key() []byte {
	return itr.bi.Key()
}

func (itr *TableIterator) Value() ([]byte, byte) {
	v := itr.bi.Value()
	return v[1:], v[0]
}

// UniIterator is a unidirectional TableIterator. It is a thin wrapper around
// TableIterator. We like to keep TableIterator as before, because it is more
// powerful and we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *TableIterator
	reversed bool
}

func (t *Table) NewUniIterator(reversed bool) *UniIterator {
	ti := t.NewIterator()
	ti.Init()
	return &UniIterator{
		iter:     ti,
		reversed: reversed,
	}
}

func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

func (s *UniIterator) Key() []byte           { return s.iter.Key() }
func (s *UniIterator) Value() ([]byte, byte) { return s.iter.Value() }
func (s *UniIterator) Valid() bool           { return s.iter.Valid() }
func (s *UniIterator) Name() string          { return "UniTableIterator" }
func (s *UniIterator) Close()                { s.iter.Close() }

type ConcatIterator struct {
	idx      int            // Which iterator is active now.
	iters    []*UniIterator // Corresponds to tables.
	tables   []*Table       // Disregarding reversed, this is in ascending order.
	reversed bool
}

func NewConcatIterator(tbls []*Table, reversed bool) *ConcatIterator {
	iters := make([]*UniIterator, len(tbls))
	for i := 0; i < len(tbls); i++ {
		iters[i] = tbls[i].NewUniIterator(reversed)
	}
	return &ConcatIterator{
		reversed: reversed,
		iters:    iters,
		tables:   tbls,
		idx:      -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.reversed {
		s.idx = 0
	} else {
		s.idx = len(s.iters) - 1
	}
	s.iters[s.idx].Rewind()
}

func (s *ConcatIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.iters) && s.iters[s.idx].Valid()
}

func (s *ConcatIterator) Name() string { return "ConcatIterator" }

func (s *ConcatIterator) Key() []byte {
	y.AssertTrue(s.Valid())
	return s.iters[s.idx].Key()
}

func (s *ConcatIterator) Value() ([]byte, byte) {
	y.AssertTrue(s.Valid())
	return s.iters[s.idx].Value()
}

// Seek brings us to element >= key if reversed is false. Otherwise, <= key.
func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if !s.reversed {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return bytes.Compare(s.tables[i].Biggest(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return bytes.Compare(s.tables[n-1-i].Smallest(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.idx = -1 // Invalid.
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.idx = idx
	s.iters[idx].Seek(key)
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.iters[s.idx].Next()
	if s.iters[s.idx].Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.reversed {
			s.idx++
		} else {
			s.idx--
		}
		if s.idx >= len(s.iters) || s.idx < 0 {
			// End of list. Valid will become false.
			return
		}
		s.iters[s.idx].Rewind()
		if s.iters[s.idx].Valid() {
			break
		}
	}
}

func (s *ConcatIterator) Close() {
	for _, it := range s.iters {
		it.Close()
	}
}
