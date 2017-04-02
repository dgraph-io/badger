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
	//	"errors"
	//	"fmt"
	"io"
	"math"
	"sort"

	"github.com/dgraph-io/badger/y"
)

/*
 *itr.Seek(key)
 *for itr.Seek(key); itr.Valid(); itr.Next() {
 *  f(itr.key(), itr.value())
 *}
 */
type BlockIterator struct {
	data    []byte
	pos     int
	err     error
	baseKey []byte

	ikey []byte

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

func (itr *BlockIterator) ensureKeyCap(h header) {
	if cap(itr.ikey) < h.plen+h.klen {
		sz := h.plen + h.klen
		if sz < 2*cap(itr.ikey) {
			sz = 2 * cap(itr.ikey)
		}
		itr.ikey = make([]byte, sz)
	}
}

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
		k, _ := itr.KeyValue()
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

func (itr *BlockIterator) parseKV(h header) {
	itr.ensureKeyCap(h)
	itr.key = itr.ikey[:h.plen+h.klen]
	// TODO: We shouldn't have to do a copy here.
	y.AssertTrue(h.plen == copy(itr.key, itr.baseKey[:h.plen]))
	y.AssertTrue(h.klen == copy(itr.key[h.plen:], itr.data[itr.pos:itr.pos+h.klen]))
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

func (itr *BlockIterator) KeyValue() ([]byte, []byte) {
	if itr.err != nil {
		return nil, nil
	}
	return itr.key, itr.val
}

type TableIterator struct {
	t    *Table
	bpos int
	bi   *BlockIterator
	err  error
	init bool
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

// Seek brings us to a key that is >= input key.
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

func (itr *TableIterator) KeyValue() ([]byte, []byte) {
	return itr.bi.KeyValue()
}

func (itr *TableIterator) Name() string { return "TableIterator" }

// ConcatIterator iterates over some tables in the given order.
type ConcatIterator struct {
	idx    int // Which table does the iterator point to currently.
	tables []*Table
	it     *TableIterator
}

func NewConcatIterator(tables []*Table) *ConcatIterator {
	//	y.AssertTrue(len(tables) > 0)
	return &ConcatIterator{
		tables: tables,
		idx:    -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) SeekToFirst() {
	if len(s.tables) == 0 {
		return
	}
	s.idx = 0
	s.it = s.tables[0].NewIterator()
	s.it.SeekToFirst()
}

func (s *ConcatIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.tables) && s.it.Valid()
}

func (s *ConcatIterator) Name() string { return "ConcatIterator" }

// KeyValue returns key, value at current position.
func (s *ConcatIterator) KeyValue() ([]byte, []byte) {
	y.AssertTrue(s.Valid())
	return s.it.KeyValue()
}

func (s *ConcatIterator) Seek(key []byte) {
	// CURRENTLY NOT IMPLEMENTED.
	y.Fatalf("ConcatIterator.Seek is currently not implemented")
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.it.Next()
	if s.it.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		s.idx++
		if s.idx >= len(s.tables) {
			// End of list. Valid will become false.
			return
		}
		s.it = s.tables[s.idx].NewIterator() // Assume tables are nonempty.
		s.it.SeekToFirst()
		if s.it.Valid() {
			break
		}
	}
	return
}
