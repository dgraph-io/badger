package table

import (
	"bytes"
	"errors"
	"fmt"
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

	lastPos int
	last    header // The last header we saw.
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
		itr.KV(func(k, v []byte) {
			if bytes.Compare(k, key) >= 0 {
				// We are done as k is >= key.
				done = true
			}
		})
		if done {
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
// CAUTION: This will only work for the last block which contains a dummy header which records
// the offset.
func (itr *BlockIterator) SeekToLast() {
	itr.err = nil
	fmt.Printf("~~Block SeekToLast\n")
	var count int
	for itr.Init(); itr.Valid(); itr.Next() {
		count++
	}
	fmt.Printf("~~Block SeekToLast count=%d\n", count)
	itr.KV(func(k, v []byte) {
		fmt.Printf("~~~Block SeekToLast k=%s v=%s\n", string(k), string(v))
	})
	itr.Prev()
}

func (itr *BlockIterator) parseKV(h header) {
	itr.ensureKeyCap(h)
	itr.key = itr.ikey[:h.plen+h.klen]
	y.AssertTrue(h.plen == copy(itr.key, itr.baseKey[:h.plen]))
	y.AssertTrue(h.klen == copy(itr.key[h.plen:], itr.data[itr.pos:itr.pos+h.klen]))
	itr.pos += h.klen

	if itr.pos+h.vlen > len(itr.data) {
		itr.err = y.Errorf("Value exceeded size of block.")
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
	if itr.last.prev == math.MaxUint16 {
		// This is the first element of the block!
		itr.err = io.EOF
		itr.pos = 0
		return
	}

	fmt.Printf("~~BI Prev\n")
	// Move back using current header's prev.
	itr.pos = itr.last.prev

	var h header
	itr.pos += h.Decode(itr.data[itr.pos:])
	itr.parseKV(h)
	itr.last = h
}

func (itr *BlockIterator) KV(fn func(k, v []byte)) {
	if itr.err != nil {
		return
	}

	fn(itr.key, itr.val)
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
	return itr.err == nil
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
	itr.Seek([]byte{}, 0)
	if !itr.Valid() {
		itr.Next()
	}
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
	// Go to the last block and seek to last.
	fmt.Printf("~~~Table SeekToLast bpos=%d\n", itr.bpos)
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
func (itr *TableIterator) Seek(key []byte, whence int) {
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
		// Here is a subcase. If idx == len(itr.t.blockIndex), then
		if idx == len(itr.t.blockIndex) {
			// Input key is greater than ANY element of the table.
			// There's nothing we can do. Valid() should return false as we seek ot table.End.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekHelper(idx, key)
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
	fmt.Printf("~~~Table Prev\n")
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
		fmt.Printf("~~~itr.bpos=%d %v\n", itr.bpos, itr.bi.Valid())
		itr.bi.KV(func(k, v []byte) {
			fmt.Printf("~~~k=%s v=%s\n", string(k), string(v))
		})
		return
	}

	itr.bi.Prev()
	if !itr.bi.Valid() {
		fmt.Printf("~~~Recurring %d to %d\n", itr.bpos, itr.bpos-1)
		itr.bpos--
		itr.bi = nil
		itr.Prev()
		return
	}
}

func (itr *TableIterator) KV(fn func(k, v []byte)) {
	itr.bi.KV(fn)
}

// ConcatIterator iterates over some tables in the given order.
type ConcatIterator struct {
	idx    int // Which table does the iterator point to currently.
	tables []*Table
	it     *TableIterator
}

func NewConcatIterator(tables []*Table) *ConcatIterator {
	y.AssertTrue(len(tables) > 0)
	s := &ConcatIterator{tables: tables}
	s.it = tables[0].NewIterator()
	s.it.SeekToFirst()
	return s
}

// openFile is an internal helper function for opening i-th file.
//func (s *ConcatIterator) openFile(i int) error {
//	s.Close() // Try to close current file.
//	s.idx = i // Set the index.
//	var err error
//	s.f, err = os.Open(s.filenames[i])
//	if err != nil {
//		return err
//	}
//	tbl := Table{fd: s.f}
//	if err = tbl.ReadIndex(); err != nil {
//		return err
//	}
//	s.it = tbl.NewIterator()
//	s.it.Reset()
//	s.it.Seek([]byte(""), ORIGIN) // Assume no such key.

// Some weird behavior for table iterator. It seems that sometimes we need to call s.it.Next().
// Sometimes, we shouldn't. TODO: Look into this.
// Example 1: The usual test case where we insert 10000 entries. Iter will be invalid initially.
// Example 2: Insert two entries. Iter is valid initially. Calling next will skip the first entry.
// See table_test.go for the above examples.
//	if !s.it.Valid() {
//		s.it.Next() // Necessary due to unexpected behavior of table iterator.
//	}
//	return nil
//}

// Close cleans up the iterator. Not really needed if you just run Next to the end.
// But just in case we did not, it is good to always close it.
//func (s *ConcatIterator) Close() {
//	if s.f == nil {
//		return
//	}
//	s.f.Close()
//	s.f = nil
//	s.it = nil
//}

func (s *ConcatIterator) Valid() bool {
	return s.it.Valid()
}

// KeyValue returns key, value at current position.
func (s *ConcatIterator) KeyValue() ([]byte, []byte) {
	y.AssertTrue(s.Valid())
	var key, val []byte
	s.it.KV(func(k, v []byte) {
		key = k
		val = v
	})
	return key, val
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() error {
	s.it.Next()
	if s.it.Valid() {
		return nil
	}
	for { // In case there are empty tables.
		s.idx++
		if s.idx >= len(s.tables) {
			return errors.New("End of list") // TODO: Define error constant.
		}
		s.it = s.tables[s.idx].NewIterator() // Assume tables are nonempty.
		s.it.SeekToFirst()
		if s.it.Valid() {
			break
		}
	}
	return nil
}

// TODO: Consider having a universal iterator interface so that MergingIterator can be
// built from any of these iterators. For now, we assume it reads from two ConcatIterators.
// Note: If both iterators have the same key, the first one gets returned first.
// Typical usage:
// it0 := NewConcatIterator(...)
// defer it0.Close()
// it1 := NewConcatIterator(...)
// defer it1.Close()
// it := NewMergingIterator(it0, it1)
// for ; it.Valid(); it.Next() {
//   k, v := it.KeyValue()
// }
// No need to close "it".
type MergingIterator struct {
	it0, it1 *ConcatIterator // We do not own this.
	idx      int             // Which ConcatIterator holds the current element.
}

// NewMergingIterator creates a new merging iterator.
func NewMergingIterator(it0, it1 *ConcatIterator) *MergingIterator {
	s := &MergingIterator{
		it0: it0,
		it1: it1,
	}
	s.updateIdx()
	return s
}

func (s *MergingIterator) updateIdx() {
	v0 := s.it0.Valid()
	v1 := s.it1.Valid()
	if !v0 && !v1 {
		return
	}
	if !v0 {
		s.idx = 1
		return
	}
	if !v1 {
		s.idx = 0
		return
	}
	k0, _ := s.it0.KeyValue()
	k1, _ := s.it1.KeyValue()
	if bytes.Compare(k0, k1) <= 0 {
		s.idx = 0
	} else {
		s.idx = 1
	}
}

func (s *MergingIterator) Next() {
	y.AssertTrue(s.it0.Valid() || s.it1.Valid())
	if s.idx == 0 {
		s.it0.Next()
	} else if s.idx == 1 {
		s.it1.Next()
	}
	s.updateIdx()
}

func (s *MergingIterator) Valid() bool {
	return s.it0.Valid() || s.it1.Valid()
}

func (s *MergingIterator) KeyValue() ([]byte, []byte) {
	y.AssertTrue(s.Valid())
	if s.idx == 0 {
		return s.it0.KeyValue()
	}
	return s.it1.KeyValue()
}
