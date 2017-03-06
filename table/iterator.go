package table

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

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

	last header
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

func (itr *BlockIterator) Seek(seek []byte, whence int) {
	itr.err = nil

	switch whence {
	case ORIGIN:
		itr.Reset()
	case CURRENT:
	}

	var done bool
	for itr.Init(); itr.Valid(); itr.Next() {
		itr.KV(func(k, v []byte) {
			if bytes.Compare(k, seek) >= 0 {
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
		// last entry in the block.
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
	fmt.Printf("LAST: %+v\n", itr.last)

	if itr.last.prev == math.MaxUint16 {
		// if itr.pos == 0 && itr.last.prev == 0 {
		itr.err = io.EOF
		itr.pos = 0
		return
	}

	itr.pos = itr.last.prev
	if itr.err != nil {
		itr.Next()
	}

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

func (itr *TableIterator) Seek(seek []byte, whence int) {
	itr.err = nil

	switch whence {
	case ORIGIN:
		itr.Reset()
	case CURRENT:
	}

	itr.bpos = itr.t.blockIndexFor(seek)
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}

	itr.bi = block.NewIterator()
	itr.bi.Seek(seek, ORIGIN)
	itr.err = itr.bi.Error()
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
	}

	itr.bi.Next()
	if !itr.bi.Valid() {
		itr.bpos++
		itr.bi = nil
		itr.Next()
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
	//	s.openFile(0)
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
