package table

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/x"
)

type Block struct {
	data []byte
}

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
	return itr.err == nil
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
}

func (itr *BlockIterator) parseKV(h header) {
	itr.ensureKeyCap(h)
	itr.key = itr.ikey[:h.plen+h.klen]
	x.AssertTrue(h.plen == copy(itr.key, itr.baseKey[:h.plen]))
	x.AssertTrue(h.klen == copy(itr.key[h.plen:], itr.data[itr.pos:itr.pos+h.klen]))
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

	fmt.Printf("POS: %v\n", itr.pos)
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
