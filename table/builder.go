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
	"encoding/binary"
	"math"

	"github.com/dgraph-io/badger/y"
)

//var tableSize int64 = 50 << 20
var (
	restartInterval int = 100 // Might want to change this to be based on total size instead of numKeys.
	bufPool             = new(bufferPool)
)

func init() {
	bufPool.Ch = make(chan *bytes.Buffer, 10)
}

type bufferPool struct {
	Ch chan *bytes.Buffer
}

func (p *bufferPool) Put(b *bytes.Buffer) {
	b.Reset()

	select {
	case p.Ch <- b:
	default:
		// ignore
	}
}

func (p *bufferPool) Get() *bytes.Buffer {
	select {
	case b := <-p.Ch:
		return b
	default:
		return new(bytes.Buffer)
	}
}

type header struct {
	plen int // Overlap with base key.
	klen int // Length of the diff.
	vlen int // Length of value.
	prev int // Offset for the previous key-value pair. The offset is relative to block base offset.
}

// Encode encodes the header.
func (h header) Encode() []byte {
	b := make([]byte, h.Size())
	y.AssertTrue(h.plen >= 0 && h.plen <= math.MaxUint16)
	y.AssertTrue(h.klen >= 0 && h.klen <= math.MaxUint16)
	y.AssertTrue(h.vlen >= 0 && h.vlen <= math.MaxUint16)
	y.AssertTrue(h.prev >= 0 && h.prev <= math.MaxUint32)

	binary.BigEndian.PutUint16(b[0:2], uint16(h.plen))
	binary.BigEndian.PutUint16(b[2:4], uint16(h.klen))
	binary.BigEndian.PutUint16(b[4:6], uint16(h.vlen))
	binary.BigEndian.PutUint32(b[6:10], uint32(h.prev))
	return b
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) int {
	h.plen = int(binary.BigEndian.Uint16(buf[0:2]))
	h.klen = int(binary.BigEndian.Uint16(buf[2:4]))
	h.vlen = int(binary.BigEndian.Uint16(buf[4:6]))
	h.prev = int(binary.BigEndian.Uint32(buf[6:10]))
	return h.Size()
}

// Size returns size of the header. Currently it's just a constant.
func (h header) Size() int { return 10 }

type TableBuilder struct {
	counter int // Number of keys written for the current block.

	// Typically tens or hundreds of meg. This is for one single file.
	buf *bytes.Buffer

	// TODO: Consider removing this var. It just tracks size of buf.
	pos int

	baseKey    []byte // Base key for the current block.
	baseOffset int    // Offset for the current block.

	restarts []uint32 // Base offsets of every block.

	// Tracks offset for the previous key-value pair. Offset is relative to block base offset.
	prevOffset int
}

func NewTableBuilder() *TableBuilder {
	return &TableBuilder{
		buf:        bufPool.Get(),
		prevOffset: math.MaxUint32, // Used for the first element!
	}
}

// Close closes the TableBuilder. Do not use buf field anymore.
func (b *TableBuilder) Close() {
	bufPool.Put(b.buf)
}

func (b *TableBuilder) Empty() bool { return b.buf.Len() == 0 }

// write appends d to our buffer.
func (b *TableBuilder) write(d []byte) {
	//	b.buf = append(b.buf, d...)
	b.buf.Write(d)
	b.pos += len(d)
	y.AssertTruef(b.pos >= b.baseOffset, "b.pos=%d b.baseOffset=%d d=%v", b.pos, b.baseOffset, d)
}

func (b *TableBuilder) writeByte(d byte) {
	b.buf.WriteByte(d)
	b.pos += 1
	y.AssertTruef(b.pos >= b.baseOffset, "b.pos=%d b.baseOffset=%d d=%v", b.pos, b.baseOffset, d)
}

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b TableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *TableBuilder) addHelper(key, value []byte, meta byte) {
	// diffKey stores the difference of key with baseKey.
	var diffKey []byte
	if len(b.baseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.baseKey = append(b.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		plen: len(key) - len(diffKey),
		klen: len(diffKey),
		vlen: len(value) + 1, // Include meta byte.
		prev: b.prevOffset,   // prevOffset is the location of the last key-value added.
	}
	b.prevOffset = b.pos - b.baseOffset // Remember current offset for the next Add call.

	// Layout: header, diffKey, value.
	b.write(h.Encode())
	b.write(diffKey)  // We only need to store the key difference.
	b.writeByte(meta) // Meta byte precedes actual value.
	b.write(value)
	b.counter++ // Increment number of keys added for this current block.
}

func (b *TableBuilder) finishBlock() {
	// When we are at the end of the block and Valid=false, and the user wants to do a Prev,
	// we need a dummy header to tell us the offset of the previous key-value pair.
	b.addHelper([]byte{}, []byte{}, 0)
}

// Add adds a key-value pair to the block.
// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
func (b *TableBuilder) Add(key, value []byte, meta byte) error {
	if b.counter >= restartInterval {
		b.finishBlock()
		// Start a new block. Initialize the block.
		b.restarts = append(b.restarts, uint32(b.pos))
		b.counter = 0
		b.baseKey = []byte{}
		b.baseOffset = b.pos
		b.prevOffset = math.MaxUint32 // First key-value pair of block has header.prev=MaxUint32.
	}
	b.addHelper(key, value, meta)
	return nil // Currently, there is no meaningful error.
}

// FinalSize returns the *rough* final size of the array, counting the header which is not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.
func (b *TableBuilder) FinalSize() int {
	return b.pos + 8 /* empty header */ + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
}

// blockIndex generates the block index for the table.
// It is mainly a list of all the block base offsets.
func (b *TableBuilder) blockIndex() []byte {
	// Store the end offset, so we know the length of the final block.
	b.restarts = append(b.restarts, uint32(b.pos))

	// Add 4 because we want to write out number of restarts at the end.
	sz := 4*len(b.restarts) + 4
	out := make([]byte, sz)
	buf := out
	for _, r := range b.restarts {
		binary.BigEndian.PutUint32(buf[:4], r)
		buf = buf[4:]
	}
	binary.BigEndian.PutUint32(buf[:4], uint32(len(b.restarts)))
	return out
}

var emptySlice = make([]byte, 100)

// Finish finishes the table by appending the index.
func (b *TableBuilder) Finish(metadata []byte) []byte {
	b.finishBlock() // This will never start a new block.
	index := b.blockIndex()
	b.write(index)

	b.write(metadata)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(metadata)))
	b.write(buf[:])

	return b.buf.Bytes()
}
