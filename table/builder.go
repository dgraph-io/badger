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
	"encoding/binary"
	"math"
	//	"github.com/dgraph-io/badger/y"
)

//var tableSize int64 = 50 << 20
var restartInterval int = 100

type header struct {
	plen int
	klen int
	vlen int
	prev int
}

func (h header) Encode() []byte {
	b := make([]byte, h.Size())
	binary.BigEndian.PutUint16(b[0:2], uint16(h.plen))
	binary.BigEndian.PutUint16(b[2:4], uint16(h.klen))
	binary.BigEndian.PutUint16(b[4:6], uint16(h.vlen))
	binary.BigEndian.PutUint16(b[6:8], uint16(h.prev))
	return b
}

func (h *header) Decode(buf []byte) int {
	h.plen = int(binary.BigEndian.Uint16(buf[0:2]))
	h.klen = int(binary.BigEndian.Uint16(buf[2:4]))
	h.vlen = int(binary.BigEndian.Uint16(buf[4:6]))
	h.prev = int(binary.BigEndian.Uint16(buf[6:8]))
	return h.Size()
}

func (h header) Size() int {
	return 8
}

type TableBuilder struct {
	counter int

	// TODO: Now that each file is one table, this table can get really big. The builder will
	// have to be initialized with a file object that we can keep appending to.
	// Builder cannot afford to store everything in memory.
	// And that is not needed because the header is at the end.
	buf []byte

	// TODO: Consider removing this var. It just tracks size of buf.
	pos int

	baseKey    []byte
	baseOffset int

	restarts   []uint32
	prevOffset int
}

func (b *TableBuilder) Empty() bool { return len(b.buf) == 0 }

func (b *TableBuilder) Reset() {
	b.counter = 0
	//	if cap(b.buf) < int(tableSize) {
	//		b.buf = make([]byte, tableSize)
	//	}
	b.buf = make([]byte, 0, 1<<20)
	b.pos = 0

	b.baseKey = []byte{}
	b.restarts = b.restarts[:0]
}

func (b *TableBuilder) write(d []byte) {
	//	y.AssertTrue(len(d) == copy(b.buf[b.pos:], d))
	b.buf = append(b.buf, d...)
	b.pos += len(d)
}

func (b TableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *TableBuilder) Add(key, value []byte) error {
	//	if len(key)+len(value)+b.length() > int(tableSize) {
	//		return y.Errorf("Exceeds table size")
	//	}

	if b.counter >= restartInterval {
		b.restarts = append(b.restarts, uint32(b.pos))
		b.counter = 0
		b.baseKey = []byte{}
		b.baseOffset = b.pos
		b.prevOffset = math.MaxUint16
	}

	// diffKey stores the difference of key with baseKey.
	var diffKey []byte
	if len(b.baseKey) == 0 {
		b.baseKey = key
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		plen: len(key) - len(diffKey),
		klen: len(diffKey),
		vlen: len(value),
		prev: b.prevOffset,
	}
	b.prevOffset = b.pos - b.baseOffset

	b.write(h.Encode())
	b.write(diffKey) // We only need to store the key difference.
	b.write(value)
	b.counter++
	return nil
}

// FinalSize returns the *rough* final size of the array, counting the header which is not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.
func (b *TableBuilder) FinalSize() int {
	return b.pos + 6 /* empty header */ + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
}

func (b *TableBuilder) blockIndex() []byte {
	// Store the end offset, so we know the length of the final block.
	b.restarts = append(b.restarts, uint32(b.pos))

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
func (b *TableBuilder) Finish() []byte {
	b.Add([]byte{}, []byte{}) // Empty record to indicate the end.

	index := b.blockIndex()
	//	newpos := int(tableSize) - len(index)
	//	y.AssertTrue(b.pos <= newpos)
	//	b.pos = newpos

	b.write(index)
	//	y.AssertTrue(b.pos == int(tableSize))
	return b.buf
}
