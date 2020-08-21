/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"encoding/binary"

	"github.com/dgraph-io/ristretto/z"
)

type Buffer struct {
	buf    []byte
	offset int
}

func NewBuffer(sz int) *Buffer {
	return &Buffer{
		buf:    z.Calloc(sz),
		offset: 0,
	}
}

func (b *Buffer) Len() int {
	return b.offset
}

func (b *Buffer) Bytes() []byte {
	return b.buf[0:b.offset]
}

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 64

func (b *Buffer) Grow(n int) {
	// In this case, len and cap are the same.
	if len(b.buf) == 0 && n <= smallBufferSize {
		b.buf = z.Calloc(smallBufferSize)
		return
	} else if b.buf == nil {
		b.buf = z.Calloc(n)
		return
	}
	if b.offset+n < len(b.buf) {
		return
	}

	sz := 2*len(b.buf) + n
	newBuf := z.Calloc(sz)
	copy(newBuf, b.buf[:b.offset])
	z.Free(b.buf)
	b.buf = newBuf
}

// Allocate is not thread-safe. The byte slice returned MUST be used before further calls to Buffer.
func (b *Buffer) Allocate(n int) []byte {
	b.Grow(n)
	off := b.offset
	b.offset += n
	return b.buf[off:b.offset]
}

func (b *Buffer) writeLen(sz int) {
	buf := b.Allocate(4)
	binary.BigEndian.PutUint32(buf, uint32(sz))
}

func (b *Buffer) SliceAllocate(sz int) []byte {
	b.Grow(4 + sz)
	b.writeLen(sz)
	return b.Allocate(sz)
}

func (b *Buffer) SliceOffsets(offsets []int) []int {
	start := 0
	for start < b.offset {
		offsets = append(offsets, start)
		sz := binary.BigEndian.Uint32(b.buf[start:])
		start += 4 + int(sz)
	}
	return offsets
}

func (b *Buffer) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(b.buf[offset:])
	start := offset + 4
	return b.buf[start : start+int(sz)]
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	b.Grow(len(p))
	n = copy(b.buf[b.offset:], p)
	b.offset += n
	return n, nil
}

func (b *Buffer) Reset() {
	b.offset = 0
}

func (b *Buffer) Release() {
	z.Free(b.buf)
}
