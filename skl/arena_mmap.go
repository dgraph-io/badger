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

package skl

import (
	"io/ioutil"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
)

// ArenaMmap should be lock-free.
type ArenaMmap struct {
	n   uint32
	buf []byte
}

// newArenaMmap returns a new arena.
func newArenaMmap(n int64) *ArenaMmap {
	fd, err := ioutil.TempFile("", "arena")
	y.Check(err)
	fd.Truncate(n)

	// mtype := unix.PROT_READ | unix.PROT_WRITE
	// data, err := unix.Mmap(-1, 0, int(sz), mtype, unix.MAP_SHARED|unix.MAP_ANONYMOUS)
	data, err := z.Mmap(fd, true, n)
	y.Check(err)
	zeroOut(data, 0)
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &ArenaMmap{
		n:   1,
		buf: data,
	}
	return out
}

func (s *ArenaMmap) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *ArenaMmap) reset() {
	atomic.StoreUint32(&s.n, 0)
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (s *ArenaMmap) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))

	// Return the aligned offset.
	m := (n - l + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// // Put will *copy* val into arena. To make better use of this, reuse your input
// // val buffer. Returns an offset into buf. User is responsible for remembering
// // size of val. We could also store this size inside arena but the encoding and
// // decoding will incur some overhead.
// func (s *Arena) putVal(uid uint64) uint32 {
// 	l := uint32(8) // uint64 is 8 bytes.
// 	n := atomic.AddUint32(&s.n, l)
// 	y.AssertTruef(int(n) <= len(s.buf),
// 		"Arena too small, toWrite:%d newTotal:%d limit:%d",
// 		l, n, len(s.buf))
// 	m := n - l
// 	binary.BigEndian.PutUint64(s.buf[m:], uid)
// 	// v.Encode(s.buf[m:])
// 	return m
// }

func (s *ArenaMmap) putKey(key string) uint32 {
	l := uint32(len(key))
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))
	m := n - l
	y.AssertTrue(len(key) == copy(s.buf[m:n], key))
	return m
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (s *ArenaMmap) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

// getKey returns byte slice at offset.
func (s *ArenaMmap) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (s *ArenaMmap) getVal(offset uint32, size uint32) (ret y.ValueStruct) {
	ret.Decode(s.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *ArenaMmap) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
