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
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/v2/y"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Arena should be lock-free.
type Arena struct {
	data   []byte
	offset uint32
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.offset))
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.offset, sz)
	if offset >= uint32(len(s.data)) {
		log.Fatalf("Arena too small, toWrite:%d newTotal:%d limit:%d",
			sz, offset, len(s.data))
	}
	return offset
}

// allocateValue encodes valueStruct and put it in the arena buffer.
// It returns the encoded uint64 => | size (32 bits) | offset (32 bits) |
func (s *Arena) allocateValue(v y.ValueStruct) uint64 {
	valOffset := s.putVal(v)
	return encodeValue(valOffset, v.EncodedSize())
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n - l + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (s *Arena) putVal(v y.ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l) - l
	buf := s.data[offset : offset+l]
	v.Encode(buf)
	return offset
}

// putKey puts the key and returns its offset
func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz) - keySz
	buf := s.data[offset : offset+keySz]
	y.AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.data[offset]))
}

// getKey returns byte slice at offset.
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.data[offset : offset+uint32(size)]
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (s *Arena) getVal(offset uint32, size uint32) (ret y.ValueStruct) {
	ret.Decode(s.data[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	val := uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.data[0])))
	return val

}
