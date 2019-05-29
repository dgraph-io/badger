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
	"hash/crc32"
	"io"

	"github.com/AndreasBriese/bbloom"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

func newBuffer(sz int) *bytes.Buffer {
	b := new(bytes.Buffer)
	b.Grow(sz)
	return b
}

type header struct {
	plen uint16 // Overlap with base key.
	klen uint16 // Length of the diff.
	vlen uint32 // Length of value.
}

// Encode encodes the header.
func (h header) Encode(b []byte) {
	binary.BigEndian.PutUint16(b[0:2], h.plen)
	binary.BigEndian.PutUint16(b[2:4], h.klen)
	binary.BigEndian.PutUint32(b[4:8], h.vlen)
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) int {
	h.plen = binary.BigEndian.Uint16(buf[0:2])
	h.klen = binary.BigEndian.Uint16(buf[2:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	return h.Size()
}

// Size returns size of the header. Currently it's just a constant.
func (h header) Size() int { return 8 }

// Builder is used in building a table.
type Builder struct {
	// Typically tens or hundreds of meg. This is for one single file.
	buf *bytes.Buffer

	blockSize    uint32   // max size of block
	baseKey      []byte   // Base key for the current block.
	baseOffset   uint32   // Offset for the current block.
	entryOffsets []uint32 // offsets of entries present in current block

	restarts []uint32 // Base offsets of every block.

	keyBuf   *bytes.Buffer
	keyCount int

	checksumType pb.ChecksumType
}

// NewTableBuilder makes a new TableBuilder.
func NewTableBuilder() *Builder {
	return &Builder{
		keyBuf:    newBuffer(1 << 20),
		buf:       newBuffer(1 << 20),
		blockSize: 4 * 1024, // TODO:(Ashish): make this configurable
	}
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.buf.Len() == 0 }

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	// Add key to bloom filter.
	if len(key) > 0 {
		var klen [2]byte
		keyNoTs := y.ParseKey(key)
		binary.BigEndian.PutUint16(klen[:], uint16(len(keyNoTs)))
		b.keyBuf.Write(klen[:])
		b.keyBuf.Write(keyNoTs)
		b.keyCount++
	}

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
		plen: uint16(len(key) - len(diffKey)),
		klen: uint16(len(diffKey)),
		vlen: uint32(v.EncodedSize()),
	}

	// store current entry's offset
	b.entryOffsets = append(b.entryOffsets, uint32(b.buf.Len())-b.baseOffset)

	// Layout: header, diffKey, value.
	var hbuf [8]byte
	h.Encode(hbuf[:])
	b.buf.Write(hbuf[:])
	b.buf.Write(diffKey) // We only need to store the key difference.

	v.EncodeTo(b.buf)
}

func (b *Builder) finishBlock() error {
	idx := &pb.BlockIndex{
		EntryOffsets: b.entryOffsets, // store offsets for all entries as block index
	}
	mo, err := idx.Marshal()
	if err != nil {
		return y.Wrapf(err, "unable to marshal block index")
	}
	n, err := b.buf.Write(mo)
	if err != nil {
		return y.Wrapf(err, "unable to write block index to buf")
	}
	y.AssertTrue(n == len(mo))
	sb := make([]byte, 4)
	binary.BigEndian.PutUint32(sb, uint32(n))
	b.buf.Write(sb) // also write size of block index

	blockBuf := b.buf.Bytes()[b.baseOffset:] // store checksum for current block
	// TODO: Add checksum algo in builder options.
	cs := &pb.Checksum{
		Algo:  pb.Checksum_CRC32C,
		Sum32: crc32.Checksum(blockBuf, y.CastagnoliCrcTable),
	}
	csm, err := cs.Marshal()
	if err != nil {
		return y.Wrapf(err, "unable to marshal block checksum")
	}
	n, err = b.buf.Write(csm)
	if err != nil {
		return y.Wrapf(err, "unable to write block checksum to buf")
	}
	sb = make([]byte, 4)
	binary.BigEndian.PutUint32(sb, uint32(n)) // also write size of block checksum
	b.buf.Write(sb)

	// TODO: If we want to make block as multiple of pages, we can implement padding.
	// This might be useful while using direct io.

	return nil
}

func (b *Builder) shouldFinishBlock(key []byte, value y.ValueStruct) bool {
	// if there is no entry till now, we will return false
	if len(b.entryOffsets) <= 0 {
		return false
	}

	// have to include current entry also in size, thats why +1 len of blockEntryOffsets
	// TODO: estimate correct size for checksum
	entriesOffsetsSize := uint32((len(b.entryOffsets)+1)*4 + 4 /*size of list*/ +
		4 /*crc32 checksum*/)
	estimatedSize := uint32(b.buf.Len()) - b.baseOffset + uint32(8 /*header size*/) +
		uint32(value.EncodedSize()) + entriesOffsetsSize

	return estimatedSize > b.blockSize
}

// Add adds a key-value pair to the block.
func (b *Builder) Add(key []byte, value y.ValueStruct) error {
	if b.shouldFinishBlock(key, value) {
		if err := b.finishBlock(); err != nil {
			return err
		}
		// Start a new block. Initialize the block.
		b.restarts = append(b.restarts, uint32(b.buf.Len()))
		b.baseKey = []byte{}
		b.baseOffset = uint32(b.buf.Len())
		b.entryOffsets = nil
	}
	b.addHelper(key, value)
	return nil // Currently, there is no meaningful error.
}

// TODO: vvv this was the comment on ReachedCapacity.
// FinalSize returns the *rough* final size of the array, counting the header which is not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(cap int64) bool {
	// 4 for list size and 4 for checksum. TODO: estimate correct size for checksum
	blocksSize := b.buf.Len() + len(b.entryOffsets)*4 + 4 + 4
	estimateSz := blocksSize + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
	return int64(estimateSz) > cap
}

// blockIndex generates the block index for the table.
// It is mainly a list of all the block base offsets.
func (b *Builder) blockIndex() []byte {
	// Store the end offset, so we know the length of the final block.
	b.restarts = append(b.restarts, uint32(b.buf.Len()))

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

// Finish finishes the table by appending the index.
func (b *Builder) Finish() []byte {
	bf := bbloom.New(float64(b.keyCount), 0.01)
	var klen [2]byte
	key := make([]byte, 1024)
	for {
		if _, err := b.keyBuf.Read(klen[:]); err == io.EOF {
			break
		} else if err != nil {
			y.Check(err)
		}
		kl := int(binary.BigEndian.Uint16(klen[:]))
		if cap(key) < kl {
			key = make([]byte, 2*int(kl)) // 2 * uint16 will overflow
		}
		key = key[:kl]
		y.Check2(b.keyBuf.Read(key))
		bf.Add(key)
	}

	b.finishBlock() // This will never start a new block.
	index := b.blockIndex()
	b.buf.Write(index)

	// Write bloom filter.
	bdata := bf.JSONMarshal()
	n, err := b.buf.Write(bdata)
	y.Check(err)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	b.buf.Write(buf[:])

	return b.buf.Bytes()
}
