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
	"crypto/aes"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
)

const (
	KB = 1024
	MB = KB * 1024
)

func newBuffer(sz int) *bytes.Buffer {
	b := new(bytes.Buffer)
	b.Grow(sz)
	return b
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Encode encodes the header.
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) Decode(buf []byte) {
	// Copy over data from buf into h. Using *h=unsafe.pointer(...) leads to
	// pointer alignment issues. See https://github.com/dgraph-io/badger/issues/1096
	// and comment https://github.com/dgraph-io/badger/pull/1097#pullrequestreview-307361714
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

// bblock is a builder block.
type bblock struct {
	buf          bytes.Buffer
	baseKey      []byte
	entryOffsets []uint32 // Offsets of entries present in current block.
	// The waitgroup allows us to write block in serial order even if they are
	// processed out of order. The wg is incremented when a block is created
	// and decremented when it is written to the main buffer (in writeBlocks go
	// routine).
	wg sync.WaitGroup
}

func (bb *bblock) reset() {
	bb.buf.Reset()
	bb.entryOffsets = bb.entryOffsets[:0]
	bb.baseKey = bb.baseKey[:0]
}

// Builder is used in building a table.
type Builder struct {
	// Typically tens or hundreds of meg. This is for one single file.
	buf   *bytes.Buffer // This is the main builder buffer
	block *bblock       // Represents a block of the table. New entries are written to block.buf

	tableIndex *pb.TableIndex
	keyHashes  []uint64 // Used for building the bloomfilter.
	opt        *Options

	// Used to concurrently compress/encrypt blocks.
	wg          sync.WaitGroup
	blockChan   chan *bblock
	receiveChan chan *bblock
	totalSize   int // Stores the total size of table.
}

// NewTableBuilder makes a new TableBuilder.
func NewTableBuilder(opts Options) *Builder {
	b := &Builder{
		// Additional 5 MB to store index (approximate).
		buf:        newBuffer(opts.TableSize + 5*MB),
		tableIndex: &pb.TableIndex{},
		keyHashes:  make([]uint64, 0, 1<<20), // Avoid some malloc calls.
		opt:        &opts,
		block:      &bblock{},
	}
	b.block.buf.Grow(opts.BlockSize)

	// If encryption or compression is not enabled, do not start compression/encryption goroutines
	// and write directly to the buffer.
	if b.opt.Compression == options.None && b.opt.DataKey == nil {
		return b
	}

	// Do not move this declaration inside builder struct initialization. It is
	// important that b.blockChan is nil if compression and encryptions both
	// are disabled.
	b.blockChan = make(chan *bblock, 1000)
	b.receiveChan = make(chan *bblock, 1000)
	// The waitgroup will be decremented in the writeblocks goroutine.
	b.block.wg.Add(1)

	count := runtime.NumCPU()
	b.wg.Add(count)
	for i := 0; i < count; i++ {
		// Handleblock encrypts/compresses a block.
		go b.handleBlock()
	}

	b.wg.Add(1)
	// Write block writes a processed block to the main builder buffer.
	go b.writeBlocks()

	return b
}

var slicePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4<<10)
		return &b
	},
}
var blockPool = sync.Pool{New: func() interface{} { return &bblock{} }}

// writeBlocks writes a block to the main builder after after it has been
// processed by the handleblock goroutine.
func (b *Builder) writeBlocks() {
	defer b.wg.Done()
	for bb := range b.receiveChan {
		// Wait for the block to be processed by the handleblock goroutine.
		bb.wg.Wait()

		b.buf.Write(bb.buf.Bytes())
		b.addBlockToIndex(bb)

		blockPool.Put(bb)
	}
}

// handleBlock encrypts/compresses a block.
func (b *Builder) handleBlock() {
	defer b.wg.Done()
	for bb := range b.blockChan {
		blockBuf := bb.buf.Bytes()
		var dst *[]byte
		// Compress the block.
		if b.opt.Compression != options.None {
			var err error

			dst = slicePool.Get().(*[]byte)
			*dst = (*dst)[:0]

			blockBuf, err = b.compressData(*dst, blockBuf)
			y.Check(err)
		}
		if b.shouldEncrypt() {
			eBlock, err := b.encrypt(blockBuf)
			y.Check(y.Wrapf(err, "Error while encrypting block in table builder."))
			blockBuf = eBlock
		}

		bb.buf.Reset()
		// Write the processed block to the original block buffer.
		bb.buf.Write(blockBuf)

		if dst != nil {
			slicePool.Put(dst)
		}
		bb.wg.Done()
	}
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.totalSize == 0 }

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b *Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.block.baseKey); i++ {
		if newKey[i] != b.block.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct, vpLen uint64) {
	b.keyHashes = append(b.keyHashes, farm.Fingerprint64(y.ParseKey(key)))

	// diffKey stores the difference of key with baseKey.
	var diffKey []byte
	if len(b.block.baseKey) == 0 {
		// Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
		// and will have to make copies of keys every time they add to builder, which is even worse.
		b.block.baseKey = append(b.block.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	y.AssertTrue(uint32(b.block.buf.Len()) < math.MaxUint32)

	// store current entry's offset
	b.block.entryOffsets = append(b.block.entryOffsets, uint32(b.block.buf.Len()))

	// Layout: header, diffKey, value.
	encHeader := h.Encode()
	b.block.buf.Write(encHeader)
	b.totalSize += len(encHeader)
	b.block.buf.Write(diffKey)
	b.totalSize += len(diffKey)

	b.totalSize += v.EncodeTo(&b.block.buf)

	// Size of KV on SST.
	sstSz := uint64(uint32(headerSize) + uint32(len(diffKey)) + v.EncodedSize())
	// Total estimated size = size on SST + size on vlog (length of value pointer).
	b.tableIndex.EstimatedSize += (sstSz + vpLen)
}

/*
Structure of Block.
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry1            | Entry2              | Entry3             | Entry4       | Entry5           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Entry6            | ...                 | ...                | ...          | EntryN           |
+-------------------+---------------------+--------------------+--------------+------------------+
| Block Meta(contains list of offsets used| Block Meta Size    | Block        | Checksum Size    |
| to perform binary search in the block)  | (4 Bytes)          | Checksum     | (4 Bytes)        |
+-----------------------------------------+--------------------+--------------+------------------+
*/
// In case the data is encrypted, the "IV" is added to the end of the block.
func (b *Builder) finishBlock() {
	initialSz := b.block.buf.Len()
	b.block.buf.Write(y.U32SliceToBytes(b.block.entryOffsets))
	b.block.buf.Write(y.U32ToBytes(uint32(len(b.block.entryOffsets))))

	writeChecksum(&b.block.buf, b.block.buf.Bytes())

	b.totalSize += b.block.buf.Len() - initialSz

	// If compression/encryption is disabled, no need to send the block to the blockChan.
	// Write the block to the main builder buffer and return.
	if b.blockChan == nil {
		b.buf.Write(b.block.buf.Bytes())
		b.addBlockToIndex(b.block)
		// Reuse the block buffer.
		b.block.reset()
		return
	}

	// Push to the block handler.
	b.blockChan <- b.block
	b.receiveChan <- b.block
}

// addBlockToIndex should be called only after the block has been written to the main buffer.
func (b *Builder) addBlockToIndex(block *bblock) {
	blockBuf := block.buf.Bytes()
	// Add key to the block index.
	bo := &pb.BlockOffset{
		Key:    y.Copy(block.baseKey),
		Offset: uint32(b.buf.Len() - len(blockBuf)),
		Len:    uint32(len(blockBuf)),
	}
	b.tableIndex.Offsets = append(b.tableIndex.Offsets, bo)
}

func (b *Builder) shouldFinishBlock(key []byte, value y.ValueStruct) bool {
	// If there is no entry till now, we will return false.
	if len(b.block.entryOffsets) <= 0 {
		return false
	}

	// Integer overflow check for statements below.
	y.AssertTrue((uint32(len(b.block.entryOffsets))+1)*4+4+8+4 < math.MaxUint32)
	// We should include current entry also in size, that's why +1 to len(b.entryOffsets).
	entriesOffsetsSize := uint32((len(b.block.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	// Add size of the incoming key to the current size of the buffer.
	estimatedSize := uint32(b.block.buf.Len()) + uint32(6 /*header size for entry*/) +
		uint32(len(key)) + uint32(value.EncodedSize()) + entriesOffsetsSize

	if b.shouldEncrypt() {
		// IV is added at the end of the block, while encrypting.
		// So, size of IV is added to estimatedSize.
		estimatedSize += aes.BlockSize
	}
	return estimatedSize > uint32(b.opt.BlockSize)
}

// Add adds a key-value pair to the block.
func (b *Builder) Add(key []byte, value y.ValueStruct, valueLen uint32) {
	if b.shouldFinishBlock(key, value) {
		b.finishBlock()
		// Start a new block. Initialize the block.
		b.block = blockPool.Get().(*bblock)
		b.block.reset()
		b.block.buf.Grow(b.opt.BlockSize)
		b.block.wg.Add(1)
	}
	b.addHelper(key, value, uint64(valueLen))
}

// TODO: vvv this was the comment on ReachedCapacity.
// FinalSize returns the *rough* final size of the array, counting the header which is
// not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity() bool {
	blocksSize := b.totalSize + // length of current buffer
		len(b.block.entryOffsets)*4 + // all entry offsets size
		4 + // count of all entry offsets
		8 + // checksum bytes
		4 // checksum length
	estimateSz := blocksSize + 200 // Approximate size of the index

	return estimateSz > b.opt.TableSize
}

// Finish finishes the table by appending the index.
/*
The table structure looks like
+---------+------------+-----------+---------------+
| Block 1 | Block 2    | Block 3   | Block 4       |
+---------+------------+-----------+---------------+
| Block 5 | Block 6    | Block ... | Block N       |
+---------+------------+-----------+---------------+
| Index   | Index Size | Checksum  | Checksum Size |
+---------+------------+-----------+---------------+
*/
// In case the data is encrypted, the "IV" is added to the end of the index.
func (b *Builder) Finish() []byte {
	bf := z.NewBloomFilter(float64(len(b.keyHashes)), b.opt.BloomFalsePositive)
	for _, h := range b.keyHashes {
		bf.Add(h)
	}
	// Add bloom filter to the index.
	b.tableIndex.BloomFilter = bf.JSONMarshal()

	b.finishBlock() // This will never start a new block.

	if b.blockChan != nil {
		close(b.blockChan)
		close(b.receiveChan)
	}
	// Wait for block handler to finish.
	b.wg.Wait()

	index, err := proto.Marshal(b.tableIndex)
	y.Check(err)

	if b.shouldEncrypt() {
		index, err = b.encrypt(index)
		y.Check(err)
	}
	// Write index the buffer.
	b.buf.Write(index)
	b.buf.Write(y.U32ToBytes(uint32(len(index))))

	writeChecksum(b.buf, index)
	return b.buf.Bytes()
}

// writeChecksum writes the checksum for "data" and its length to the "buf" buffer.
func writeChecksum(buf *bytes.Buffer, data []byte) {
	// Build checksum for the index.
	checksum := pb.Checksum{
		// TODO: The checksum type should be configurable from the
		// options.
		// We chose to use CRC32 as the default option because
		// it performed better compared to xxHash64.
		// See the BenchmarkChecksum in table_test.go file
		// Size     =>   1024 B        2048 B
		// CRC32    => 63.7 ns/op     112 ns/op
		// xxHash64 => 87.5 ns/op     158 ns/op
		Sum:  y.CalculateChecksum(data, pb.Checksum_CRC32C),
		Algo: pb.Checksum_CRC32C,
	}

	// Write checksum to the file.
	chksum, err := proto.Marshal(&checksum)
	y.Check(err)
	buf.Write(chksum)

	// Write checksum size.
	buf.Write(y.U32ToBytes(uint32(len(chksum))))
}

// DataKey returns datakey of the builder.
func (b *Builder) DataKey() *pb.DataKey {
	return b.opt.DataKey
}

// encrypt will encrypt the given data and appends IV to the end of the encrypted data.
// This should be only called only after checking shouldEncrypt method.
func (b *Builder) encrypt(data []byte) ([]byte, error) {
	iv, err := y.GenerateIV()
	if err != nil {
		return data, y.Wrapf(err, "Error while generating IV in Builder.encrypt")
	}
	data, err = y.XORBlock(data, b.DataKey().Data, iv)
	if err != nil {
		return data, y.Wrapf(err, "Error while encrypting in Builder.encrypt")
	}
	data = append(data, iv...)
	return data, nil
}

// shouldEncrypt tells us whether to encrypt the data or not.
// We encrypt only if the data key exist. Otherwise, not.
func (b *Builder) shouldEncrypt() bool {
	return b.opt.DataKey != nil
}

// compressData compresses the given data.
func (b *Builder) compressData(dst, data []byte) ([]byte, error) {
	switch b.opt.Compression {
	case options.None:
		return data, nil
	case options.Snappy:
		return snappy.Encode(dst, data), nil
	case options.ZSTD:
		return y.ZSTDCompress(dst, data, b.opt.ZSTDCompressionLevel)
	}
	return nil, errors.New("Unsupported compression type")
}
