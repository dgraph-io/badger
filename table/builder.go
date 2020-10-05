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
	"crypto/aes"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/v2/fb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	fbs "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
)

const (
	KB = 1024
	MB = KB * 1024

	// When a block is encrypted, it's length increases. We add 256 bytes of padding to
	// handle cases when block size increases. This is an approximate number.
	padding = 256
)

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

// bblock represents a block that is being compressed/encrypted in the background.
type bblock struct {
	data  []byte
	start uint32 // Points to the starting offset of the block.
	end   uint32 // Points to the end offset of the block.
}

// Builder is used in building a table.
type Builder struct {
	// Typically tens or hundreds of meg. This is for one single file.
	buf        []byte
	sz         uint32
	bufLock    sync.Mutex // This lock guards the buf. We acquire lock when we resize the buf.
	actualSize uint32     // Used to store the sum of sizes of blocks after compression/encryption.

	baseKey    []byte // Base key for the current block.
	baseOffset uint32 // Offset for the current block.

	entryOffsets  []uint32 // Offsets of entries present in current block.
	offsets       *z.Buffer
	estimatedSize uint32
	keyHashes     []uint32 // Used for building the bloomfilter.
	opt           *Options
	maxVersion    uint64

	// Used to concurrently compress/encrypt blocks.
	wg        sync.WaitGroup
	blockChan chan *bblock
	blockList []*bblock
}

// NewTableBuilder makes a new TableBuilder.
func NewTableBuilder(opts Options) *Builder {
	b := &Builder{
		// Additional 16 MB to store index (approximate).
		// We trim the additional space in table.Finish().
		// TODO: Switch this buf over to z.Buffer.
		buf:     make([]byte, int(opts.TableSize+16*MB)),
		opt:     &opts,
		offsets: z.NewBuffer(1 << 20),
	}

	// If encryption or compression is not enabled, do not start compression/encryption goroutines
	// and write directly to the buffer.
	if b.opt.Compression == options.None && b.opt.DataKey == nil {
		return b
	}

	count := 2 * runtime.NumCPU()
	b.blockChan = make(chan *bblock, count*2)

	b.wg.Add(count)
	for i := 0; i < count; i++ {
		go b.handleBlock()
	}
	return b
}

func (b *Builder) handleBlock() {
	defer b.wg.Done()

	doCompress := b.opt.Compression != options.None
	for item := range b.blockChan {
		// Extract the block.
		blockBuf := item.data[item.start:item.end]
		// Compress the block.
		if doCompress {
			var err error
			blockBuf, err = b.compressData(blockBuf)
			y.Check(err)
		}
		if b.shouldEncrypt() {
			eBlock, err := b.encrypt(blockBuf, doCompress)
			y.Check(y.Wrapf(err, "Error while encrypting block in table builder."))
			blockBuf = eBlock
		}

		// BlockBuf should always less than or equal to allocated space. If the blockBuf is greater
		// than allocated space that means the data from this block cannot be stored in its
		// existing location and trying to copy it over would mean we would over-write some data
		// of the next block.
		allocatedSpace := (item.end - item.start) + padding + 1
		y.AssertTruef(uint32(len(blockBuf)) <= allocatedSpace, "newend: %d oldend: %d padding: %d",
			item.start+uint32(len(blockBuf)), item.end, padding)

		// Acquire the buflock here. The builder.grow function might change
		// the b.buf while this goroutine was running.
		b.bufLock.Lock()
		// Copy over compressed/encrypted data back to the main buffer.
		copy(b.buf[item.start:], blockBuf)
		b.bufLock.Unlock()

		// Add the actual size of current block.
		atomic.AddUint32(&b.actualSize, uint32(len(blockBuf)))

		// Fix the boundary of the block.
		item.end = item.start + uint32(len(blockBuf))

		if doCompress {
			z.Free(blockBuf)
		}
	}
}

// Close closes the TableBuilder.
func (b *Builder) Close() {
	b.offsets.Release()
}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.sz == 0 }

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b *Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct, vpLen uint32) {
	b.keyHashes = append(b.keyHashes, y.Hash(y.ParseKey(key)))

	if version := y.ParseTs(key); version > b.maxVersion {
		b.maxVersion = version
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

	y.AssertTrue(len(key)-len(diffKey) <= math.MaxUint16)
	y.AssertTrue(len(diffKey) <= math.MaxUint16)

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// store current entry's offset
	y.AssertTrue(b.sz < math.MaxUint32)
	b.entryOffsets = append(b.entryOffsets, b.sz-b.baseOffset)

	// Layout: header, diffKey, value.
	b.append(h.Encode())
	b.append(diffKey)

	if uint32(len(b.buf)) < b.sz+v.EncodedSize() {
		b.grow(v.EncodedSize())
	}
	b.sz += v.Encode(b.buf[b.sz:])

	// Size of KV on SST.
	sstSz := uint32(headerSize) + uint32(len(diffKey)) + v.EncodedSize()
	// Total estimated size = size on SST + size on vlog (length of value pointer).
	b.estimatedSize += (sstSz + vpLen)
}

// grow increases the size of b.buf by atleast 50%.
func (b *Builder) grow(n uint32) {
	l := uint32(len(b.buf))
	if n < l/2 {
		n = l / 2
	}
	newBuf := make([]byte, l+n)
	y.AssertTrue(uint32(len(newBuf)) == l+n)

	b.bufLock.Lock()
	copy(newBuf, b.buf)
	b.buf = newBuf
	b.bufLock.Unlock()
}
func (b *Builder) append(data []byte) {
	// Ensure we have enough space to store new data.
	if uint32(len(b.buf)) < b.sz+uint32(len(data)) {
		b.grow(uint32(len(data)))
	}
	copy(b.buf[b.sz:], data)
	b.sz += uint32(len(data))
}

func (b *Builder) addPadding(sz uint32) {
	if uint32(len(b.buf)) < b.sz+sz {
		b.grow(sz)
	}
	b.sz += sz
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
	if len(b.entryOffsets) == 0 {
		return
	}
	b.append(y.U32SliceToBytes(b.entryOffsets))
	b.append(y.U32ToBytes(uint32(len(b.entryOffsets))))

	b.writeChecksum(b.buf[b.baseOffset:b.sz])

	// If compression/encryption is disabled, no need to send the block to the blockChan.
	// There's nothing to be done.
	if b.blockChan == nil {
		atomic.StoreUint32(&b.actualSize, b.sz)
		b.addBlockToIndex()
		return
	}

	b.addPadding(padding)

	// Block end is the actual end of the block ignoring the padding.
	block := &bblock{start: b.baseOffset, end: uint32(b.sz - padding), data: b.buf}
	b.blockList = append(b.blockList, block)

	b.addBlockToIndex()
	// Push to the block handler.
	b.blockChan <- block
}

func (b *Builder) addBlockToIndex() {
	blockBuf := b.buf[b.baseOffset:b.sz]
	// Add key to the block index.
	builder := fbs.NewBuilder(64)
	off := builder.CreateByteVector(b.baseKey)

	fb.BlockOffsetStart(builder)
	fb.BlockOffsetAddKey(builder, off)
	fb.BlockOffsetAddOffset(builder, b.baseOffset)
	fb.BlockOffsetAddLen(builder, uint32(len(blockBuf)))
	uoff := fb.BlockOffsetEnd(builder)
	builder.Finish(uoff)

	out := builder.FinishedBytes()
	dst := b.offsets.SliceAllocate(len(out))
	copy(dst, out)
}

func (b *Builder) shouldFinishBlock(key []byte, value y.ValueStruct) bool {
	// If there is no entry till now, we will return false.
	if len(b.entryOffsets) <= 0 {
		return false
	}

	// Integer overflow check for statements below.
	y.AssertTrue((uint32(len(b.entryOffsets))+1)*4+4+8+4 < math.MaxUint32)
	// We should include current entry also in size, that's why +1 to len(b.entryOffsets).
	entriesOffsetsSize := uint32((len(b.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	estimatedSize := uint32(b.sz) - b.baseOffset + uint32(6 /*header size for entry*/) +
		uint32(len(key)) + uint32(value.EncodedSize()) + entriesOffsetsSize

	if b.shouldEncrypt() {
		// IV is added at the end of the block, while encrypting.
		// So, size of IV is added to estimatedSize.
		estimatedSize += aes.BlockSize
	}
	// Integer overflow check for table size.
	y.AssertTrue(uint64(b.sz)+uint64(estimatedSize) < math.MaxUint32)

	return estimatedSize > uint32(b.opt.BlockSize)
}

// Add adds a key-value pair to the block.
func (b *Builder) Add(key []byte, value y.ValueStruct, valueLen uint32) {
	if b.shouldFinishBlock(key, value) {
		b.finishBlock()
		// Start a new block. Initialize the block.
		b.baseKey = []byte{}
		y.AssertTrue(uint32(b.sz) < math.MaxUint32)
		b.baseOffset = uint32((b.sz))
		b.entryOffsets = b.entryOffsets[:0]
	}
	b.addHelper(key, value, valueLen)
}

// TODO: vvv this was the comment on ReachedCapacity.
// FinalSize returns the *rough* final size of the array, counting the header which is
// not yet written.
// TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
// at the end. The diff can vary.

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(capacity uint64) bool {
	blocksSize := atomic.LoadUint32(&b.actualSize) + // actual length of current buffer
		uint32(len(b.entryOffsets)*4) + // all entry offsets size
		4 + // count of all entry offsets
		8 + // checksum bytes
		4 // checksum length

	estimateSz := blocksSize +
		4 + // Index length
		uint32(b.offsets.Len())

	return uint64(estimateSz) > capacity
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
func (b *Builder) Finish(allocate bool) []byte {
	b.finishBlock() // This will never start a new block.
	if b.blockChan != nil {
		close(b.blockChan)
	}
	if b.sz == 0 {
		return nil
	}
	// Wait for block handler to finish.
	b.wg.Wait()

	// We have added padding after each block so we should minus the
	// padding from the actual table size. len(blocklist) would be zero if
	// there is no compression/encryption.
	uncompressedSize := b.sz - uint32(padding*len(b.blockList))
	dst := b.buf
	// Fix block boundaries. This includes moving the blocks so that we
	// don't have any interleaving space between them.
	bo, next := []byte{}, 1
	if len(b.blockList) > 0 {
		dstLen := uint32(0)
		for _, bl := range b.blockList {
			bo, next = b.offsets.Slice(next)
			// Length of the block is end minus the start.
			fbo := fb.GetRootAsBlockOffset(bo, 0)
			fbo.MutateLen(bl.end - bl.start)
			// New offset of the block is the point in the main buffer till
			// which we have written data.
			fbo.MutateOffset(dstLen)

			copy(dst[dstLen:], b.buf[bl.start:bl.end])

			// New length is the start of the block plus its length.
			dstLen = fbo.Offset() + fbo.Len()
		}
		y.AssertTrue(next == 0)
		// Start writing to the buffer from the point until which we have valid data.
		// Fix the length because append and writeChecksum also rely on it.
		b.sz = dstLen
	}

	var f y.Filter
	if b.opt.BloomFalsePositive > 0 {
		bits := y.BloomBitsPerKey(len(b.keyHashes), b.opt.BloomFalsePositive)
		f = y.NewFilter(b.keyHashes, bits)
	}
	index := b.buildIndex(f, uncompressedSize)

	var err error
	if b.shouldEncrypt() {
		index, err = b.encrypt(index, false)
		y.Check(err)
	}
	// Write index the buffer.
	b.append(index)
	b.append(y.U32ToBytes(uint32(len(index))))

	b.writeChecksum(index)

	if allocate {
		return append([]byte{}, b.buf[:b.sz]...)
	}
	return b.buf[:b.sz]
}

func (b *Builder) writeChecksum(data []byte) {
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
	b.append(chksum)

	// Write checksum size.
	b.append(y.U32ToBytes(uint32(len(chksum))))
}

// DataKey returns datakey of the builder.
func (b *Builder) DataKey() *pb.DataKey {
	return b.opt.DataKey
}

// encrypt will encrypt the given data and appends IV to the end of the encrypted data.
// This should be only called only after checking shouldEncrypt method.
func (b *Builder) encrypt(data []byte, viaC bool) ([]byte, error) {
	iv, err := y.GenerateIV()
	if err != nil {
		return data, y.Wrapf(err, "Error while generating IV in Builder.encrypt")
	}
	needSz := len(data) + len(iv)
	var dst []byte
	if viaC {
		dst = z.Calloc(needSz)
	} else {
		dst = make([]byte, needSz)
	}
	dst = dst[:len(data)]

	if err = y.XORBlock(dst, data, b.DataKey().Data, iv); err != nil {
		if viaC {
			z.Free(dst)
		}
		return data, y.Wrapf(err, "Error while encrypting in Builder.encrypt")
	}
	if viaC {
		z.Free(data)
	}

	y.AssertTrue(cap(dst)-len(dst) >= len(iv))
	return append(dst, iv...), nil
}

// shouldEncrypt tells us whether to encrypt the data or not.
// We encrypt only if the data key exist. Otherwise, not.
func (b *Builder) shouldEncrypt() bool {
	return b.opt.DataKey != nil
}

// compressData compresses the given data.
func (b *Builder) compressData(data []byte) ([]byte, error) {
	switch b.opt.Compression {
	case options.None:
		return data, nil
	case options.Snappy:
		sz := snappy.MaxEncodedLen(len(data))
		dst := z.Calloc(sz)
		return snappy.Encode(dst, data), nil
	case options.ZSTD:
		sz := y.ZSTDCompressBound(len(data))
		dst := z.Calloc(sz)
		return y.ZSTDCompress(dst, data, b.opt.ZSTDCompressionLevel)
	}
	return nil, errors.New("Unsupported compression type")
}

func (b *Builder) buildIndex(bloom []byte, tableSz uint32) []byte {
	builder := fbs.NewBuilder(3 << 20)

	boList := b.writeBlockOffsets(builder)
	// Write block offset vector the the idxBuilder.
	fb.TableIndexStartOffsetsVector(builder, len(boList))

	// Write individual block offsets.
	for i := 0; i < len(boList); i++ {
		builder.PrependUOffsetT(boList[i])
	}
	boEnd := builder.EndVector(len(boList))

	var bfoff fbs.UOffsetT
	// Write the bloom filter.
	if len(bloom) > 0 {
		bfoff = builder.CreateByteVector(bloom)
	}

	fb.TableIndexStart(builder)
	fb.TableIndexAddOffsets(builder, boEnd)
	fb.TableIndexAddBloomFilter(builder, bfoff)
	fb.TableIndexAddEstimatedSize(builder, b.estimatedSize)
	fb.TableIndexAddMaxVersion(builder, b.maxVersion)
	fb.TableIndexAddUncompressedSize(builder, tableSz)
	fb.TableIndexAddKeyCount(builder, uint32(len(b.keyHashes)))
	builder.Finish(fb.TableIndexEnd(builder))

	return builder.FinishedBytes()
}

// writeBlockOffsets writes all the blockOffets in b.offsets and returns the
// offsets for the newly written items.
func (b *Builder) writeBlockOffsets(builder *fbs.Builder) []fbs.UOffsetT {
	so := b.offsets.SliceOffsets()
	var uoffs []fbs.UOffsetT
	for i := len(so) - 1; i >= 0; i-- {
		// We add these in reverse order.
		data, _ := b.offsets.Slice(so[i])
		uoff := b.writeBlockOffset(builder, data)
		uoffs = append(uoffs, uoff)
	}
	return uoffs
}

// writeBlockOffset writes the given key,offset,len triple to the indexBuilder.
// It returns the offset of the newly written blockoffset.
func (b *Builder) writeBlockOffset(builder *fbs.Builder, data []byte) fbs.UOffsetT {
	// Write the key to the buffer.
	bo := fb.GetRootAsBlockOffset(data, 0)

	k := builder.CreateByteVector(bo.KeyBytes())

	// Build the blockOffset.
	fb.BlockOffsetStart(builder)
	fb.BlockOffsetAddKey(builder, k)
	fb.BlockOffsetAddOffset(builder, bo.Offset())
	fb.BlockOffsetAddLen(builder, bo.Len())
	return fb.BlockOffsetEnd(builder)
}
