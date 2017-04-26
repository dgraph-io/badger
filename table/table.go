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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

const fileSuffix = ".sst"

const (
	Nothing = iota
	MemoryMap
	LoadToRAM
)

type keyOffset struct {
	key    []byte
	offset int
	len    int
}

type Table struct {
	sync.Mutex

	fd        *os.File // Own fd.
	tableSize int      // Initialized in OpenTable, using fd.Stat().

	blockIndex []keyOffset
	metadata   []byte
	ref        int32 // For file garbage collection.

	mapTableTo int
	mmap       []byte // Memory mapped.

	// The following are initialized once and const.
	smallest, biggest []byte // Smallest and largest keys.
	id                uint64
}

func (s *Table) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Table) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef == 0 {
		// We can safely delete this file, because for all the current files, we always have
		// at least one reference pointing to them.
		filename := s.fd.Name()
		y.Check(s.fd.Close())
		os.Remove(filename)
	}
}

type Block struct {
	offset int
	data   []byte
}

func (b Block) NewIterator() *BlockIterator {
	return &BlockIterator{data: b.data}
}

type byKey []keyOffset

func (b byKey) Len() int               { return len(b) }
func (b byKey) Swap(i int, j int)      { b[i], b[j] = b[j], b[i] }
func (b byKey) Less(i int, j int) bool { return bytes.Compare(b[i].key, b[j].key) < 0 }

// OpenTable assumes file has only one table and opens it.
func OpenTable(fd *os.File, mapTableTo int) (*Table, error) {
	id, ok := ParseFileID(fd.Name())
	if !ok {
		return nil, y.Errorf("Invalid filename: %s", fd.Name())
	}
	t := &Table{
		fd:         fd,
		ref:        1, // Caller is given one reference.
		id:         id,
		mapTableTo: mapTableTo,
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	t.tableSize = int(fileInfo.Size())

	if mapTableTo == MemoryMap {
		t.mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(fileInfo.Size()),
			syscall.PROT_READ, syscall.MAP_PRIVATE|syscall.MAP_POPULATE)
		//		t.mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(fileInfo.Size()),
		//			syscall.PROT_READ, syscall.MAP_PRIVATE)
		if err != nil {
			y.Fatalf("Unable to map file: %v", err)
		}
	} else if mapTableTo == LoadToRAM {
		t.LoadToRAM()
	}

	if err := t.readIndex(); err != nil {
		return nil, err
	}

	it := t.NewIterator(false)
	defer it.Close()
	it.Rewind()
	if it.Valid() {
		t.smallest = it.Key()
	}

	it2 := t.NewIterator(true)
	defer it2.Close()
	it2.Rewind()
	if it2.Valid() {
		t.biggest = it2.Key()
	}
	return t, nil
}

func (s *Table) Close() {
	s.fd.Close()
	if s.mapTableTo == MemoryMap {
		syscall.Munmap(s.mmap)
	}
}

// SetMetadata updates our metadata to the new metadata.
// For now, they must be of the same size.
func (t *Table) SetMetadata(meta []byte) error {
	y.AssertTrue(len(meta) == len(t.metadata))
	pos := t.tableSize - 4 - len(t.metadata)
	written, err := t.fd.WriteAt(meta, int64(pos))
	y.AssertTrue(written == len(meta))
	return err
}

var EOF = errors.New("End of mapped region")

func (t *Table) read(off int, sz int) ([]byte, error) {
	if t.mmap != nil {
		if len(t.mmap[off:]) < sz {
			return nil, EOF
		}
		return t.mmap[off : off+sz], nil
	}

	res := make([]byte, sz)
	_, err := t.fd.ReadAt(res, int64(off))
	return res, err
}

func (t *Table) readNoFail(off int, sz int) []byte {
	res, err := t.read(off, sz)
	y.Check(err)
	return res
}

func (t *Table) readIndex() error {
	readPos := t.tableSize - 4
	buf := t.readNoFail(t.tableSize-4, 4)

	metadataSize := int(binary.BigEndian.Uint32(buf))
	readPos -= metadataSize
	t.metadata = t.readNoFail(readPos, metadataSize)

	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	restartsLen := int(binary.BigEndian.Uint32(buf))

	readPos -= 4 * restartsLen
	buf = t.readNoFail(readPos, 4*restartsLen)

	offsets := make([]int, restartsLen)
	for i := 0; i < restartsLen; i++ {
		offsets[i] = int(binary.BigEndian.Uint32(buf[:4]))
		buf = buf[4:]
	}

	// The last offset stores the end of the last block.
	for i := 0; i < len(offsets); i++ {
		var o int
		if i == 0 {
			o = 0
		} else {
			o = offsets[i-1]
		}

		ko := keyOffset{
			offset: o,
			len:    offsets[i] - o,
		}
		t.blockIndex = append(t.blockIndex, ko)
	}

	if len(t.blockIndex) == 1 {
		return nil
	}

	che := make(chan error, len(t.blockIndex))
	for i := 0; i < len(t.blockIndex); i++ {

		bo := &t.blockIndex[i]
		go func(ko *keyOffset) {
			var h header

			offset := ko.offset
			buf, err := t.read(offset, h.Size())
			if err != nil {
				che <- errors.Wrap(err, "While reading first header in block")
				return
			}

			h.Decode(buf)
			y.AssertTrue(h.plen == 0)

			offset += h.Size()
			buf = make([]byte, h.klen)
			if out, err := t.read(offset, h.klen); err != nil {
				che <- errors.Wrap(err, "While reading first key in block")
				return
			} else {
				y.AssertTrue(len(buf) == copy(buf, out))
			}

			ko.key = buf
			che <- nil
		}(bo)
	}

	for _ = range t.blockIndex {
		err := <-che
		if err != nil {
			return err
		}
	}
	sort.Sort(byKey(t.blockIndex))
	return nil
}

// Metadata returns metadata. Do not mutate this.
func (t *Table) Metadata() []byte { return t.metadata }

func (t *Table) block(idx int) (Block, error) {
	y.AssertTruef(idx >= 0, "idx=%d", idx)
	if idx >= len(t.blockIndex) {
		return Block{}, errors.New("Block out of index.")
	}

	ko := t.blockIndex[idx]
	block := Block{
		offset: ko.offset,
	}
	var err error
	if block.data, err = t.read(block.offset, ko.len); err != nil {
		return block, err
	}
	return block, nil
}

func (t *Table) Size() int64      { return int64(t.tableSize) }
func (t *Table) Smallest() []byte { return t.smallest }
func (t *Table) Biggest() []byte  { return t.biggest }
func (t *Table) Filename() string { return t.fd.Name() }
func (t *Table) ID() uint64       { return t.id }

func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	y.AssertTrue(id >= 0)
	return uint64(id), true
}

func NewFilename(id uint64, dir string) string {
	return filepath.Join(dir, fmt.Sprintf("%06d", id)+fileSuffix)
}

func (t *Table) LoadToRAM() {
	t.mmap = make([]byte, t.tableSize)
	read, err := t.fd.ReadAt(t.mmap, 0)
	if err != nil || read != t.tableSize {
		y.Fatalf("Unable to load file in memory: %v. Read: %v", err, read)
	}
}
