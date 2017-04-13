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
	//	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

type keyOffset struct {
	key    []byte
	offset int64
	len    int64
}

type Table struct {
	sync.Mutex

	offset    int64
	fd        *os.File // Do not own.
	tableSize int64    // Initialized in OpenTable, using fd.Stat().

	blockIndex []keyOffset

	metadata []byte
}

type Block struct {
	offset int64
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
func OpenTable(fd *os.File) (*Table, error) {
	t := &Table{
		fd:     fd,
		offset: 0, // Consider removing this field.
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	t.tableSize = fileInfo.Size()

	if err := t.readIndex(); err != nil {
		return nil, err
	}
	return t, nil
}

// SetMetadata updates our metadata to the new metadata.
// For now, they must be of the same size.
func (t *Table) SetMetadata(meta []byte) error {
	y.AssertTrue(len(meta) == len(t.metadata))
	pos := int64(t.offset + t.tableSize - 4 - int64(len(t.metadata)))
	written, err := t.fd.WriteAt(meta, pos)
	y.AssertTrue(written == len(meta))
	return err
}

func (t *Table) readIndex() error {
	buf := make([]byte, 4)
	readPos := int64(t.offset + t.tableSize - 4)
	if _, err := t.fd.ReadAt(buf, t.offset+t.tableSize-4); err != nil {
		return errors.Wrap(err, "While reading metadata size")
	}
	metadataSize := int64(binary.BigEndian.Uint32(buf))
	t.metadata = make([]byte, metadataSize)
	readPos -= int64(len(t.metadata))
	if _, err := t.fd.ReadAt(t.metadata, readPos); err != nil {
		return errors.Wrap(err, "While reading metadata")
	}

	readPos -= 4
	if _, err := t.fd.ReadAt(buf, readPos); err != nil {
		return errors.Wrap(err, "While reading block index")
	}
	restartsLen := int(binary.BigEndian.Uint32(buf))

	buf = make([]byte, 4*restartsLen)
	readPos -= int64(len(buf))
	if _, err := t.fd.ReadAt(buf, readPos); err != nil {
		return errors.Wrap(err, "While reading block index")
	}

	offsets := make([]uint32, restartsLen)
	for i := 0; i < restartsLen; i++ {
		offsets[i] = binary.BigEndian.Uint32(buf[:4])
		buf = buf[4:]
	}

	// The last offset stores the end of the last block.
	for i := 0; i < len(offsets); i++ {
		var o int64
		if i == 0 {
			o = 0
		} else {
			o = int64(offsets[i-1])
		}

		ko := keyOffset{
			offset: o,
			len:    int64(offsets[i]) - o,
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

			offset := t.offset + ko.offset
			buf := make([]byte, h.Size())
			if _, err := t.fd.ReadAt(buf, offset); err != nil {
				che <- errors.Wrap(err, "While reading first header in block")
				return
			}

			h.Decode(buf)
			y.AssertTrue(h.plen == 0)

			offset += int64(h.Size())
			buf = make([]byte, h.klen)
			if _, err := t.fd.ReadAt(buf, offset); err != nil {
				che <- errors.Wrap(err, "While reading first key in block")
				return
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

	// TODO: add Block caching here.
	block := Block{
		offset: ko.offset + t.offset,
		data:   make([]byte, int(ko.len)),
	}
	if _, err := t.fd.ReadAt(block.data, block.offset); err != nil {
		return block, err
	}
	return block, nil
}

func (t *Table) NewIterator() *TableIterator {
	return &TableIterator{t: t}
}

func (t *Table) Size() int64 { return t.tableSize }
