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

package badger

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/y"
)

// Similar to memtable. Might go to y.
const (
	headerSize = 4
)

type WriteBatch struct {
	rep []byte
}

func NewWriteBatch(reserved int) *WriteBatch {
	return &WriteBatch{
		rep: make([]byte, headerSize, 100),
	}
}

func (s *WriteBatch) Clear() {
	s.rep = s.rep[:headerSize]
	for i := 0; i < headerSize; i++ {
		s.rep[i] = 0
	}
}

func (s *WriteBatch) Count() int { return int(binary.BigEndian.Uint32(s.rep)) }

func (s *WriteBatch) SetCount(n int) {
	binary.BigEndian.PutUint32(s.rep[:4], uint32(n))
}

func (s *WriteBatch) Put(key []byte, val []byte) {
	s.RawPut(key, 0, val)
}

func (s *WriteBatch) Delete(key []byte) {
	s.RawPut(key, y.BitDelete, nil)
}

func (s *WriteBatch) RawPut(key []byte, headerByte byte, val []byte) {
	s.SetCount(s.Count() + 1)

	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], uint64(len(key)))
	s.rep = append(s.rep, tmp[:n]...)
	s.rep = append(s.rep, key...)

	s.rep = append(s.rep, headerByte)

	if val != nil {
		y.AssertTrue((headerByte & y.BitDelete) == 0)
		n = binary.PutUvarint(tmp[:], uint64(len(val)))
		s.rep = append(s.rep, tmp[:n]...)
		s.rep = append(s.rep, val...)
	} else {
		y.AssertTrue((headerByte & y.BitDelete) != 0)
	}
}

func (s *WriteBatch) Iterate(h WriteBatchHandler) error {
	input := s.rep[headerSize:]
	var key, val []byte
	n := s.Count()
	for i := 0; i < n; i++ {
		key, input = y.GetLengthPrefixedSlice(input)
		headerByte := input[0]
		input = input[1:]
		if (headerByte & y.BitDelete) == 0 {
			val, input = y.GetLengthPrefixedSlice(input)
			h.RawPut(key, headerByte, val)
		} else {
			h.RawPut(key, headerByte, nil)
		}
	}
	return nil
}

func (s *WriteBatch) InsertInto(mem *memtable.Memtable) error {
	inserter := &MemtableInserter{mem: mem}
	return s.Iterate(inserter)
}

func (s *WriteBatch) SetContents(contents []byte) {
	y.AssertTrue(len(contents) >= headerSize)
	s.rep = contents
}

// Append adds input w to this WriteBatch.
func (s *WriteBatch) Append(w *WriteBatch) {
	s.SetCount(s.Count() + w.Count())
	s.rep = append(s.rep, w.rep[headerSize:]...)
}

// WriteBatch's Iterate will communicate with this interface.
type WriteBatchHandler interface {
	RawPut(key []byte, headerByte byte, val []byte)
}

// MemtableInserter is a WriteBatchHandler. Applies WriteBatch to memtable.
type MemtableInserter struct {
	mem *memtable.Memtable
}

func (s *MemtableInserter) RawPut(key []byte, headerByte byte, val []byte) {
	// This WriteBatch logic is unnecessarily complicated. Need to clean up.
	newVal := make([]byte, len(val)+1)
	newVal[0] = headerByte
	y.AssertTrue(len(val) == copy(newVal[1:], val))
	s.mem.Put(key, newVal)
}
