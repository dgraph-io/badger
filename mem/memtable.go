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

// TODO: Consider merging memtable and skl.
package mem

import (
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/skl"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// Table is a thin wrapper over Skiplist, at least for now.
type Table struct {
	table *skl.Skiplist
	size  int64
}

// NewTable creates a new memtable. Input is the user key comparator.
func NewTable() *Table {
	return &Table{
		table: skl.NewSkiplist(),
	}
}

// Put sets a key-value pair. We don't use onlyIfAbsent now. And we ignore the old value returned
// by the skiplist. These can be used later on to support more operations, e.g., GetOrCreate can
// be a Put with an empty value with onlyIfAbsent=true.
func (s *Table) Put(key, value []byte, meta byte) {
	atomic.AddInt64(&s.size, int64(len(key)+len(value)+1)) // This is not right when key already exists.
	s.table.Put(key, unsafe.Pointer(&value), meta)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func (s *Table) WriteLevel0Table(f *os.File) error {
	iter := s.NewIterator()
	defer iter.Close()
	b := table.NewTableBuilder()
	defer b.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		val, meta := iter.Value()
		if err := b.Add(iter.Key(), val, meta); err != nil {
			return err
		}
	}
	var buf [2]byte // Level 0. Leave it initialized as 0.
	_, err := f.Write(b.Finish(buf[:]))
	return err
}

func (s *Table) IncrRef() {
	s.table.IncrRef()
}

func (s *Table) DecrRef() {
	s.table.DecrRef()
}

// Iterator is an iterator over memtable.
type Iterator struct {
	iter *skl.Iterator
}

// NewIterator returns a memtable iterator.
func (s *Table) NewIterator() *Iterator {
	return &Iterator{iter: s.table.NewIterator()}
}

func (s *Iterator) Name() string { return "MemtableIterator" }

func (s *Iterator) Seek(key []byte) { s.iter.Seek(key) }
func (s *Iterator) Valid() bool     { return s.iter.Valid() }
func (s *Iterator) SeekToFirst()    { s.iter.SeekToFirst() }
func (s *Iterator) SeekToLast()     { s.iter.SeekToLast() }
func (s *Iterator) Next()           { s.iter.Next() }
func (s *Iterator) Prev()           { s.iter.Prev() }
func (s *Iterator) Key() []byte     { return s.iter.Key() }
func (s *Iterator) Close()          { s.iter.Close() }

// Value returns the value and whether the key is deleted.
func (s *Iterator) Value() ([]byte, byte) {
	v, meta := s.iter.Value()
	y.AssertTrue(v != nil)
	return *(*[]byte)(v), meta
}

// Get looks up a key. This includes the meta byte.
func (s *Table) Get(key []byte) ([]byte, byte) {
	v, meta := s.table.Get(key)
	if v == nil {
		// This is different from unsafe.Pointer(nil).
		return nil, meta
	}
	return *(*[]byte)(v), meta
}

// Size returns number of bytes used for keys and values.
func (s *Table) Size() int64 {
	return atomic.LoadInt64(&s.size)
}
