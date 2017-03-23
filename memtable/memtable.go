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

package memtable

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/dgraph-io/badger/skl"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// Memtable is a thin wrapper over Skiplist, at least for now.
type Memtable struct {
	table *skl.Skiplist
	arena *y.Arena
}

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	byteData   = 0
	byteDelete = 1
)

// NewMemtable creates a new memtable. Input is the user key comparator.
func NewMemtable() *Memtable {
	return &Memtable{
		arena: new(y.Arena),
		table: skl.NewSkiplist(),
	}
}

// Put sets a key-value pair. We don't use onlyIfAbsent now. And we ignore the old value returned
// by the skiplist. These can be used later on to support more operations, e.g., GetOrCreate can
// be a Put with an empty value with onlyIfAbsent=true.
func (s *Memtable) Put(key, value []byte) {
	data := s.arena.Allocate(1 + len(key) + len(value))
	y.AssertTrue(len(key) == copy(data[:len(key)], key))
	v := data[len(key):]
	v[0] = byteData
	y.AssertTrue(len(value) == copy(v[1:], value))
	s.table.Put(data[:len(key)], unsafe.Pointer(&v), false)
}

// Delete deletes a key from the table.
func (s *Memtable) Delete(key []byte) {
	data := s.arena.Allocate(1 + len(key))
	y.AssertTrue(len(key) == copy(data[:len(key)], key))
	v := data[len(key):]
	v[0] = byteDelete
	s.table.Put(data[:len(key)], unsafe.Pointer(&v), false)
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func (s *Memtable) WriteLevel0Table(f *os.File) error {
	iter := s.NewIterator()
	b := table.TableBuilder{}
	b.Reset()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	f.Write(b.Finish())
	return nil
}

// Iterator is an iterator over memtable.
type Iterator struct {
	iter *skl.Iterator
}

// NewIterator returns a memtable iterator.
func (s *Memtable) NewIterator() *Iterator {
	return &Iterator{iter: s.table.NewIterator()}
}

func (s *Iterator) Seek(key []byte) { s.iter.Seek(key) }
func (s *Iterator) Valid() bool     { return s.iter.Valid() }
func (s *Iterator) SeekToFirst()    { s.iter.SeekToFirst() }
func (s *Iterator) SeekToLast()     { s.iter.SeekToLast() }
func (s *Iterator) Next()           { s.iter.Next() }
func (s *Iterator) Prev()           { s.iter.Prev() }
func (s *Iterator) Key() []byte     { return s.iter.Key() }

// Value returns the value and whether the key is deleted.
func (s *Iterator) Value() []byte {
	v := s.iter.Value()
	y.AssertTrue(v != nil)
	return *(*[]byte)(v)
}

func (s *Iterator) KeyValue() ([]byte, []byte) {
	return s.Key(), s.Value()
}

// IsDeleted returns whether the value returned denotes a deletion.
func IsDeleted(v []byte) bool { return v[0] == byteDelete }

// Get looks up a key. Returns value which could indicate a deletion. If not found, returns nil.
func (s *Memtable) Get(key []byte) []byte {
	v := s.table.Get(key)
	if v == nil {
		// This is different from unsafe.Pointer(nil).
		return nil
	}
	return *(*[]byte)(v)
}

// MemUsage returns an approximate mem usage.
func (s *Memtable) MemUsage() int {
	return s.arena.MemUsage()
}

func (s *Memtable) DebugString() string {
	it := s.NewIterator()
	it.SeekToFirst()
	k1, _ := it.KeyValue()
	it.SeekToLast()
	k2, _ := it.KeyValue()
	return fmt.Sprintf("memtable: %s %s", string(k1), string(k2))
}
