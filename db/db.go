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

package db

import (
	"os"
	"sync"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/y"
)

type DBOptions struct {
	WriteBufferSize int
}

var DefaultDBOptions = &DBOptions{
	WriteBufferSize: 1 << 10,
}

type DB struct {
	imm       *memtable.Memtable // Immutable, memtable being flushed.
	mem       *memtable.Memtable
	immWg     sync.WaitGroup // Nonempty when flushing immutable memtable.
	dbOptions DBOptions
}

func NewDB(opt *DBOptions) *DB {
	// VersionEdit strongly tied to table files. Omit for now.
	db := &DB{
		mem:       memtable.NewMemtable(),
		dbOptions: *opt, // Make a copy.
	}
	return db
}

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	if v := s.mem.Get(key); v != nil {
		// v is not nil means we either have an explicit deletion or we have a value.
		// v is nil means there is nothing about "key" in "mem". We need to look deeper.
		return memtable.ExtractValue(v)
	}
	if v := s.imm.Get(key); v != nil {
		return memtable.ExtractValue(v)
	}
	// TODO: Get data from disk.
	return nil
}

// Write applies a WriteBatch.
func (s *DB) Write(wb *WriteBatch) error {
	if err := s.makeRoomForWrite(); err != nil {
		return err
	}
	return wb.InsertInto(s.mem)
}

// Put puts a key-val pair.
func (s *DB) Put(key []byte, val []byte) error {
	wb := NewWriteBatch(0)
	wb.Put(key, val)
	return s.Write(wb)
}

// Delete deletes a key.
func (s *DB) Delete(key []byte) error {
	wb := NewWriteBatch(0)
	wb.Delete(key)
	return s.Write(wb)
}

func (s *DB) makeRoomForWrite() error {
	if s.mem.MemUsage() < s.dbOptions.WriteBufferSize {
		// Nothing to do. We have enough space.
		return nil
	}
	s.immWg.Wait() // Make sure we finish flushing immutable memtable.
	s.imm = s.mem
	s.mem = memtable.NewMemtable()
	s.compactMemtable() // This is for imm.
	return nil
}

func (s *DB) compactMemtable() {
	y.AssertTrue(s.imm != nil)
	s.immWg.Add(1)
	go func() {
		defer s.immWg.Done()
		f, err := os.Create("/tmp/l0") // Fix later.
		y.Check(err)
		defer f.Close()
		y.Check(s.imm.WriteLevel0Table(f))
	}()
}
