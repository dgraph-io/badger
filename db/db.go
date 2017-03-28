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
	"io/ioutil"
	"os"
	"sync"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

// DBOptions are params for creating DB object.
type DBOptions struct {
	WriteBufferSize int
	CompactOpt      CompactOptions
}

var DefaultDBOptions = DBOptions{
	WriteBufferSize: 1 << 20, // Size of each memtable.
	CompactOpt:      DefaultCompactOptions(),
}

type DB struct {
	sync.RWMutex // Guards imm, mem.

	imm       *memtable.Memtable // Immutable, memtable being flushed.
	mem       *memtable.Memtable
	immWg     sync.WaitGroup // Nonempty when flushing immutable memtable.
	dbOptions DBOptions
	lc        *levelsController
	vlog      value.Log
}

// NewDB returns a new DB object. Compact levels are created as well.
func NewDB(opt DBOptions) *DB {
	out := &DB{
		mem:       memtable.NewMemtable(),
		dbOptions: opt, // Make a copy.
		lc:        newLevelsController(opt.CompactOpt),
	}
	out.vlog.Open("/tmp/vlog")
	return out
}

// Close closes a DB.
func (s *DB) Close() {
	// For now, we just delete the value log. This should be removed eventually.
	os.Remove("/tmp/vlog")
}

func (s *DB) getMemImm() (*memtable.Memtable, *memtable.Memtable) {
	s.RLock()
	defer s.RUnlock()
	return s.mem, s.imm
}

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	mem, imm := s.getMemImm() // Lock should be released.
	if mem != nil {
		if v := mem.Get(key); v != nil {
			// v is not nil means we either have an explicit deletion or we have a value.
			// v is nil means there is nothing about "key" in "mem". We need to look deeper.
			return y.ExtractValue(v)
		}
	}
	if imm != nil {
		if v := s.imm.Get(key); v != nil {
			return y.ExtractValue(v)
		}
	}

	// Check disk.
	out := s.lc.get(key)
	if out == nil {
		return nil
	}
	return y.ExtractValue(out)
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
	entry := value.Entry{
		Key:   key,
		Value: val,
	}
	pt, err := s.vlog.Write([]value.Entry{entry})
	y.Check(err)
	y.AssertTrue(len(pt) == 1)

	wb := NewWriteBatch(0)
	wb.Put(key, pt[0].Encode())
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

	s.Lock()
	// Changing imm, mem requires lock.
	s.imm = s.mem
	s.mem = memtable.NewMemtable()

	// Note: It is important to start the compaction within this lock.
	// Otherwise, you might be compactng the wrong imm!
	s.immWg.Add(1)
	go func() {
		defer s.immWg.Done()
		f, err := ioutil.TempFile("", "badger") // TODO: Stop using temp files.
		// TODO: Add file closing logic. Maybe use runtime finalizer and let GC close the file.
		y.Check(err)
		y.Check(s.imm.WriteLevel0Table(f))
		tbl, err := newTableHandler(f)
		y.Check(err)
		s.lc.addLevel0Table(tbl)
	}()
	s.Unlock()
	return nil
}

// NewIterator returns a MergeIterator over iterators of memtable and compaction levels.
func (s *DB) NewIterator() y.Iterator {
	// The order we add these iterators is important.
	// Imagine you add level0 first, then add imm. In between, the initial imm might be moved into
	// level0, and be completely missed. On the other hand, if you add imm first and it got moved
	// to level 0, you would just have that data appear twice which is fine.
	mem, imm := s.getMemImm()
	var iters []y.Iterator
	if mem != nil {
		iters = append(iters, mem.NewIterator())
	}
	if imm != nil {
		iters = append(iters, imm.NewIterator())
	}
	iters = s.lc.AppendIterators(iters)
	return y.NewMergeIterator(iters)
}
