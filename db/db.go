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
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

// DBOptions are params for creating DB object.
type DBOptions struct {
	Dir                     string
	WriteBufferSize         int   // Memtable size.
	NumLevelZeroTables      int   // Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTablesStall int   // If we hit this number of Level 0 tables, we will stall until level 0 is compacted away.
	LevelOneSize            int64 // Maximum total size for Level 1.
	MaxLevels               int   // Maximum number of levels of compaction. May be made variable later.
	NumCompactWorkers       int   // Number of goroutines ddoing compaction.
	MaxTableSize            int64 // Each table (or file) is at most this size.
	LevelSizeMultiplier     int
	Verbose                 bool
}

var DefaultDBOptions = DBOptions{
	WriteBufferSize:         1 << 20, // Size of each memtable.
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	LevelOneSize:            11 << 20,
	MaxLevels:               7,
	NumCompactWorkers:       3,
	MaxTableSize:            2 << 20,
	LevelSizeMultiplier:     5,
	Verbose:                 true,
}

type DB struct {
	sync.RWMutex // Guards imm, mem.

	imm   *memtable.Memtable // Immutable, memtable being flushed.
	mem   *memtable.Memtable
	immWg sync.WaitGroup // Nonempty when flushing immutable memtable.
	opt   DBOptions
	lc    *levelsController
	vlog  value.Log
}

// NewDB returns a new DB object. Compact levels are created as well.
func NewDB(opt DBOptions) *DB {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &DB{
		mem: memtable.NewMemtable(),
		opt: opt, // Make a copy.
		lc:  newLevelsController(opt),
	}
	vlogPath := filepath.Join(opt.Dir, "vlog")
	out.vlog.Open(vlogPath)
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

type vlogWriter struct {
	vlog    *value.Log
	entries []value.Entry
}

func (s *vlogWriter) Put(key []byte, val []byte) {
	// This shouldn't be necessary. We should be able to just copy data directly from WriteBatch.
	v := make([]byte, len(val)+1)
	v[0] = byteData
	y.AssertTrue(len(val) == copy(v[1:], val))
	s.entries = append(s.entries, value.Entry{
		Key:   key,
		Value: v,
	})
}

func (s *vlogWriter) Delete(key []byte) {
	s.entries = append(s.entries, value.Entry{
		Key:   key,
		Value: []byte{byteDelete},
	})
}

// Write applies a WriteBatch.
func (s *DB) Write(wb *WriteBatch) error {
	writer := &vlogWriter{
		vlog: &s.vlog,
	}
	y.Check(wb.Iterate(writer))
	pt, err := s.vlog.Write(writer.entries)
	y.Check(err)
	y.AssertTrue(len(pt) == wb.Count())

	// For now, we create another WriteBatch.
	// It is possible to avoid this by modifying MemtableInserter.
	wbReduced := NewWriteBatch(len(pt))
	for i := 0; i < len(pt); i++ {
		wbReduced.Put(writer.entries[i].Key, pt[i].Encode())
	}
	if err := s.makeRoomForWrite(); err != nil {
		return err
	}
	return wbReduced.InsertInto(s.mem)
}

// Put puts a key-val pair.
func (s *DB) Put(key []byte, val []byte) error {
	wb := NewWriteBatch(1)
	wb.Put(key, val)
	return s.Write(wb)
}

// Delete deletes a key.
func (s *DB) Delete(key []byte) error {
	wb := NewWriteBatch(1)
	wb.Delete(key)
	return s.Write(wb)
}

func (s *DB) makeRoomForWrite() error {
	if s.mem.MemUsage() < s.opt.WriteBufferSize {
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
		f, err := y.TempFile(s.opt.Dir)
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
