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
	ValueThreshold          int // If value size >= this threshold, we store offsets in value log.
	Verbose                 bool
}

var DefaultDBOptions = DBOptions{
	Dir:                     "/tmp",
	WriteBufferSize:         1 << 20, // Size of each memtable.
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	LevelOneSize:            11 << 20,
	MaxLevels:               7,
	NumCompactWorkers:       3,
	MaxTableSize:            2 << 20,
	LevelSizeMultiplier:     5,
	ValueThreshold:          20,
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

func (s *DB) getValueHelper(key []byte) []byte {
	mem, imm := s.getMemImm() // Lock should be released.
	if mem != nil {
		if v := mem.Get(key); v != nil {
			return v
		}
	}
	if imm != nil {
		if v := s.imm.Get(key); v != nil {
			return v
		}
	}
	return s.lc.get(key)
}

func decodeValue(val []byte, vlog *value.Log) []byte {
	if (val[0] & y.BitDelete) != 0 {
		// Tombstone encountered.
		return nil
	}
	if (val[0] & y.BitValueOffset) != 0 {
		var vp value.Pointer
		vp.Decode(val[1:])
		var out []byte
		vlog.Read(vp, func(entry value.Entry) {
			if (entry.Value[0] & y.BitDelete) == 0 { // Not tombstone.
				out = entry.Value[1:]
			}
		})
		return out
	}
	return val[1:]
}

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	val := s.getValueHelper(key)
	if val == nil {
		return nil
	}
	return decodeValue(val, &s.vlog)
}

type vlogWriter struct {
	vlog    *value.Log
	entries []value.Entry
}

/*
Currently, there are two WriteBatches. The first one has values=byteData/byteDelete + value.
The vlogWriter here pushes WriteBatch into valueLog and gets Pointers.
The data sent to valueLog is the same: byteData/byteDelete + value.
After we get these Pointers, we want to insert that into our memtable.
*/
func (s *vlogWriter) RawPut(key []byte, headerByte byte, val []byte) {
	v := make([]byte, len(val)+1)
	v[0] = headerByte
	y.AssertTrue(len(val) == copy(v[1:], val))
	s.entries = append(s.entries, value.Entry{
		Key:   key,
		Value: v,
	})
}

// Reduces original WriteBatch containing value data to WriteBatch containing value offsets.
type writebatchRewriter struct {
	out  *WriteBatch
	ptrs []value.Pointer
	idx  int
	db   *DB
}

func (s *writebatchRewriter) RawPut(key []byte, headerByte byte, val []byte) {
	if (headerByte & y.BitDelete) != 0 {
		s.out.RawPut(key, headerByte, nil)
	} else if len(val) >= s.db.opt.ValueThreshold {
		s.out.RawPut(key, y.BitValueOffset, s.ptrs[s.idx].Encode())
	} else {
		s.out.RawPut(key, 0, val)
	}
	s.idx++
}

// Write applies a WriteBatch.
func (s *DB) Write(wb *WriteBatch) error {
	writer := vlogWriter{
		vlog: &s.vlog,
	}
	y.Check(wb.Iterate(&writer))
	ptrs, err := s.vlog.Write(writer.entries)
	y.Check(err)
	y.AssertTrue(len(ptrs) == wb.Count())

	// Create a new WriteBatch for offsets. Could consider using a different inserter for WriteBatch.
	wbReduced := NewWriteBatch(len(ptrs))
	writebatchRewriter := writebatchRewriter{
		out:  wbReduced,
		ptrs: ptrs,
		db:   s,
	}
	y.Check(wb.Iterate(&writebatchRewriter))
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

type DBIterator struct {
	it y.Iterator
	db *DB
}

func (s *DBIterator) Next() {
	s.it.Next()
	s.nextValid()
}

func (s *DBIterator) SeekToFirst() {
	s.it.SeekToFirst()
	s.nextValid()
}

func (s *DBIterator) Seek(key []byte) {
	s.it.Seek(key)
	s.nextValid()
}

func (s *DBIterator) Valid() bool { return s.it.Valid() }

func (s *DBIterator) KeyValue() ([]byte, []byte) {
	key, val := s.it.KeyValue()
	return key, decodeValue(val, &s.db.vlog)
}

func (s *DBIterator) nextValid() {

}

func (s *DBIterator) Name() string { return "DBIterator" }

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
	return &DBIterator{
		it: y.NewMergeIterator(iters),
		db: s,
	}
}
