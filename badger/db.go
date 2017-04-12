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
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/mem"
	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

// Options are params for creating DB object.
type Options struct {
	Dir                     string
	NumLevelZeroTables      int   // Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTablesStall int   // If we hit this number of Level 0 tables, we will stall until level 0 is compacted away.
	LevelOneSize            int64 // Maximum total size for Level 1.
	MaxLevels               int   // Maximum number of levels of compaction. May be made variable later.
	NumCompactWorkers       int   // Number of goroutines ddoing compaction.
	MaxTableSize            int64 // Each table (or file) is at most this size.
	LevelSizeMultiplier     int
	ValueThreshold          int // If value size >= this threshold, we store offsets in value log.
	Verbose                 bool
	DoNotCompact            bool
}

var DefaultOptions = Options{
	Dir:                     "/tmp",
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	LevelOneSize:            256 << 20,
	MaxLevels:               7,
	NumCompactWorkers:       3, // Max possible = num levels / 2.
	MaxTableSize:            64 << 20,
	LevelSizeMultiplier:     10,
	ValueThreshold:          20,
	Verbose:                 true,
	DoNotCompact:            false, // Only for testing.
}

type DB struct {
	sync.RWMutex // Guards imm, mem.

	imm     *mem.Table // Immutable, memtable being flushed.
	mt      *mem.Table
	immWg   sync.WaitGroup // Nonempty when flushing immutable memtable.
	opt     Options
	lc      *levelsController
	vlog    value.Log
	voffset uint64
}

// NewDB returns a new DB object. Compact levels are created as well.
func NewDB(opt Options) *DB {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &DB{
		mt:  mem.NewTable(),
		opt: opt, // Make a copy.
		lc:  newLevelsController(opt),
	}
	vlogPath := filepath.Join(opt.Dir, "vlog")
	out.vlog.Open(vlogPath)

	val := out.Get(Head)
	var voffset uint64
	if len(val) == 0 {
		voffset = 0
	} else {
		voffset = binary.BigEndian.Uint64(val)
	}

	fn := func(k, v []byte, meta byte) {
		out.mt.Put(k, v, meta)
	}
	out.vlog.Replay(voffset, fn)

	return out
}

// Close closes a DB.
func (s *DB) Close() {
}

func (s *DB) getMemTables() (*mem.Table, *mem.Table) {
	s.RLock()
	defer s.RUnlock()
	return s.mt, s.imm
}

func decodeValue(val []byte, vlog *value.Log) []byte {
	if (val[0] & value.BitDelete) != 0 {
		// Tombstone encountered.
		return nil
	}
	if (val[0] & value.BitValuePointer) == 0 {
		return val[1:]
	}

	var vp value.Pointer
	vp.Decode(val[1:])
	entry, err := vlog.Read(vp)
	y.Checkf(err, "Unable to read from value log: %+v", vp)

	if (entry.Value[0] & value.BitDelete) == 0 { // Not tombstone.
		return entry.Value
	}
	return []byte{}
}

// getValueHelper returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *DB) get(key []byte) []byte {
	mem, imm := s.getMemTables() // Lock should be released.
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

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	val := s.get(key)
	if val == nil {
		return nil
	}
	return decodeValue(val, &s.vlog)
}

func (s *DB) updateOffset(ptrs []value.Pointer) {
	ptr := ptrs[len(ptrs)-1]

	s.Lock()
	defer s.Unlock()
	if s.voffset < ptr.Offset {
		s.voffset = ptr.Offset + uint64(ptr.Len)
	}
}

// Write applies a list of value.Entry to our memtable.
func (s *DB) Write(entries []value.Entry) error {
	ptrs, err := s.vlog.Write(entries)
	y.Check(err)
	y.AssertTrue(len(ptrs) == len(entries))
	s.updateOffset(ptrs)

	if err := s.makeRoomForWrite(); err != nil {
		return err
	}

	var offsetBuf [20]byte
	for i, entry := range entries {
		if len(entry.Value) < s.opt.ValueThreshold { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key, entry.Value, entry.Meta)
		} else {
			s.mt.Put(entry.Key, ptrs[i].Encode(offsetBuf[:]), entry.Meta|value.BitValuePointer)
		}
	}
	return nil
}

// Put puts a key-val pair.
func (s *DB) Put(key []byte, val []byte) error {
	return s.Write([]value.Entry{
		{
			Key:   key,
			Value: val,
		},
	})
}

// Delete deletes a key.
func (s *DB) Delete(key []byte) error {
	return s.Write([]value.Entry{
		{
			Key:  key,
			Meta: value.BitDelete,
		},
	})
}

var (
	Head = []byte("_head_")
)

func (s *DB) makeRoomForWrite() error {
	if s.mt.MemUsage() < s.opt.MaxTableSize {
		// Nothing to do. We have enough space.
		return nil
	}
	s.immWg.Wait() // Make sure we finish flushing immutable memtable.

	s.Lock()
	if s.voffset > 0 {
		fmt.Printf("Storing offset: %v\n", s.voffset)
		offset := make([]byte, 8)
		binary.BigEndian.PutUint64(offset, s.voffset)
		s.mt.Put(Head, offset, 0)
	}

	// Changing imm, mem requires lock.
	s.imm = s.mt
	s.mt = mem.NewTable()

	// Note: It is important to start the compaction within this lock.
	// Otherwise, you might be compacting the wrong imm!
	s.immWg.Add(1)
	go func(imm *mem.Table) {
		defer s.immWg.Done()

		fileID, f := tempFile(s.opt.Dir)
		y.Check(imm.WriteLevel0Table(f))
		tbl, err := newTableHandler(fileID, f)
		defer tbl.decrRef()
		y.Check(err)
		s.lc.addLevel0Table(tbl) // This will incrRef again.
	}(s.imm)
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
	// TODO: Keep moving next if we see deleted entries.
}

func (s *DBIterator) Name() string { return "DBIterator" }

func (s *DBIterator) Close() { s.it.Close() }

// NewIterator returns a MergeIterator over iterators of memtable and compaction levels.
// Note: This acquires references to underlying tables. Remember to close the returned iterator.
func (s *DB) NewIterator() y.Iterator {
	// The order we add these iterators is important.
	// Imagine you add level0 first, then add imm. In between, the initial imm might be moved into
	// level0, and be completely missed. On the other hand, if you add imm first and it got moved
	// to level 0, you would just have that data appear twice which is fine.
	mem, imm := s.getMemTables()
	var iters []y.Iterator
	if mem != nil {
		iters = append(iters, mem.NewIterator())
	}
	if imm != nil {
		iters = append(iters, imm.NewIterator())
	}
	iters = s.lc.appendIterators(iters) // This will increment references.
	return &DBIterator{
		it: y.NewMergeIterator(iters),
		db: s,
	}
}

// Check checks if DB makes sense.
func (s *DB) Check() { s.lc.Check() }
