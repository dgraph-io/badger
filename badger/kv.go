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
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/mem"
	"github.com/dgraph-io/badger/table"
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
	MaxTableSize:            64 << 20,
	LevelSizeMultiplier:     10,
	ValueThreshold:          20,
	Verbose:                 true,
	DoNotCompact:            false, // Only for testing.
}

type KV struct {
	sync.RWMutex // Guards imm, mem.

	imm       *mem.Table // Immutable, memtable being flushed.
	mt        *mem.Table
	immWg     sync.WaitGroup // Nonempty when flushing immutable memtable.
	opt       Options
	lc        *levelsController
	vlog      value.Log
	voffset   uint64
	maxFileID uint64
}

// dbSummary is produced when DB is closed. Currently it is used only for testing.
type dbSummary struct {
	filenames map[string]bool
}

// NewKV returns a new KV object. Compact levels are created as well.
func NewKV(opt *Options) *KV {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &KV{
		mt:  mem.NewTable(),
		opt: *opt, // Make a copy.
	}
	// newLevelsController potentially loads files in directory.
	out.lc, out.maxFileID = newLevelsController(out)
	out.lc.startCompact()
	vlogPath := filepath.Join(opt.Dir, "vlog")
	out.vlog.Open(vlogPath)

	val := out.Get(context.Background(), Head)
	var voffset uint64
	if len(val) == 0 {
		voffset = 0
	} else {
		voffset = binary.BigEndian.Uint64(val)
	}

	first := true
	fn := func(k, v []byte, meta byte) {
		if first {
			fmt.Printf("key=%s\n", k)
		}
		first = false
		out.mt.Put(k, v, meta)
	}
	out.vlog.Replay(voffset, fn)

	return out
}

// Close closes a KV.
func (s *KV) Close() *dbSummary {
	// TODO(ganesh): Block writes to mem.
	s.makeRoomForWrite(true) // Force mem to be emptied. Initiate new imm flush.
	s.immWg.Wait()           // Wait for imm to go to disk.
	summary := &dbSummary{
		filenames: make(map[string]bool),
	}
	s.lc.close(summary)
	return summary
}

func (s *KV) getMemTables() (*mem.Table, *mem.Table) {
	s.RLock()
	defer s.RUnlock()
	return s.mt, s.imm
}

func (s *KV) decodeValue(ctx context.Context, val []byte, meta byte) []byte {
	if (meta & value.BitDelete) != 0 {
		// Tombstone encountered.
		return nil
	}
	if (meta & value.BitValuePointer) == 0 {
		y.Trace(ctx, "Value stored with key")
		return val
	}

	var vp value.Pointer
	vp.Decode(val)
	entry, err := s.vlog.Read(ctx, vp)
	y.Checkf(err, "Unable to read from value log: %+v", vp)

	if (entry.Meta & value.BitDelete) == 0 { // Not tombstone.
		return entry.Value
	}
	return []byte{}
}

// getValueHelper returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *KV) get(ctx context.Context, key []byte) ([]byte, byte) {
	y.Trace(ctx, "Retrieving key from memtables")
	mem, imm := s.getMemTables() // Lock should be released.
	if mem != nil {
		v, meta := mem.Get(key)
		if meta != 0 || v != nil {
			return v, meta
		}
	}
	if imm != nil {
		v, meta := imm.Get(key)
		if meta != 0 || v != nil {
			return v, meta
		}
	}
	y.Trace(ctx, "Not found in memtables. Getting from levels")
	return s.lc.get(ctx, key)
}

// Get looks for key and returns value. If not found, return nil.
func (s *KV) Get(ctx context.Context, key []byte) []byte {
	val, meta := s.get(ctx, key)
	return s.decodeValue(ctx, val, meta)
}

func (s *KV) updateOffset(ptrs []value.Pointer) {
	ptr := ptrs[len(ptrs)-1]

	s.Lock()
	defer s.Unlock()
	if s.voffset < ptr.Offset {
		s.voffset = ptr.Offset + uint64(ptr.Len)
	}
}

// Write applies a list of value.Entry to our memtable.
func (s *KV) Write(ctx context.Context, entries []value.Entry) error {
	y.Trace(ctx, "Making room for writes")
	if err := s.makeRoomForWrite(false); err != nil {
		return err
	}

	y.Trace(ctx, "Writing to value log.")
	ptrs, err := s.vlog.Write(entries)
	y.Check(err)
	y.AssertTrue(len(ptrs) == len(entries))
	s.updateOffset(ptrs)

	y.Trace(ctx, "Writing to memtable")
	var offsetBuf [20]byte
	for i, entry := range entries {
		if len(entry.Value) < s.opt.ValueThreshold { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key, entry.Value, entry.Meta)
		} else {
			s.mt.Put(entry.Key, ptrs[i].Encode(offsetBuf[:]), entry.Meta|value.BitValuePointer)
		}
	}
	y.Trace(ctx, "Wrote %d entries.", len(entries))
	return nil
}

// Put puts a key-val pair.
func (s *KV) Put(ctx context.Context, key []byte, val []byte) error {
	return s.Write(ctx, []value.Entry{
		{
			Key:   key,
			Value: val,
		},
	})
}

// Delete deletes a key.
func (s *KV) Delete(ctx context.Context, key []byte) error {
	return s.Write(ctx, []value.Entry{
		{
			Key:  key,
			Meta: value.BitDelete,
		},
	})
}

var (
	Head = []byte("/head/")
)

// makeRoomForWrite may create a new memtable and make imm <- mem.
// But before that, it will make sure that imm is flushed.
// If force==true, we will always proceed to make the above change.
// Note: After function returns, we may still be flushing the new imm to disk.
func (s *KV) makeRoomForWrite(force bool) error {
	if !force && s.mt.Size() < s.opt.MaxTableSize {
		// Nothing to do. We have enough space.
		return nil
	}
	if s.mt.Size() == 0 {
		// Even if forced, we do not attempt to write an empty table!
		return nil
	}
	s.immWg.Wait() // Make sure we finish flushing immutable memtable.

	s.Lock()
	defer s.Unlock()
	if s.voffset > 0 {
		fmt.Printf("Storing offset: %v\n", s.voffset)
		offset := make([]byte, 8)
		binary.BigEndian.PutUint64(offset, s.voffset)
		s.mt.Put(Head, offset, 0)
	}

	// Changing imm, mem requires lock.
	s.imm = s.mt
	s.mt = mem.NewTable()
	s.immWg.Add(1)
	go func(imm *mem.Table) {
		defer s.immWg.Done()
		f := s.newFile()
		y.Check(imm.WriteLevel0Table(f))
		tbl, err := table.OpenTable(f)
		defer tbl.DecrRef()
		y.Check(err)
		s.lc.addLevel0Table(tbl) // This will incrRef again.
	}(s.imm)
	return nil
}

// Check checks if DB makes sense.
func (s *KV) Check() { s.lc.Check() }
