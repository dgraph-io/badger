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
	"time"

	"github.com/dgraph-io/badger/mem"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

var (
	Head = []byte("/head/") // For storing value offset for replay.
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
	ValueThreshold          int // If value size >= this threshold, we store value offsets in tables.
	Verbose                 bool
	DoNotCompact            bool
	MapTablesTo             int
	NumMemtables            int
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
	MapTablesTo:             table.MemoryMap,
	NumMemtables:            5,
}

type KV struct {
	sync.RWMutex // Guards imm, mem.

	mt        *mem.Table
	imm       []*mem.Table   // Add here only AFTER pushing to flushChan.
	flushChan chan flushTask // For flushing memtables.
	flushDone chan struct{}
	opt       Options
	lc        *levelsController
	vlog      value.Log
	voffset   uint64
}

type flushTask struct {
	mt      *mem.Table
	voffset uint64
}

// Summary is produced when DB is closed. Currently it is used only for testing.
type Summary struct {
	fileIDs map[uint64]bool
}

func (s *KV) flushMemtable() {
	var done bool
	for !done {
		select {
		case ft := <-s.flushChan:
			if ft.mt == nil {
				if s.opt.Verbose {
					y.Printf("Closing flushChan\n")
				}
				close(s.flushChan) // This is our close signal.
				done = true
				break
			}
			if ft.voffset > 0 {
				if s.opt.Verbose {
					fmt.Printf("Storing offset: %v\n", ft.voffset)
				}
				offset := make([]byte, 8)
				binary.BigEndian.PutUint64(offset, ft.voffset)
				ft.mt.Put(Head, offset, 0)
			}
			fileID, _ := s.lc.reserveFileIDs(1)
			fd, err := y.OpenSyncedFile(table.NewFilename(fileID, s.opt.Dir))
			y.Check(err)
			y.Check(ft.mt.WriteLevel0Table(fd))
			tbl, err := table.OpenTable(fd, s.opt.MapTablesTo)
			defer tbl.DecrRef()
			y.Check(err)
			s.lc.addLevel0Table(tbl) // This will incrRef again.

			// Update s.imm. Need a lock.
			s.Lock()
			y.AssertTrue(ft.mt == s.imm[0]) //For now, single threaded.
			s.imm = s.imm[1:]
			s.Unlock()
		}
	}
	s.flushDone <- struct{}{}
}

// NewKV returns a new KV object. Compact levels are created as well.
func NewKV(opt *Options) *KV {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &KV{
		mt:        mem.NewTable(),
		imm:       make([]*mem.Table, 0, opt.NumMemtables),
		flushChan: make(chan flushTask, opt.NumMemtables),
		flushDone: make(chan struct{}),
		opt:       *opt, // Make a copy.
	}

	// newLevelsController potentially loads files in directory.
	out.lc = newLevelsController(out)
	out.lc.startCompact()
	go out.flushMemtable() // Need levels controller to be up.

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
func (s *KV) Close() {
	if s.opt.Verbose {
		y.Printf("Closing database\n")
	}

	// TODO(ganesh): Block writes to mem.
	if s.mt.Size() > 0 {
		s.Lock()
		if s.opt.Verbose {
			y.Printf("Flushing memtable\n")
		}
		y.AssertTrue(s.mt != nil)
		s.flushChan <- flushTask{s.mt, s.voffset}
		s.imm = append(s.imm, s.mt) // Flusher will attempt to remove this from s.imm.
		s.mt = nil                  // Will segfault if we try writing!
		s.Unlock()
	}
	s.flushChan <- flushTask{nil, 0} // Tell flusher to quit.
	<-s.flushDone                    // Wait for flusher to be done.
	if s.opt.Verbose {
		y.Printf("Memtable flushed\n")
	}
	s.lc.close()
}

func (s *KV) getMemTables() (*mem.Table, []*mem.Table) {
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
	for i := len(imm) - 1; i >= 0; i-- {
		v, meta := imm[i].Get(key)
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
	for !s.hasRoomForWrite() {
		// We need to poll a bit because if both hasRoomForWrite and the flusher need
		// access to s.imm. When flushChan is full and you are blocked there, and the flusher
		// is trying to update s.imm, you will get a deadlock.
		time.Sleep(10 * time.Millisecond)
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

func (s *KV) hasRoomForWrite() bool {
	s.Lock()
	defer s.Unlock()
	if s.mt.Size() < s.opt.MaxTableSize {
		return true
	}

	y.AssertTrue(s.mt != nil) // A nil mt indicates that KV is being closed.
	select {
	case s.flushChan <- flushTask{s.mt, s.voffset}:
		if s.opt.Verbose {
			y.Printf("Flushing memtable, size of flushChan: %d\n", len(s.flushChan))
		}
		// We manage to push this task. Let's modify imm.
		s.imm = append(s.imm, s.mt)
		s.mt = mem.NewTable()
		// New memtable is empty. We certainly have room.
		return true
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return false
	}
}

func (s *KV) Validate() { s.lc.validate() }

func (s *KV) DebugPrintMore() { s.lc.debugPrintMore() }
