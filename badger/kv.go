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
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/skl"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

var (
	Head          = []byte("/head/") // For storing value offset for replay.
	backgroundCtx context.Context
)

func init() {
	backgroundCtx = context.Background()
}

// Options are params for creating DB object.
type Options struct {
	Dir                     string
	NumLevelZeroTables      int   // Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTablesStall int   // If we hit this number of Level 0 tables, we will stall until level 0 is compacted away.
	LevelOneSize            int64 // Maximum total size for Level 1.
	MaxLevels               int   // Maximum number of levels of compaction. May be made variable later.
	MaxTableSize            int64 // Each table (or file) is at most this size.
	MemtableSlack           int64 // Arena has to be slightly bigger than MaxTableSize.
	LevelSizeMultiplier     int
	ValueThreshold          int // If value size >= this threshold, we store value offsets in tables.
	Verbose                 bool
	DoNotCompact            bool
	MapTablesTo             int
	NumMemtables            int
	ValueGCThreshold        float64
	SyncWrites              bool
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
	MemtableSlack:           10 << 20,
	ValueGCThreshold:        0.8, // Set to zero to not run GC.
	SyncWrites:              true,
}

type KV struct {
	sync.RWMutex // Guards imm, mem.

	closer        *y.Closer
	elog          trace.EventLog
	mt            *skl.Skiplist
	imm           []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt           Options
	lc            *levelsController
	vlog          valueLog
	vptr          valuePointer
	arenaPool     *skl.ArenaPool
	writeCh       chan *block
	flushChan     chan flushTask // For flushing memtables.
	blockWriterWg sync.WaitGroup
}

// NewKV returns a new KV object. Compact levels are created as well.
func NewKV(opt *Options) *KV {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &KV{
		imm:       make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan: make(chan flushTask, opt.NumMemtables),
		writeCh:   make(chan *block, 1000),
		opt:       *opt, // Make a copy.
		arenaPool: skl.NewArenaPool(opt.MaxTableSize+opt.MemtableSlack, opt.NumMemtables+5),
		closer:    y.NewCloser(0),
		elog:      trace.NewEventLog("Badger", "KV"),
	}
	out.mt = skl.NewSkiplist(out.arenaPool)

	// newLevelsController potentially loads files in directory.
	out.lc = newLevelsController(out)
	out.lc.startCompact()

	out.closer.Register()
	go out.flushMemtable() // Need levels controller to be up.

	out.vlog.Open(out, opt)

	val, _ := out.Get(backgroundCtx, Head) // casCounter ignored.
	var vptr valuePointer
	if len(val) > 0 {
		vptr.Decode(val)
	}

	first := true
	fn := func(e Entry) bool { // Function for replaying.
		if first {
			fmt.Printf("key=%s\n", e.Key)
		}
		first = false
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		nv := make([]byte, len(e.Value))
		copy(nv, e.Value)

		// Currently, we do not save casCounters in value log. Instead, when
		// replaying, we generate new casCounters.
		v := y.ValueStruct{nv, e.Meta, newCASCounter()}
		out.mt.Put(nk, v)
		return true
	}
	out.vlog.Replay(vptr, fn)

	out.closer.Register()
	out.blockWriterWg.Add(1)
	go out.doWrites()
	return out
}

// Close closes a KV.
func (s *KV) Close() {
	if s.opt.Verbose {
		y.Printf("Closing database\n")
	}

	s.vlog.Close() // Close value log first, because it still needs KV to be active.
	s.elog.Printf("Closing database")
	s.closer.Signal()

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	// TODO: Closer framework doesn't care about order. We need to improve it and clean this up.
	s.blockWriterWg.Wait()

	if s.mt.Size() > 0 {
		if s.opt.Verbose {
			y.Printf("Flushing memtable\n")
		}
		s.Lock()
		y.AssertTrue(s.mt != nil)
		s.flushChan <- flushTask{s.mt, s.vptr}
		s.imm = append(s.imm, s.mt) // Flusher will attempt to remove this from s.imm.
		s.mt = nil                  // Will segfault if we try writing!
		s.Unlock()
	}
	s.flushChan <- flushTask{nil, valuePointer{}} // Tell flusher to quit.
	if s.opt.Verbose {
		y.Printf("Memtable flushed\n")
	}

	s.lc.close()
	s.elog.Printf("Waiting for closer")
	s.closer.Wait()
	s.elog.Finish()
}

// getMemtables returns the current memtables and get references.
func (s *KV) getMemTables() ([]*skl.Skiplist, func()) {
	s.RLock()
	defer s.RUnlock()
	tables := make([]*skl.Skiplist, len(s.imm)+1)
	tables[0] = s.mt
	tables[0].IncrRef()
	last := len(s.imm) - 1
	for i := range s.imm {
		tables[i+1] = s.imm[last-i]
		tables[i+1].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (s *KV) decodeValue(ctx context.Context, val []byte, meta byte) []byte {
	if (meta & BitDelete) != 0 {
		// Tombstone encountered.
		return nil
	}
	if (meta & BitValuePointer) == 0 {
		y.Trace(ctx, "Value stored with key")
		return val
	}

	var vp valuePointer
	vp.Decode(val)
	entry, err := s.vlog.Read(ctx, vp)
	y.Checkf(err, "Unable to read from value log: %+v", vp)

	if (entry.Meta & BitDelete) == 0 { // Not tombstone.
		return entry.Value
	}
	return []byte{}
}

// getValueHelper returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *KV) get(ctx context.Context, key []byte) y.ValueStruct {
	y.Trace(ctx, "Retrieving key from memtables")
	tables, decr := s.getMemTables() // Lock should be released.
	defer decr()
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		if vs.Meta != 0 || vs.Value != nil {
			return vs
		}
	}
	y.Trace(ctx, "Not found in memtables. Getting from levels")
	return s.lc.get(ctx, key)
}

// Get looks for key and returns value. If not found, return nil.
func (s *KV) Get(ctx context.Context, key []byte) ([]byte, uint16) {
	vs := s.get(ctx, key)
	return s.decodeValue(ctx, vs.Value, vs.Meta), vs.CASCounter
}

func (s *KV) updateOffset(ptrs []valuePointer) {
	ptr := ptrs[len(ptrs)-1]

	s.Lock()
	defer s.Unlock()
	if s.vptr.Fid < ptr.Fid {
		s.vptr = ptr
	} else if s.vptr.Offset < ptr.Offset {
		s.vptr = ptr
	}
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return new(block)
	},
}

func (s *KV) writeToLSM(b *block) {
	var offsetBuf [16]byte
	y.AssertTrue(len(b.Ptrs) == len(b.Entries))
	for i, entry := range b.Entries {
		if entry.skip {
			continue
		}
		if len(entry.Value) < s.opt.ValueThreshold { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key,
				y.ValueStruct{entry.Value, entry.Meta, entry.casCounter})
		} else {
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:      b.Ptrs[i].Encode(offsetBuf[:]),
					Meta:       entry.Meta | BitValuePointer,
					CASCounter: entry.casCounter})
		}
	}
}

func (s *KV) writeBlocks(blocks []*block) {
	if len(blocks) == 0 {
		return
	}
	s.elog.Printf("writeBlocks called")

	s.elog.Printf("Writing to value log")
	// Before writing to value log, generate the random CAS counters. Also check
	// CAS counters if asked to.
	// TODO: If the same key appears in the same list of blocks, the get here
	// will be insufficient for comparing CAS counters.
	for _, block := range blocks {
		for _, e := range block.Entries {
			e.skip = false // To be sure.
			if e.CASCounterCheck != 0 {
				oldValue := s.get(backgroundCtx, e.Key) // No need to decode existing value. Just need old CAS counter.
				if oldValue.CASCounter != e.CASCounterCheck {
					e.skip = true
					continue
				}
			}
			e.casCounter = newCASCounter()
		}
	}
	s.vlog.Write(blocks)

	s.elog.Printf("Writing to memtable")
	for i, b := range blocks {
		for !s.hasRoomForWrite() {
			s.elog.Printf("Making room for writes")
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		s.writeToLSM(b)
		s.elog.Printf("Wrote %d entries from block %d", len(b.Entries), i)
		s.updateOffset(b.Ptrs)
		b.Wg.Done()
	}
}

func (s *KV) doWrites() {
	defer s.closer.Done()
	defer s.blockWriterWg.Done()

	blocks := make([]*block, 0, 10)
	for {
		select {
		case b := <-s.writeCh:
			blocks = append(blocks, b)

		case <-s.closer.HasBeenClosed():
			close(s.writeCh)

			for b := range s.writeCh { // Flush the channel.
				blocks = append(blocks, b)
			}
			s.writeBlocks(blocks)
			return

		default:
			if len(blocks) == 0 {
				time.Sleep(time.Millisecond)
				break
			}
			s.writeBlocks(blocks)
			blocks = blocks[:0]
		}
	}
}

// Write applies a list of value.Entry to our memtable.
func (s *KV) Write(ctx context.Context, entries []*Entry) error {
	b := blockPool.Get().(*block)
	defer blockPool.Put(b)

	b.Entries = entries
	b.Wg = sync.WaitGroup{}
	b.Wg.Add(1)
	s.writeCh <- b
	b.Wg.Wait()

	return nil
}

// Put puts a key-val pair.
func (s *KV) Put(ctx context.Context, key []byte, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return s.Write(ctx, []*Entry{e})
}

// CASPut attempts to put given value. Nop if existing key has different casCounter,
func (s *KV) CASPut(ctx context.Context, key []byte, val []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Value:           val,
		CASCounterCheck: casCounter,
	}
	return s.Write(ctx, []*Entry{e})
}

// Delete deletes a key.
func (s *KV) Delete(ctx context.Context, key []byte) error {
	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}
	return s.Write(ctx, []*Entry{e})
}

// CASDelete deletes a key. Nop if existing key has different casCounter,
func (s *KV) CASDelete(ctx context.Context, key []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Meta:            BitDelete,
		CASCounterCheck: casCounter,
	}
	return s.Write(ctx, []*Entry{e})
}

func (s *KV) hasRoomForWrite() bool {
	s.Lock()
	defer s.Unlock()
	if s.mt.Size() < s.opt.MaxTableSize {
		return true
	}

	y.AssertTrue(s.mt != nil) // A nil mt indicates that KV is being closed.
	select {
	case s.flushChan <- flushTask{s.mt, s.vptr}:
		if s.opt.Verbose {
			y.Printf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
				s.mt.Size(), len(s.flushChan))
		}
		// We manage to push this task. Let's modify imm.
		s.imm = append(s.imm, s.mt)
		s.mt = skl.NewSkiplist(s.arenaPool)
		// New memtable is empty. We certainly have room.
		return true
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return false
	}
}

// WriteLevel0Table flushes memtable. It drops deleteValues.
func writeLevel0Table(s *skl.Skiplist, f *os.File) error {
	iter := s.NewIterator()
	defer iter.Close()
	b := table.NewTableBuilder()
	defer b.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	var buf [2]byte // Level 0. Leave it initialized as 0.
	_, err := f.Write(b.Finish(buf[:]))
	return err
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

func (s *KV) flushMemtable() {
	defer s.closer.Done()

	for {
		select {
		case ft := <-s.flushChan:
			if ft.mt == nil {
				return
			}

			if ft.vptr.Fid > 0 || ft.vptr.Offset > 0 {
				if s.opt.Verbose {
					fmt.Printf("Storing offset: %+v\n", ft.vptr)
				}
				offset := make([]byte, 16)
				s.Lock() // For vptr.
				s.vptr.Encode(offset)
				s.Unlock()
				ft.mt.Put(Head, y.ValueStruct{Value: offset}) // casCounter not needed.
			}
			fileID, _ := s.lc.reserveFileIDs(1)
			fd, err := y.OpenSyncedFile(table.NewFilename(fileID, s.opt.Dir), true)
			y.Check(err)
			y.Check(writeLevel0Table(ft.mt, fd))

			tbl, err := table.OpenTable(fd, s.opt.MapTablesTo)
			defer tbl.DecrRef()

			y.Check(err)
			s.lc.addLevel0Table(tbl) // This will incrRef again.

			// Update s.imm. Need a lock.
			s.Lock()
			y.AssertTrue(ft.mt == s.imm[0]) //For now, single threaded.
			s.imm = s.imm[1:]
			ft.mt.DecrRef() // Return memory.
			s.Unlock()
		}
	}
}
