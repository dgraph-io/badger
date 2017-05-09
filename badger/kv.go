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
	head = []byte("/head/") // For storing value offset for replay.
)

// Options are params for creating DB object.
type Options struct {
	Dir                      string
	NumLevelZeroTables       int   // Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTablesStall  int   // If we hit this number of Level 0 tables, we will stall until level 0 is compacted away.
	LevelOneSize             int64 // Maximum total size for Level 1.
	MaxLevels                int   // Maximum number of levels of compaction. May be made variable later.
	MaxTableSize             int64 // Each table (or file) is at most this size.
	MemtableSlack            int64 // Arena has to be slightly bigger than MaxTableSize.
	LevelSizeMultiplier      int
	ValueThreshold           int // If value size >= this threshold, we store value offsets in tables.
	Verbose                  bool
	DoNotCompact             bool
	MapTablesTo              int
	NumMemtables             int
	ValueGCThreshold         float64
	SyncWrites               bool
	ValueCompressionMinSize  int     // Minimal size in bytes of kv pair to be compressed.
	ValueCompressionMinRatio float64 // Minimal compression ratio of kv pair to be compressed.
}

var DefaultOptions = Options{
	Dir:                      "/tmp",
	NumLevelZeroTables:       5,
	NumLevelZeroTablesStall:  10,
	LevelOneSize:             256 << 20,
	MaxLevels:                7,
	MaxTableSize:             64 << 20,
	LevelSizeMultiplier:      10,
	ValueThreshold:           20,
	Verbose:                  true,
	DoNotCompact:             false, // Only for testing.
	MapTablesTo:              table.MemoryMap,
	NumMemtables:             5,
	MemtableSlack:            10 << 20,
	ValueGCThreshold:         0.5, // Set to zero to not run GC.
	SyncWrites:               true,
	ValueCompressionMinSize:  1024,
	ValueCompressionMinRatio: 2.0,
}

type KV struct {
	sync.RWMutex // Guards imm, mem.

	closer    *y.Closer
	elog      trace.EventLog
	mt        *skl.Skiplist
	imm       []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt       Options
	lc        *levelsController
	vlog      valueLog
	vptr      valuePointer
	arenaPool *skl.ArenaPool
	writeCh   chan *request
	flushChan chan flushTask // For flushing memtables.
}

// NewKV returns a new KV object. Compact levels are created as well.
func NewKV(opt *Options) *KV {
	y.AssertTrue(len(opt.Dir) > 0)
	out := &KV{
		imm:       make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan: make(chan flushTask, opt.NumMemtables),
		writeCh:   make(chan *request, 1000),
		opt:       *opt, // Make a copy.
		arenaPool: skl.NewArenaPool(opt.MaxTableSize+opt.MemtableSlack, opt.NumMemtables+5),
		closer:    y.NewCloser(),
		elog:      trace.NewEventLog("Badger", "KV"),
	}
	out.mt = skl.NewSkiplist(out.arenaPool)

	// newLevelsController potentially loads files in directory.
	out.lc = newLevelsController(out)
	out.lc.startCompact()

	lc := out.closer.Register("memtable")
	go out.flushMemtable(lc) // Need levels controller to be up.

	out.vlog.Open(out, opt)

	lc = out.closer.Register("value-gc")
	go out.vlog.runGCInLoop(lc)

	val, _ := out.Get(head) // casCounter ignored.
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

		if e.CASCounterCheck != 0 {
			oldValue := out.get(e.Key)
			if oldValue.CASCounter != e.CASCounterCheck {
				return true
			}
		}
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		nv := make([]byte, len(e.Value))
		copy(nv, e.Value)

		v := y.ValueStruct{
			Value:      nv,
			Meta:       e.Meta,
			CASCounter: e.casCounter,
		}
		out.mt.Put(nk, v)
		return true
	}
	out.vlog.Replay(vptr, fn)

	lc = out.closer.Register("writes")
	go out.doWrites(lc)

	return out
}

// Close closes a KV.
func (s *KV) Close() {
	if s.opt.Verbose {
		y.Printf("Closing database\n")
	}
	s.elog.Printf("Closing database")
	// Stop value GC first.
	lc := s.closer.Get("value-gc")
	lc.SignalAndWait()

	// Stop writes next.
	lc = s.closer.Get("writes")
	lc.SignalAndWait()

	// Now close the value log.
	s.vlog.Close()

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if s.mt.Size() > 0 {
		if s.opt.Verbose {
			y.Printf("Flushing memtable\n")
		}
		for {
			pushedFlushTask := func() bool {
				s.Lock()
				defer s.Unlock()
				y.AssertTrue(s.mt != nil)
				select {
				case s.flushChan <- flushTask{s.mt, s.vptr}:
					s.imm = append(s.imm, s.mt) // Flusher will attempt to remove this from s.imm.
					s.mt = nil                  // Will segfault if we try writing!
					return true
				default:
					// If we fail to push, we need to unlock and wait for a short while.
					// The flushing operation needs to update s.imm. Otherwise, we have a deadlock.
					// TODO: Think about how to do this more cleanly, maybe without any locks.
				}
				return false
			}()
			if pushedFlushTask {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	s.flushChan <- flushTask{nil, valuePointer{}} // Tell flusher to quit.

	lc = s.closer.Get("memtable")
	lc.Wait()
	if s.opt.Verbose {
		y.Printf("Memtable flushed\n")
	}

	s.lc.close()
	s.elog.Printf("Waiting for closer")
	s.closer.SignalAll()
	s.closer.WaitForAll()
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

func (s *KV) decodeValue(val []byte, meta byte, slice *y.Slice) []byte {
	if (meta & BitDelete) != 0 {
		// Tombstone encountered.
		return nil
	}
	if (meta & BitValuePointer) == 0 {
		return val
	}

	var vp valuePointer
	vp.Decode(val)
	entry, err := s.vlog.Read(vp, slice)
	y.Checkf(err, "Unable to read from value log: %+v", vp)

	if (entry.Meta & BitDelete) == 0 { // Not tombstone.
		return entry.Value
	}
	return []byte{}
}

// getValueHelper returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *KV) get(key []byte) y.ValueStruct {
	tables, decr := s.getMemTables() // Lock should be released.
	defer decr()
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		if vs.Meta != 0 || vs.Value != nil {
			return vs
		}
	}
	return s.lc.get(key)
}

// Get looks for key and returns value. If not found, return nil.
func (s *KV) Get(key []byte) ([]byte, uint16) {
	vs := s.get(key)
	slice := new(y.Slice)
	return s.decodeValue(vs.Value, vs.Meta, slice), vs.CASCounter
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

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (s *KV) writeToLSM(b *request) {
	var offsetBuf [16]byte
	y.AssertTrue(len(b.Ptrs) == len(b.Entries))
	for i, entry := range b.Entries {
		if entry.CASCounterCheck != 0 {
			oldValue := s.get(entry.Key) // No need to decode existing value. Just need old CAS counter.
			if oldValue.CASCounter != entry.CASCounterCheck {
				continue
			}
		}

		if len(entry.Value) < s.opt.ValueThreshold { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:      entry.Value,
					Meta:       entry.Meta,
					CASCounter: entry.casCounter})
		} else {
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:      b.Ptrs[i].Encode(offsetBuf[:]),
					Meta:       entry.Meta | BitValuePointer,
					CASCounter: entry.casCounter})
		}
	}
}

func (s *KV) writeRequests(reqs []*request) {
	if len(reqs) == 0 {
		return
	}
	s.elog.Printf("writeRequests called")

	s.elog.Printf("Writing to value log")

	// CAS counter for all operations has to go onto value log. Otherwise, if it is just in memtable for
	// a long time, and following CAS operations use that as a check, when replaying, we will think that
	// these CAS operations should fail, when they are actually valid.
	for _, req := range reqs {
		for _, e := range req.Entries {
			e.casCounter = newCASCounter()
		}
	}
	s.vlog.Write(reqs)

	s.elog.Printf("Writing to memtable")
	for i, b := range reqs {
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

func (s *KV) doWrites(lc *y.LevelCloser) {
	defer lc.Done()

	blocks := make([]*request, 0, 10)
	for {
		select {
		case b := <-s.writeCh:
			blocks = append(blocks, b)

		case <-lc.HasBeenClosed():
			close(s.writeCh)

			for b := range s.writeCh { // Flush the channel.
				blocks = append(blocks, b)
			}
			s.writeRequests(blocks)
			return

		default:
			if len(blocks) == 0 {
				time.Sleep(time.Millisecond)
				break
			}
			s.writeRequests(blocks)
			blocks = blocks[:0]
		}
	}
}

// BatchSet applies a list of value.Entry to our memtable.
func (s *KV) BatchSet(entries []*Entry) error {
	b := requestPool.Get().(*request)
	defer requestPool.Put(b)

	b.Entries = entries
	b.Wg = sync.WaitGroup{}
	b.Wg.Add(1)
	s.writeCh <- b
	b.Wg.Wait()

	return nil
}

// Set puts a key-val pair.
func (s *KV) Set(key []byte, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return s.BatchSet([]*Entry{e})
}

// CompareAndSet attempts to put given value. Nop if existing key has different casCounter,
func (s *KV) CompareAndSet(key []byte, val []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Value:           val,
		CASCounterCheck: casCounter,
	}
	return s.BatchSet([]*Entry{e})
}

// Delete deletes a key.
func (s *KV) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}
	return s.BatchSet([]*Entry{e})
}

// CompareAndDelete deletes a key. Nop if existing key has different casCounter,
func (s *KV) CompareAndDelete(key []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Meta:            BitDelete,
		CASCounterCheck: casCounter,
	}
	return s.BatchSet([]*Entry{e})
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

func (s *KV) flushMemtable(lc *y.LevelCloser) {
	defer lc.Done()

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
				ft.mt.Put(head, y.ValueStruct{Value: offset}) // casCounter not needed.
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
