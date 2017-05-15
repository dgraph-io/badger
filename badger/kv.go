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
	Dir string // Directory to store the data in.

	// The following affect all levels of LSM tree.
	MaxTableSize        int64 // Each table (or file) is at most this size.
	LevelSizeMultiplier int   // Equals SizeOf(Li+1)/SizeOf(Li).
	MaxLevels           int   // Maximum number of levels of compaction.
	ValueThreshold      int   // If value size >= this threshold, only store value offsets in tree.
	MapTablesTo         int   // How should LSM tree be accessed.

	// The following affect only memtables in LSM tree.
	MemtableSlack int64 // Arena has to be slightly bigger than MaxTableSize.
	NumMemtables  int   // Maximum number of tables to keep in memory, before stalling.

	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int
	// If we hit this number of Level 0 tables, we will stall until L0 is compacted away.
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	LevelOneSize int64

	// Run value log garbage collection if we can reclaim at least this much space. This is a ratio.
	ValueGCThreshold float64

	// Size of single value log file.
	ValueLogFileSize int

	// The following affect value compression in value log.
	ValueCompressionMinSize  int     // Minimal size in bytes of KV pair to be compressed.
	ValueCompressionMinRatio float64 // Minimal compression ratio of KV pair to be compressed.

	// Sync all writes to disk. Setting this to true would slow down data loading significantly.
	SyncWrites bool

	// Flags for testing purposes.
	DoNotCompact bool // Stops LSM tree from compactions.
	Verbose      bool // Turns on verbose mode.
}

// DefaultOptions sets a list of safe recommended options. Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	Dir:                      "/tmp",
	DoNotCompact:             false,
	LevelOneSize:             256 << 20,
	LevelSizeMultiplier:      10,
	MapTablesTo:              table.MemoryMap,
	MaxLevels:                7,
	MaxTableSize:             64 << 20,
	MemtableSlack:            10 << 20,
	NumLevelZeroTables:       5,
	NumLevelZeroTablesStall:  10,
	NumMemtables:             5,
	SyncWrites:               false,
	ValueCompressionMinRatio: 2.0,
	ValueCompressionMinSize:  1024,
	ValueGCThreshold:         0.5, // Set to zero to not run GC.
	ValueLogFileSize:         1 << 30,
	ValueThreshold:           20,
	Verbose:                  false,
}

// KV provides the various functions required to interact with Badger.
// KV is thread-safe.
type KV struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

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

// NewKV returns a new KV object.
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
	y.VerboseMode = opt.Verbose

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

	lc = out.closer.Register("writes")
	go out.doWrites(lc)

	first := true
	fn := func(e Entry) bool { // Function for replaying.
		if first {
			y.Printf("First key=%s\n", e.Key)
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
		for !out.hasRoomForWrite() {
			y.Printf("Replay: Making room for writes")
			time.Sleep(10 * time.Millisecond)
		}
		out.mt.Put(nk, v)
		return true
	}
	out.vlog.Replay(vptr, fn)

	return out
}

// Close closes a KV. It's crucial to call it to ensure all the pending updates
// make their way to disk.
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
	return nil
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

// Get looks for key and returns value along with the current CAS counter.
// If key is not found, value returned is nil.
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
	var offsetBuf [10]byte
	y.AssertTrue(len(b.Ptrs) == len(b.Entries))

	for i, entry := range b.Entries {
		entry.Error = nil
		if entry.CASCounterCheck != 0 {
			oldValue := s.get(entry.Key) // No need to decode existing value. Just need old CAS counter.
			if oldValue.CASCounter != entry.CASCounterCheck {
				entry.Error = CasMismatch
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
		if len(b.Entries) == 0 {
			b.Wg.Done()
			continue
		}
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

// BatchSet applies a list of badger.Entry. Errors are set on each Entry invidividually.
//   for _, e := range entries {
//      Check(e.Error)
//   }
func (s *KV) BatchSet(entries []*Entry) {
	b := requestPool.Get().(*request)
	defer requestPool.Put(b)

	b.Entries = entries
	b.Wg = sync.WaitGroup{}
	b.Wg.Add(1)
	s.writeCh <- b
	b.Wg.Wait()
}

// Set sets the provided value for a given key. If key is not present, it is created.
// If it is present, the existing value is overwritten with the one provided.
func (s *KV) Set(key, val []byte) {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	s.BatchSet([]*Entry{e})
}

// EntriesSet adds a Set to the list of entries.
// Exposing this so that user does not have to specify the Entry directly.
func EntriesSet(s []*Entry, key, val []byte) []*Entry {
	return append(s, &Entry{
		Key:   key,
		Value: val,
	})
}

// CompareAndSet sets the given value, ensuring that the no other Set operation has happened,
// since last read. If the key has a different casCounter, this would not update the key
// and return an error.
func (s *KV) CompareAndSet(key []byte, val []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Value:           val,
		CASCounterCheck: casCounter,
	}
	s.BatchSet([]*Entry{e})
	return e.Error
}

// Delete deletes a key.
// Exposing this so that user does not have to specify the Entry directly.
// For example, BitDelete seems internal to badger.
func (s *KV) Delete(key []byte) {
	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}

	s.BatchSet([]*Entry{e})
}

// EntriesDelete adds a Del to the list of entries.
func EntriesDelete(s []*Entry, key []byte) []*Entry {
	return append(s, &Entry{
		Key:  key,
		Meta: BitDelete,
	})
}

// CompareAndDelete deletes a key ensuring that the it has not been changed since last read.
// If existing key has different casCounter, this would not delete the key and return an error.
func (s *KV) CompareAndDelete(key []byte, casCounter uint16) error {
	e := &Entry{
		Key:             key,
		Meta:            BitDelete,
		CASCounterCheck: casCounter,
	}
	s.BatchSet([]*Entry{e})
	return e.Error
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

	for ft := range s.flushChan {
		if ft.mt == nil {
			return
		}

		if ft.vptr.Fid > 0 || ft.vptr.Offset > 0 {
			if s.opt.Verbose {
				fmt.Printf("Storing offset: %+v\n", ft.vptr)
			}
			offset := make([]byte, 10)
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
