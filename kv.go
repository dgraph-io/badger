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
	"container/heap"
	"expvar"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/skl"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

var (
	badgerPrefix = []byte("!badger!")     // Prefix for internal keys used by badger.
	head         = []byte("!badger!head") // For storing value offset for replay.
	txnKey       = []byte("!badger!txn")  // For indicating end of entries in txn.
)

type closers struct {
	updateSize *y.Closer
	compactors *y.Closer
	memtable   *y.Closer
	writes     *y.Closer
	valueGC    *y.Closer
}

// KV provides the various functions required to interact with Badger.
// KV is thread-safe.
type KV struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *DirectoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *DirectoryLockGuard

	closers   closers
	elog      trace.EventLog
	mt        *skl.Skiplist   // Our latest (actively written) in-memory table
	imm       []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	vptr      valuePointer // less than or equal to a pointer to the last vlog value put into mt
	writeCh   chan *request
	flushChan chan flushTask // For flushing memtables.

	// Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
	// we use an atomic op.
	lastUsedCommitTs uint64

	txnState *globalTxnState
}

const (
	kvWriteChCapacity = 1000
)

// NewKV returns a new KV object.
func NewKV(optParam *Options) (out *KV, err error) {
	// Make a copy early and fill in maxBatchSize
	opt := *optParam
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return nil, y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			return nil, ErrInvalidDir
		}
	}
	absDir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	absValueDir, err := filepath.Abs(opt.ValueDir)
	if err != nil {
		return nil, err
	}

	dirLockGuard, err := AcquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.Release()
		}
	}()
	var valueDirLockGuard *DirectoryLockGuard
	if absValueDir != absDir {
		valueDirLockGuard, err = AcquireDirectoryLock(opt.ValueDir, lockFile)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if valueDirLockGuard != nil {
			_ = valueDirLockGuard.Release()
		}
	}()
	if !(opt.ValueLogFileSize <= 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}
	manifestFile, manifest, err := openOrCreateManifestFile(opt.Dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	gs := &globalTxnState{
		nextCommit:     1,
		pendingCommits: make(map[uint64]struct{}),
		commits:        make(map[uint64]uint64),
	}
	heap.Init(&gs.commitMark)

	out = &KV{
		imm:           make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan:     make(chan flushTask, opt.NumMemtables),
		writeCh:       make(chan *request, kvWriteChCapacity),
		opt:           opt,
		manifest:      manifestFile,
		elog:          trace.NewEventLog("Badger", "KV"),
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		txnState:      gs,
	}

	out.closers.updateSize = y.NewCloser(1)
	go out.updateSize(out.closers.updateSize)
	out.mt = skl.NewSkiplist(arenaSize(&opt))

	// newLevelsController potentially loads files in directory.
	if out.lc, err = newLevelsController(out, &manifest); err != nil {
		return nil, err
	}

	out.closers.compactors = y.NewCloser(1)
	out.lc.startCompact(out.closers.compactors)

	out.closers.memtable = y.NewCloser(1)
	go out.flushMemtable(out.closers.memtable) // Need levels controller to be up.

	if err = out.vlog.Open(out, &opt); err != nil {
		return nil, err
	}

	vs, err := out.get(head)
	if err != nil {
		return nil, errors.Wrap(err, "Retrieving head")
	}
	out.txnState.curRead = vs.Version
	var vptr valuePointer
	if len(vs.Value) > 0 {
		vptr.Decode(vs.Value)
	}

	// lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
	// written value log entry that we replay.  (Subsequent value log entries might be _less_
	// than lastUsedCasCounter, if there was value log gc so we have to max() values while
	// replaying.)
	// out.lastUsedCasCounter = item.casCounter
	// TODO: Figure this out. This would update the read timestamp, and set nextCommitTs.

	replayCloser := y.NewCloser(1)
	go out.doWrites(replayCloser)

	type txnEntry struct {
		nk []byte
		v  y.ValueStruct
	}

	var txn []txnEntry
	var lastCommit uint64

	toLSM := func(nk []byte, vs y.ValueStruct) {
		for err := out.ensureRoomForWrite(); err != nil; err = out.ensureRoomForWrite() {
			out.elog.Printf("Replay: Making room for writes")
			time.Sleep(10 * time.Millisecond)
		}
		out.mt.Put(nk, vs)
	}

	first := true
	fn := func(e Entry, vp valuePointer) error { // Function for replaying.
		if first {
			out.elog.Printf("First key=%s\n", e.Key)
		}
		first = false

		if out.txnState.curRead < y.ParseTs(e.Key) {
			out.txnState.curRead = y.ParseTs(e.Key)
		}

		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if out.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = make([]byte, vptrSize)
			vp.Encode(nv)
			meta = meta | BitValuePointer
		}

		v := y.ValueStruct{
			Value:    nv,
			Meta:     meta,
			UserMeta: e.UserMeta,
		}

		if e.Meta&BitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				return errors.Wrapf(err, "Unable to parse txn fin: %q", e.Value)
			}
			y.AssertTrue(lastCommit == txnTs)
			y.AssertTrue(len(txn) > 0)
			// Got the end of txn. Now we can store them.
			for _, t := range txn {
				toLSM(t.nk, t.v)
			}
			txn = txn[:0]

		} else if e.Meta&BitTxn == 0 {
			// This entry is from a rewrite.
			toLSM(nk, v)

			// We shouldn't get this entry in the middle of a transaction.
			y.AssertTrue(lastCommit == 0)
			y.AssertTrue(len(txn) == 0)

		} else {
			txnTs := y.ParseTs(nk)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			y.AssertTrue(lastCommit == txnTs)
			te := txnEntry{nk: nk, v: v}
			txn = append(txn, te)
		}
		return nil
	}
	if err = out.vlog.Replay(vptr, fn); err != nil {
		return out, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.
	// Now that we have the curRead, we can update the nextCommit.
	out.txnState.nextCommit = out.txnState.curRead + 1

	// Mmap writable log
	lf := out.vlog.filesMap[out.vlog.maxFid]
	if err = lf.mmap(2 * out.vlog.opt.ValueLogFileSize); err != nil {
		return out, errors.Wrapf(err, "Unable to mmap RDWR log file")
	}

	out.writeCh = make(chan *request, kvWriteChCapacity)
	out.closers.writes = y.NewCloser(1)
	go out.doWrites(out.closers.writes)

	out.closers.valueGC = y.NewCloser(1)
	go out.vlog.waitOnGC(out.closers.valueGC)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return out, nil
}

// Close closes a KV. It's crucial to call it to ensure all the pending updates
// make their way to disk.
func (s *KV) Close() (err error) {
	s.elog.Printf("Closing database")
	// Stop value GC first.
	s.closers.valueGC.SignalAndWait()

	// Stop writes next.
	s.closers.writes.SignalAndWait()

	// Now close the value log.
	if vlogErr := s.vlog.Close(); err == nil {
		err = errors.Wrap(vlogErr, "KV.Close")
	}

	// Make sure that block writer is done pushing stuff into memtable!
	// Otherwise, you will have a race condition: we are trying to flush memtables
	// and remove them completely, while the block / memtable writer is still
	// trying to push stuff into the memtable. This will also resolve the value
	// offset problem: as we push into memtable, we update value offsets there.
	if !s.mt.Empty() {
		s.elog.Printf("Flushing memtable")
		for {
			pushedFlushTask := func() bool {
				s.Lock()
				defer s.Unlock()
				y.AssertTrue(s.mt != nil)
				select {
				case s.flushChan <- flushTask{s.mt, s.vptr}:
					s.imm = append(s.imm, s.mt) // Flusher will attempt to remove this from s.imm.
					s.mt = nil                  // Will segfault if we try writing!
					s.elog.Printf("pushed to flush chan\n")
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

	s.closers.memtable.Wait()
	s.elog.Printf("Memtable flushed")

	s.closers.compactors.SignalAndWait()
	s.elog.Printf("Compaction finished")

	if lcErr := s.lc.close(); err == nil {
		err = errors.Wrap(lcErr, "KV.Close")
	}
	s.elog.Printf("Waiting for closer")
	s.closers.updateSize.SignalAndWait()

	s.elog.Finish()

	if guardErr := s.dirLockGuard.Release(); err == nil {
		err = errors.Wrap(guardErr, "KV.Close")
	}
	if s.valueDirGuard != nil {
		if guardErr := s.valueDirGuard.Release(); err == nil {
			err = errors.Wrap(guardErr, "KV.Close")
		}
	}
	if manifestErr := s.manifest.close(); err == nil {
		err = errors.Wrap(manifestErr, "KV.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(s.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "KV.Close")
	}
	if syncErr := syncDir(s.opt.ValueDir); err == nil {
		err = errors.Wrap(syncErr, "KV.Close")
	}

	return err
}

const (
	lockFile = "LOCK"
)

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := OpenDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// getMemtables returns the current memtables and get references.
func (s *KV) getMemTables() ([]*skl.Skiplist, func()) {
	s.RLock()
	defer s.RUnlock()

	tables := make([]*skl.Skiplist, len(s.imm)+1)

	// Get mutable memtable.
	tables[0] = s.mt
	tables[0].IncrRef()

	// Get immutable memtables.
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

func (s *KV) yieldItemValue(item *KVItem, consumer func([]byte) error) error {
	if !item.hasValue() {
		return consumer(nil)
	}

	if item.slice == nil {
		item.slice = new(y.Slice)
	}

	if (item.meta & BitValuePointer) == 0 {
		val := item.slice.Resize(len(item.vptr))
		copy(val, item.vptr)
		return consumer(val)
	}

	var vp valuePointer
	vp.Decode(item.vptr)
	err := s.vlog.Read(vp, consumer)
	if err != nil {
		return err
	}
	return nil
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
func (s *KV) get(key []byte) (y.ValueStruct, error) {
	tables, decr := s.getMemTables() // Lock should be released.
	defer decr()

	y.NumGets.Add(1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGets.Add(1)
		if vs.Meta != 0 || vs.Value != nil {
			return vs, nil
		}
	}
	return s.lc.get(key)
}

func (s *KV) updateOffset(ptrs []valuePointer) {
	var ptr valuePointer
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}

	s.Lock()
	defer s.Unlock()
	y.AssertTrue(!ptr.Less(s.vptr))
	s.vptr = ptr
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (s *KV) shouldWriteValueToLSM(e Entry) bool {
	return len(e.Value) < s.opt.ValueThreshold
}

func (s *KV) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if s.shouldWriteValueToLSM(*entry) { // Will include deletion / tombstone case.
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:    entry.Value,
					Meta:     entry.Meta,
					UserMeta: entry.UserMeta,
				})
		} else {
			var offsetBuf [vptrSize]byte
			s.mt.Put(entry.Key,
				y.ValueStruct{
					Value:    b.Ptrs[i].Encode(offsetBuf[:]),
					Meta:     entry.Meta | BitValuePointer,
					UserMeta: entry.UserMeta,
				})
		}
	}
	return nil
}

// writeRequests is called serially by only one goroutine.
func (s *KV) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}

	s.elog.Printf("writeRequests called. Writing to value log")

	err := s.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}

	s.elog.Printf("Writing to memtable")
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		for err := s.ensureRoomForWrite(); err != nil; err = s.ensureRoomForWrite() {
			s.elog.Printf("Making room for writes")
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := s.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		s.updateOffset(b.Ptrs)
	}
	done(nil)
	s.elog.Printf("%d entries written", count)
	return nil
}

func (s *KV) doWrites(lc *y.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := s.writeRequests(reqs); err != nil {
			log.Printf("ERROR in Badger::writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)
	y.PendingWrites.Set(s.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-s.writeCh:
		case <-lc.HasBeenClosed():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-s.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		close(s.writeCh)
		for r := range s.writeCh { // Flush the channel.
			reqs = append(reqs, r)
		}

		pendingCh <- struct{}{} // Push to pending before doing a write.
		writeRequests(reqs)
		return

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

func (s *KV) sendToWriteCh(entries []*Entry) (*request, error) {
	var count, size int64
	for _, e := range entries {
		size += int64(s.opt.estimateSize(e))
		count++
	}
	if count >= s.opt.maxBatchCount || size >= s.opt.maxBatchSize {
		return nil, ErrTxnTooBig
	}

	// We can only service one request because we need each txn to be stored in a contigous section.
	// Txns should not interleave among other txns or rewrites.
	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	s.writeCh <- req
	y.NumPuts.Add(int64(len(entries)))

	return req, nil
}

// batchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
//   Check(kv.BatchSet(entries))
func (s *KV) batchSet(entries []*Entry) error {
	req, err := s.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	req.Wg.Wait()
	req.Entries = nil
	requestPool.Put(req)
	return req.Err
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//   err := kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//   }
func (s *KV) batchSetAsync(entries []*Entry, f func(error)) error {
	req, err := s.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		req.Wg.Wait()
		err := req.Err
		req.Entries = nil
		requestPool.Put(req)
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

var errNoRoom = errors.New("No room for write")

// ensureRoomForWrite is always called serially.
func (s *KV) ensureRoomForWrite() error {
	var err error
	s.Lock()
	defer s.Unlock()
	if s.mt.MemSize() < s.opt.MaxTableSize {
		return nil
	}

	y.AssertTrue(s.mt != nil) // A nil mt indicates that KV is being closed.
	select {
	case s.flushChan <- flushTask{s.mt, s.vptr}:
		s.elog.Printf("Flushing value log to disk if async mode.")
		// Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
		err = s.vlog.sync()
		if err != nil {
			return err
		}

		s.elog.Printf("Flushing memtable, mt.size=%d size of flushChan: %d\n",
			s.mt.MemSize(), len(s.flushChan))
		// We manage to push this task. Let's modify imm.
		s.imm = append(s.imm, s.mt)
		s.mt = skl.NewSkiplist(arenaSize(&s.opt))
		// New memtable is empty. We certainly have room.
		return nil
	default:
		// We need to do this to unlock and allow the flusher to modify imm.
		return errNoRoom
	}
}

func arenaSize(opt *Options) int64 {
	return opt.MaxTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
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
	_, err := f.Write(b.Finish())
	return err
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

func (s *KV) flushMemtable(lc *y.Closer) error {
	defer lc.Done()

	for ft := range s.flushChan {
		if ft.mt == nil {
			return nil
		}

		if !ft.vptr.IsZero() {
			s.elog.Printf("Storing offset: %+v\n", ft.vptr)
			offset := make([]byte, vptrSize)
			ft.vptr.Encode(offset)

			// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
			// commits.
			headTs := y.KeyWithTs(head, s.txnState.commitTs())
			ft.mt.Put(headTs, y.ValueStruct{Value: offset})
		}
		fileID := s.lc.reserveFileID()
		fd, err := y.CreateSyncedFile(table.NewFilename(fileID, s.opt.Dir), true)
		if err != nil {
			return y.Wrap(err)
		}

		// Don't block just to sync the directory entry.
		dirSyncCh := make(chan error)
		go func() { dirSyncCh <- syncDir(s.opt.Dir) }()

		err = writeLevel0Table(ft.mt, fd)
		dirSyncErr := <-dirSyncCh

		if err != nil {
			s.elog.Errorf("ERROR while writing to level 0: %v", err)
			return err
		}
		if dirSyncErr != nil {
			s.elog.Errorf("ERROR while syncing level directory: %v", dirSyncErr)
			return err
		}

		tbl, err := table.OpenTable(fd, s.opt.TableLoadingMode)
		if err != nil {
			s.elog.Printf("ERROR while opening table: %v", err)
			return err
		}
		// We own a ref on tbl.
		err = s.lc.addLevel0Table(tbl) // This will incrRef (if we don't error, sure)
		tbl.DecrRef()                  // Releases our ref.
		if err != nil {
			return err
		}

		// Update s.imm. Need a lock.
		s.Lock()
		y.AssertTrue(ft.mt == s.imm[0]) //For now, single threaded.
		s.imm = s.imm[1:]
		ft.mt.DecrRef() // Return memory.
		s.Unlock()
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (s *KV) updateSize(lc *y.Closer) {
	defer lc.Done()

	metricsTicker := time.NewTicker(5 * time.Minute)
	defer metricsTicker.Stop()

	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if ext == ".sst" {
				lsmSize += info.Size()
			} else if ext == ".vlog" {
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			s.elog.Printf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	for {
		select {
		case <-metricsTicker.C:
			lsmSize, vlogSize := totalSize(s.opt.Dir)
			y.LSMSize.Set(s.opt.Dir, newInt(lsmSize))
			// If valueDir is different from dir, we'd have to do another walk.
			if s.opt.ValueDir != s.opt.Dir {
				_, vlogSize = totalSize(s.opt.ValueDir)
			}
			y.VlogSize.Set(s.opt.Dir, newInt(vlogSize))
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// RunValueLogGC would trigger a value log garbage collection with no guarantees that a call would
// result in a space reclaim. Every run would in the best case rewrite only one log file. So,
// repeated calls may be necessary.
//
// The way it currently works is that it would randomly pick up a value log file, and sample it. If
// the sample shows that we can discard at least discardRatio space of that file, it would be
// rewritten. Else, an ErrNoRewrite error would be returned indicating that the GC didn't result in
// any file rewrite.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the
// space can be discarded.  This results in a lifetime value log write amplification of 2 (1 from
// original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result
// in fewer space reclaims, while setting it to a lower value would result in more space reclaims at
// the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0),
// both endpoints excluded, otherwise an ErrInvalidRequest is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or KV has been closed, this
// would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
func (s *KV) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return ErrInvalidRequest
	}
	return s.vlog.runGC(discardRatio)
}
