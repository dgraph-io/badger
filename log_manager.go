/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

type logType uint32

const (
	VLOG logType = iota
	WAL
)
const (
	walFileSuffix   = ".log"
	valueFileSuffix = ".vlog"
)

func walFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.log", dirPath, string(os.PathSeparator), fid)
}

// logManager will takes care of both WAL and vlog replaying and writing.
type logManager struct {
	sync.RWMutex
	walWritten           uint32
	vlogWritten          uint32
	maxLogID             uint32
	vlogFilesTobeDeleted []uint32
	// we need to know number of active iterator when we go GC. beacuse,
	// we can delete the log files, when itreator using the log file.
	numActiveIterators int32
	opt                Options
	// latest log file that going to be used for write.
	wal            *logFile
	vlog           *logFile
	db             *DB
	elog           trace.EventLog
	filesLock      sync.RWMutex
	vlogFileMap    map[uint32]*logFile
	lfDiscardStats *lfDiscardStats
	garbageCh      chan struct{}
}

// openLogManager will replay all the logs and give back the logmanager struct.
func openLogManager(db *DB, vhead valuePointer, walhead valuePointer,
	replayFn logEntry) (*logManager, error) {
	manager := &logManager{
		opt:         db.opt,
		db:          db,
		elog:        y.NoEventLog,
		maxLogID:    0,
		vlogFileMap: map[uint32]*logFile{},
		garbageCh:   make(chan struct{}, 1),
		lfDiscardStats: &lfDiscardStats{
			m:         make(map[uint32]int64),
			closer:    y.NewCloser(1),
			flushChan: make(chan map[uint32]int64, 16),
		},
	}
	if manager.opt.EventLogging {
		manager.elog = trace.NewEventLog("Badger", "LogManager")
	}
	// logFilesToBeReplayed will picks the log file that needs to be replayed.
	logFilesToBeReplayed := func(logIDS map[uint32]struct{}, head valuePointer,
		logtype logType) ([]uint32, error) {
		toBeReplayed := []uint32{}
		for fid := range logIDS {
			// calculate the max LogID.
			if fid > manager.maxLogID {
				manager.maxLogID = fid
			}
			// Filter log that needs to be replayed.
			if fid < head.Fid {
				// Delete the wal file since it's not needed anymore.
				if !db.opt.ReadOnly && logtype == WAL {
					path := walFilePath(manager.opt.ValueDir, uint32(fid))
					if err := os.Remove(path); err != nil {
						return nil, y.Wrapf(err, "Error while removing log file %d", fid)
					}
				}
				continue
			}
			toBeReplayed = append(toBeReplayed, fid)
		}
		sort.Slice(toBeReplayed, func(i, j int) bool {
			return toBeReplayed[i] < toBeReplayed[j]
		})
		return toBeReplayed, nil
	}
	// Take all WAL file.
	walFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, walFileSuffix)
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating map in openLogManager")
	}

	// pick log files that needs to be replayed.
	filteredWALIDs, err := logFilesToBeReplayed(walFiles, walhead, WAL)
	if err != nil {
		return nil, y.Wrapf(err, "Error while picking wal files for replaying")
	}

	// We filtered all the WAL file that needs to replayed. Now, We're going
	// to pick vlog files that needs to be replayed.
	vlogFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".vlog")
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating vlog files")
	}
	// filter the vlog files that needs to be replayed.
	filteredVlogIDs, err := logFilesToBeReplayed(vlogFiles, vhead, VLOG)
	if err != nil {
		return nil, y.Wrapf(err, "Error while picking vlog files for replaying")
	}

	if manager.maxLogID == 0 {
		y.AssertTrue(len(filteredVlogIDs) == 0)
		y.AssertTrue(len(filteredWALIDs) == 0)
		if err = manager.bootstrapManager(); err != nil {
			return nil, y.Wrapf(err, "Error while bootstrping log manager")
		}
		return manager, nil
	}
	replayer := logReplayer{
		walIDs:      filteredWALIDs,
		vlogIDs:     filteredVlogIDs,
		vhead:       vhead,
		opt:         db.opt,
		keyRegistry: db.registry,
		whead:       walhead,
	}
	// replay the log.
	if err = replayer.replay(replayFn); err != nil {
		return nil, y.Wrapf(err, "Error while replaying log")
	}
	if err = manager.initLogManager(); err != nil {
		return nil, y.Wrapf(err, "Error while intializing log manager")
	}
	return manager, nil
}

func (manager *logManager) bootstrapManager() error {
	// First time opening DB. So, no need to replay just create log files and give it back.
	manager.maxLogID++
	wal, err := manager.createlogFile(manager.maxLogID, WAL)
	if err != nil {
		return y.Wrapf(err, "Error while creating wal file %d", manager.maxLogID)
	}
	// No need to lock here. Since we're creating the log manager.
	manager.wal = wal
	vlog, err := manager.createlogFile(manager.maxLogID,
		VLOG)
	if err != nil {
		return y.Wrapf(err, "Error while creating vlog file %d", manager.maxLogID)
	}
	if err = vlog.init(); err != nil {
		return y.Wrapf(err, "Error while init vlog file %d", vlog.fid)
	}
	manager.vlog = vlog
	manager.vlogFileMap[manager.maxLogID] = vlog
	// mmap the current vlog.
	return manager.vlog.mmap(2 * manager.opt.ValueLogFileSize)
}

func (manager *logManager) initLogManager() error {
	// Populate all log files.
	vlogFiles, err := y.PopulateFilesForSuffix(manager.db.opt.ValueDir, ".vlog")
	if err != nil {
		return y.Wrapf(err, "Error while populating vlog filesS")
	}
	var flags uint32
	switch {
	case manager.opt.ReadOnly:
		// If we have read only, we don't need SyncWrites.
		flags |= y.ReadOnly
		// Set sync flag.
	case manager.opt.SyncWrites:
		flags |= y.Sync
	}
	// populate vlogFile map.
	for fid := range vlogFiles {
		vlogFile := &logFile{
			fid:         fid,
			path:        vlogFilePath(manager.opt.ValueDir, fid),
			loadingMode: manager.opt.ValueLogLoadingMode,
			registry:    manager.db.registry,
		}
		if err = vlogFile.open(vlogFilePath(manager.opt.ValueDir, fid), flags); err != nil {
			return y.Wrapf(err, "Error while opening vlog file %d", fid)
		}
		// Only initialize the the vlog which is not current vlog.
		// Because, we need to mmap the last vlog for higher number to do the further
		// write.
		if fid != manager.maxLogID {
			if err = vlogFile.init(); err != nil {
				return y.Wrapf(err, "Error while init vlog file %d", vlogFile.fid)
			}
		}
		manager.vlogFileMap[fid] = vlogFile
	}

	if manager.opt.ReadOnly {
		// Initialize the last vlog file as well.
		lf := manager.vlogFileMap[manager.maxLogID]
		if err = lf.init(); err != nil {
			return y.Wrapf(err, "Error while init vlog file %d", lf.fid)
		}
		// No need for wal file in read only mode.
		return nil
	}

	wal := &logFile{
		fid:         manager.maxLogID,
		path:        walFilePath(manager.opt.ValueDir, manager.maxLogID),
		loadingMode: manager.opt.ValueLogLoadingMode,
		registry:    manager.db.registry,
	}
	if err = wal.open(walFilePath(manager.opt.ValueDir, manager.maxLogID), flags); err != nil {
		return y.Wrapf(err, "Error while opening wal file %d", manager.maxLogID)
	}
	// seek to the end
	offset, err := wal.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return y.Wrapf(err, "Error while seek end for the wal %d", wal.fid)
	}
	wal.offset = uint32(offset)
	manager.wal = wal
	manager.vlog = manager.vlogFileMap[manager.maxLogID]
	// seek to the end
	offset, err = manager.vlog.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return y.Wrapf(err, "Error while seek end for the value log %d", manager.vlog.fid)
	}
	manager.vlog.offset = uint32(offset)
	// mmap the current vlog.
	if err = manager.vlog.mmap(2 * manager.opt.ValueLogFileSize); err != nil {
		return y.Wrapf(err, "Error while mmaping vlog file %d", manager.vlog.fid)
	}
	return manager.populateDiscardStats()
}

func (manager *logManager) createlogFile(fid uint32, logtype logType) (*logFile, error) {
	var path string

	switch logtype {
	case WAL:
		path = walFilePath(manager.opt.ValueDir, fid)
	case VLOG:
		path = vlogFilePath(manager.opt.ValueDir, fid)
	}
	lf := &logFile{
		fid:         fid,
		path:        path,
		loadingMode: manager.opt.ValueLogLoadingMode,
		registry:    manager.db.registry,
	}
	// writableLogOffset is only written by write func, by read by Read func.
	// To avoid a race condition, all reads and updates to this variable must be
	// done via atomics.
	var err error
	if lf.fd, err = y.CreateSyncedFile(path, manager.opt.SyncWrites); err != nil {
		return nil, errFile(err, lf.path, "Create value log file")
	}

	if err = lf.bootstrap(); err != nil {
		return nil, err
	}

	if err = syncDir(manager.opt.ValueDir); err != nil {
		return nil, errFile(err, manager.opt.ValueDir, "Sync value log dir")
	}
	// writableLogOffset is only written by write func, by read by Read func.
	// To avoid a race condition, all reads and updates to this variable must be
	// done via atomics.
	atomic.StoreUint32(&lf.offset, vlogHeaderSize)
	if logtype == WAL {
		return lf, nil
	}
	// we mmap only for vlog.
	if err = lf.mmap(2 * manager.opt.ValueLogFileSize); err != nil {
		return nil, errFile(err, lf.path, "Mmap value log file")
	}
	return lf, nil
}

// write will write the log of the request. write method will decide where to write the entry. Whether in
// vlog or wal file.
func (manager *logManager) write(reqs []*request) error {
	vlogBuf := &bytes.Buffer{}
	walBuf := &bytes.Buffer{}
	// get the wal and vlog files, because files may be rotated while db flush.
	// so get the current log files.
	manager.RLock()
	wal := manager.wal
	vlog := manager.vlog
	manager.RUnlock()
	toDisk := func() error {
		// Persist the log to the disk.
		// TODO: make it concurrent. Golang, should give us async interface :(
		if walBuf.Len() == 0 && vlogBuf.Len() == 0 {
			return nil
		}
		var walErr error
		var vlogErr error
		walErr = wal.writeLog(walBuf)
		if walErr != nil {
			return y.Wrapf(walErr, "Error while writing log to WAL %d", wal.fid)
		}
		vlogErr = vlog.writeLog(vlogBuf)
		if vlogErr != nil {
			return y.Wrapf(vlogErr, "Error while writing log to vlog %d", vlog.fid)
		}
		// reset the buf for next batch of entries.
		vlogBuf.Reset()
		walBuf.Reset()
		// TODO: @balaji check the calculation.
		// check whether vlog hits the defined threshold.
		rotate := vlog.fileOffset()+uint32(vlogBuf.Len()) > uint32(manager.opt.ValueLogFileSize) ||
			manager.walWritten > uint32(manager.opt.ValueLogMaxEntries)
		if rotate {
			// we need to rotate both the files here. Because, the trasaction entries have to corresponding
			// entries. This is needed while doing truncation. For example, one vlog file courrupted in the
			// middle. So we delete the vlog file if there is truncation. Then we replay the next vlog file,
			// with different timestamp. the wal file will have lesser timestamp. There, we miss the order.
			// So, it is important to keep WAL and vlog mapping.
			if err := manager.rotateLog(); err != nil {
				return y.Wrapf(err, "Error while rotating log file")
			}
			manager.RLock()
			wal = manager.wal
			vlog = manager.vlog
			manager.RUnlock()
			// reset written entries.
			manager.walWritten = 0
		}
		return nil
	}
	// Process each request.
	for i := range reqs {
		var walWritten uint32
		var vlogWritten uint32
		b := reqs[i]
		// Process this batch.
		y.AssertTrue(len(b.Ptrs) == 0)
	inner:
		// last two entries are end entries for vlog and WAL finish mark. so igoring that.
		for j := 0; j < len(b.Entries); j++ {

			if b.Entries[j].skipVlog {
				b.Ptrs = append(b.Ptrs, valuePointer{})
				continue inner
			}
			var p valuePointer
			var entryOffset uint32
			if b.Entries[j].forceWal {
				// value size is less than threshold. So writing to WAL
				entryOffset = wal.fileOffset() + uint32(walBuf.Len())
				entryLen, err := wal.encodeEntry(b.Entries[j], walBuf, entryOffset)
				if err != nil {
					return y.Wrapf(err, "Error while encoding entry for WAL %d", manager.wal.fid)
				}
				p.Offset = entryOffset
				p.Len = uint32(entryLen)
				p.Fid = wal.fid
				p.log = WAL
				b.Ptrs = append(b.Ptrs, p)
				walWritten++
				continue inner
			}
			// Since the value size is bigger, So we're writing to vlog.
			entryOffset = vlog.fileOffset() + uint32(vlogBuf.Len())
			p.Offset = entryOffset
			entryLen, err := vlog.encodeEntry(b.Entries[j], vlogBuf, entryOffset)
			if err != nil {
				return y.Wrapf(err, "Error while encoding entry for vlog %d", manager.vlog.fid)
			}
			p.Len = uint32(entryLen)
			p.Fid = vlog.fid
			p.log = VLOG
			b.Ptrs = append(b.Ptrs, p)
			vlogWritten++
		}
		y.AssertTrue(len(b.Entries) == len(b.Ptrs))
		// update written metrics
		atomic.AddUint32(&manager.walWritten, walWritten)
		atomic.AddUint32(&manager.vlogWritten, vlogWritten)
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		writeNow :=
			vlog.fileOffset()+uint32(vlogBuf.Len()) > uint32(manager.opt.ValueLogFileSize) ||
				manager.walWritten > uint32(manager.opt.ValueLogMaxEntries)
		if writeNow {
			if err := toDisk(); err != nil {
				return err
			}
		}
	}
	return toDisk()
}

// rotateLog will rotate a new log file based on logType.
func (manager *logManager) rotateLog() error {
	manager.Lock()
	defer manager.Unlock()
	// increment the max ID.
	maxLogID := atomic.AddUint32(&manager.maxLogID, 1)
	lf, err := manager.createlogFile(maxLogID, WAL)
	if err != nil {
		return y.Wrapf(err, "Error while creating wal file %d", maxLogID)
	}
	// we don't mmap wal so just close it.
	if err = manager.wal.fd.Close(); err != nil {
		return y.Wrapf(err, "Error while closing WAL file in rotateLog %d", manager.wal.fid)
	}
	manager.wal = lf
	// rotate vlog.
	lf, err = manager.createlogFile(maxLogID, VLOG)
	if err != nil {
		return y.Wrapf(err, "Error while creating vlog file %d", maxLogID)
	}
	// Here we mmaped the file so don't close it. This log file is part of vlog filesMap and it is used by
	// value pointer. doneWriting will take take care of unmmap, truncate and mmap it back.
	if err = manager.vlog.doneWriting(manager.vlog.fileOffset()); err != nil {
		return y.Wrapf(err, "Error while doneWriting vlog %d", manager.vlog.fid)
	}
	manager.vlog = lf
	// update the files map.
	manager.filesLock.Lock()
	defer manager.filesLock.Unlock()
	manager.vlogFileMap[lf.fid] = lf
	return nil
}

func (manager *logManager) Read(vp valuePointer, s *y.Slice) ([]byte, func(), error) {
	// ASK: do we need this check?
	// Check for valid offset if we are reading to writable log.
	//maxFid := atomic.LoadUint32(&lm.maxVlogID)
	// if vp.Fid == maxFid && vp.Offset >= lm.vlog.fileOffset() {
	// 	return nil, nil, errors.Errorf(
	// 		"Invalid value pointer offset: %d greater than current offset: %d",
	// 		vp.Offset, lm.vlog.fileOffset())
	// }
	buf, lf, err := manager.readValueBytes(vp, s)
	// log file is locked so, decide whether to lock immediately or let the caller to
	// unlock it, after caller uses it.
	cb := manager.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
	}
	if manager.opt.VerifyValueChecksum {
		hash := crc32.New(y.CastagnoliCrcTable)
		if _, err := hash.Write(buf); err != nil {
			runCallback(cb)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		// Fetch checksum from the end of the buffer.
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != y.BytesToU32(checksum) {
			runCallback(cb)
			return nil, nil, errors.Wrapf(y.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}
	var h header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	if lf.encryptionEnabled() {
		kv, err = lf.decryptKV(kv, vp.Offset)
		if err != nil {
			return nil, cb, err
		}
	}
	return kv[h.klen : h.klen+h.vlen], cb, nil
}

// getUnlockCallback will returns a function which unlock the logfile if the logfile is mmaped.
// otherwise, it unlock the logfile and return nil.
func (manager *logManager) getUnlockCallback(lf *logFile) func() {
	if lf == nil {
		return nil
	}
	if manager.opt.ValueLogLoadingMode == options.MemoryMap {
		return lf.lock.RUnlock
	}
	lf.lock.RUnlock()
	return nil
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (manager *logManager) getFileRLocked(fid uint32) (*logFile, error) {
	manager.filesLock.RLock()
	defer manager.filesLock.RUnlock()
	ret, ok := manager.vlogFileMap[fid]
	if !ok {
		// log file has gone away, will need to retry the operation.
		return nil, ErrRetry
	}
	ret.lock.RLock()
	return ret, nil
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (manager *logManager) readValueBytes(vp valuePointer, s *y.Slice) ([]byte, *logFile, error) {
	lf, err := manager.getFileRLocked(vp.Fid)
	if err != nil {
		return nil, nil, err
	}
	buf, err := lf.read(vp, s)
	return buf, lf, err
}

func (manager *logManager) Close() error {
	manager.elog.Printf("Stopping garbage collection of values.")
	defer manager.elog.Finish()

	var err error
	for id, f := range manager.vlogFileMap {
		f.lock.Lock() // We won’t release the lock.
		if munmapErr := f.munmap(); munmapErr != nil && err == nil {
			err = munmapErr
		}

		maxFid := atomic.LoadUint32(&manager.maxLogID)
		if !manager.opt.ReadOnly && id == maxFid {
			// truncate writable log file to correct offset.
			if truncErr := f.fd.Truncate(
				int64(f.fileOffset())); truncErr != nil && err == nil {
				err = truncErr
			}
		}

		if closeErr := f.fd.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closedErr := manager.wal.fd.Close(); closedErr != nil {
		err = closedErr
	}
	return err
}

// populateDiscardStats populates vlog.lfDiscardStats.
// This function will be called while initializing logmanger.
func (manager *logManager) populateDiscardStats() error {
	key := y.KeyWithTs(lfDiscardStatsKey, math.MaxUint64)
	var statsMap map[uint32]int64
	var val []byte
	var vp valuePointer
	for {
		vs, err := manager.db.get(key)
		if err != nil {
			return err
		}
		// Value doesn't exist.
		if vs.Meta == 0 && len(vs.Value) == 0 {
			manager.opt.Debugf("Value log discard stats empty")
			return nil
		}
		vp.Decode(vs.Value)
		// Entry stored in LSM tree.
		if vs.Meta&bitValuePointer == 0 {
			val = y.SafeCopy(val, vs.Value)
			break
		}
		// Read entry from value log.
		result, cb, err := manager.Read(vp, new(y.Slice))
		runCallback(cb)
		val = y.SafeCopy(val, result)
		// The result is stored in val. We can break the loop from here.
		if err == nil {
			break
		}
		if err != ErrRetry {
			return err
		}
		// If we're at this point it means we haven't found the value yet and if the current key has
		// badger move prefix, we should break from here since we've already tried the original key
		// and the key with move prefix. "val" would be empty since we haven't found the value yet.
		if bytes.HasPrefix(key, badgerMove) {
			break
		}
		// If we're at this point it means the discard stats key was moved by the GC and the actual
		// entry is the one prefixed by badger move key.
		// Prepend existing key with badger move and search for the key.
		key = append(badgerMove, key...)
	}

	if len(val) == 0 {
		return nil
	}
	if err := json.Unmarshal(val, &statsMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	manager.opt.Debugf("Value Log Discard stats: %v", statsMap)
	manager.lfDiscardStats.flushChan <- statsMap
	return nil
}
