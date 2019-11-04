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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
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

func walFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.log", dirPath, string(os.PathSeparator), fid)
}

// logManager will takes care of both WAL and vlog replaying and writing.
type logManager struct {
	sync.RWMutex
	walWritten           uint32
	vlogWritten          uint32
	maxWalID             uint32
	maxVlogID            uint32
	vlogFilesTobeDeleted []uint32
	numActiveIterators   int32
	opt                  Options
	wal                  *logFile
	vlog                 *logFile
	db                   *DB
	elog                 trace.EventLog
	filesLock            sync.RWMutex
	vlogFileMap          map[uint32]*logFile
	lfDiscardStats       *lfDiscardStats
	garbageCh            chan struct{}
}

// openLogManager will replay all the logs and give back the logmanager struct.
func openLogManager(db *DB, vhead valuePointer, walhead valuePointer,
	replayFn logEntry) (*logManager, error) {
	manager := &logManager{
		opt:         db.opt,
		db:          db,
		elog:        y.NoEventLog,
		maxWalID:    0,
		maxVlogID:   0,
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
	// Take all WAL file.
	walFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".log")
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating map in openLogManager")
	}
	// filter the wal files that needs to be replayed.
	filteredWALIDs := []uint32{}
	for fid := range walFiles {
		// Calculate the max wal id.
		if fid > manager.maxWalID {
			manager.maxWalID = fid
		}
		// Filter wal id that needs to be replayed.
		if fid < walhead.Fid {
			// Delete the wal file if is not needed any more.
			if !db.opt.ReadOnly {
				path := walFilePath(manager.opt.ValueDir, uint32(fid))
				if err := os.Remove(path); err != nil {
					return nil, y.Wrapf(err, "Error while removing log file %d", fid)
				}
			}
			continue
		}
		filteredWALIDs = append(filteredWALIDs, fid)
	}

	// We filtered all the WAL file that needs to replayed. Now, We're going
	// to pick vlog files that needs to be replayed.
	vlogFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".vlog")
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating vlog files")
	}
	// filter the vlog files that needs to be replayed.
	filteredVlogIDs := []uint32{}
	for fid := range vlogFiles {
		// set max vlog ID.
		if fid > manager.maxVlogID {
			manager.maxVlogID = fid
		}
		if fid < vhead.Fid {
			// Skip the vlog files that we don't need to replay.
			continue
		}
		filteredVlogIDs = append(filteredVlogIDs, fid)
	}
	// Sort all the ids.
	sort.Slice(filteredWALIDs, func(i, j int) bool {
		return filteredWALIDs[i] < filteredWALIDs[j]
	})
	sort.Slice(filteredVlogIDs, func(i, j int) bool {
		return filteredVlogIDs[i] < filteredVlogIDs[j]
	})
	replayer := logReplayer{
		walIDs:      filteredWALIDs,
		vlogIDs:     filteredVlogIDs,
		vhead:       vhead,
		opt:         db.opt,
		keyRegistry: db.registry,
		whead:       walhead,
	}
	// replay the log.
	err = replayer.replay(replayFn)
	if err != nil {
		return nil, y.Wrapf(err, "Error while replaying log")
	}

	if manager.maxWalID == 0 {
		// No WAL files and vlog file so advancing both the ids.
		y.AssertTrue(manager.maxVlogID == 0)
		manager.maxWalID++
		wal, err := manager.createlogFile(manager.maxWalID, WAL)
		if err != nil {
			return nil, y.Wrapf(err, "Error while creating wal file %d", manager.maxWalID)
		}
		// No need to lock here. Since we're creating the log manager.
		manager.wal = wal
		manager.maxVlogID++
		vlog, err := manager.createlogFile(manager.maxVlogID,
			VLOG)
		if err != nil {
			return nil, y.Wrapf(err, "Error while creating vlog file %d", manager.maxVlogID)
		}
		if err = vlog.init(); err != nil {
			return nil, y.Wrapf(err, "Error while init vlog file %d", vlog.fid)
		}
		manager.vlog = vlog
		manager.vlogFileMap[manager.maxVlogID] = vlog
		// mmap the current vlog.
		if err = manager.vlog.mmap(2 * manager.opt.ValueLogFileSize); err != nil {
			return nil, y.Wrapf(err, "Error while mmaping vlog file %d", manager.vlog.fid)
		}
		return manager, nil
	}

	// Populate all log files.
	vlogFiles, err = y.PopulateFilesForSuffix(db.opt.ValueDir, ".vlog")
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating vlog filesS")
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
			return nil, y.Wrapf(err, "Error while opening vlog file %d", fid)
		}
		// Only initialize the the vlog which is not current vlog.
		// Because, we need to mmap the last vlog for higher number to do the further
		// write.
		if fid < manager.maxVlogID {
			if err = vlogFile.init(); err != nil {
				return nil, y.Wrapf(err, "Error while init vlog file %d", vlogFile.fid)
			}
		}
		manager.vlogFileMap[fid] = vlogFile
	}

	if manager.opt.ReadOnly {
		// Initialize the last vlog file as well.
		lf := manager.vlogFileMap[manager.maxVlogID]
		if err = lf.init(); err != nil {
			return nil, y.Wrapf(err, "Error while init vlog file %d", lf.fid)
		}
		// No need for wal file in read only mode.
		return manager, nil
	}

	wal := &logFile{
		fid:         manager.maxWalID,
		path:        walFilePath(manager.opt.ValueDir, manager.maxWalID),
		loadingMode: manager.opt.ValueLogLoadingMode,
		registry:    manager.db.registry,
	}
	if err = wal.open(walFilePath(manager.opt.ValueDir, manager.maxWalID), flags); err != nil {
		return nil, y.Wrapf(err, "Error while opening wal file %d", manager.maxWalID)
	}
	// seek to the end
	offset, err := wal.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, y.Wrapf(err, "Error while seek end for the wal %d", wal.fid)
	}
	wal.offset = uint32(offset)
	manager.wal = wal
	manager.vlog = manager.vlogFileMap[manager.maxVlogID]
	// seek to the end
	offset, err = manager.vlog.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, y.Wrapf(err, "Error while seek end for the value log %d", manager.vlog.fid)
	}
	manager.vlog.offset = uint32(offset)
	// mmap the current vlog.
	if err = manager.vlog.mmap(2 * manager.opt.ValueLogFileSize); err != nil {
		return nil, y.Wrapf(err, "Error while mmaping vlog file %d", manager.vlog.fid)
	}
	if err := manager.populateDiscardStats(); err != nil {
		// Print the error and continue. We don't want to prevent value log open if there's an error
		// with the fetching discards stats.
		db.opt.Errorf("Failed to populate discard stats: %s", err)
	}
	return manager, nil
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

// logReplayer is used to replay all the log.
type logReplayer struct {
	walIDs      []uint32
	vlogIDs     []uint32
	vhead       valuePointer
	opt         Options
	keyRegistry *KeyRegistry
	whead       valuePointer
}

// replay will take replayFn as input and replayed will replay all the entries to the
// memtable.
func (lp *logReplayer) replay(replayFn logEntry) error {
	var flags uint32
	truncateNeeded := false
	switch {
	case lp.opt.ReadOnly:
		// If we have read only, we don't need SyncWrites.
		flags |= y.ReadOnly
		// Set sync flag.
	case lp.opt.SyncWrites:
		flags |= y.Sync
	}
	// No need to replay if all the SST's are flushed properly.
	if len(lp.walIDs) == 0 {
		y.AssertTrue(len(lp.vlogIDs) == 0)
		return nil
	}

	currentVlogIndex := 0
	vlogFile := &logFile{
		fid:         uint32(lp.vlogIDs[currentVlogIndex]),
		path:        vlogFilePath(lp.opt.ValueDir, uint32(lp.vlogIDs[currentVlogIndex])),
		loadingMode: lp.opt.ValueLogLoadingMode,
		registry:    lp.keyRegistry,
	}
	err := vlogFile.open(vlogFilePath(lp.opt.ValueDir, uint32(lp.vlogIDs[currentVlogIndex])), flags)
	if err != nil {
		return y.Wrapf(err, "Error while opening vlog file %d in log replayer", lp.vlogIDs[currentVlogIndex])
	}

	vlogOffset := uint32(vlogHeaderSize)
	if vlogFile.fid == lp.vhead.Fid {
		vlogOffset = lp.vhead.Offset + lp.vhead.Len
	}
	if vlogFile.fileOffset() < vlogOffset {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.vlogIDs) == 1)
		truncateNeeded = true
	}
	currentWalIndex := 0
	vlogIterator, err := newLogIterator(vlogFile, vlogOffset, lp.opt)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for the vlog file %s", vlogFile.path)
	}
	walFile := &logFile{
		fid:         uint32(lp.walIDs[currentWalIndex]),
		path:        walFilePath(lp.opt.ValueDir, uint32(lp.walIDs[currentWalIndex])),
		loadingMode: lp.opt.ValueLogLoadingMode,
		registry:    lp.keyRegistry,
	}
	err = walFile.open(walFile.path, flags)
	if err != nil {
		return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
			lp.walIDs[currentWalIndex])
	}
	walOffset := uint32(vlogHeaderSize)
	if walFile.fid == lp.whead.Fid {
		walOffset = lp.whead.Offset + lp.whead.Len
	}
	if walFile.fileOffset() < walOffset {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.walIDs) == 1)
		truncateNeeded = true
	}
	walIterator, err := newLogIterator(walFile, walOffset, lp.opt)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for the wal file %s", walFile.path)
	}
	walEntries, walCommitTs, walErr := walIterator.iterateEntries()
	vlogEntries, vlogCommitTs, vlogErr := vlogIterator.iterateEntries()

	isTruncateNeeded := func(validOffset uint32, log *logFile) (bool, error) {
		info, err := log.fd.Stat()
		if err != nil {
			return false, err
		}
		return info.Size() != int64(validOffset), nil
	}
	for {
		if walErr == errTruncate || vlogErr == errTruncate || truncateNeeded {
			truncateNeeded = true
			break
		}

		// If any of the log reaches EOF we need to advance both the log file because vlog and wal has 1 to 1 mapping
		// isTruncateNeeded check will take care of truncation.
		if walErr == io.ErrUnexpectedEOF || walErr == io.EOF || vlogErr == io.ErrUnexpectedEOF || vlogErr == io.EOF {
			var err error
			// check whether we iterated till the valid offset.
			truncateNeeded, err = isTruncateNeeded(walIterator.validOffset, walFile)
			if err != nil {
				return y.Wrapf(err, "Error while checking truncation for the wal file %s",
					walFile.path)
			}
			// close the log file.
			err = walFile.fd.Close()
			if err != nil {
				return y.Wrapf(err, "Error while closing the WAL file %s in replay", walFile.path)
			}
			// We successfully iterated till the end of the file. Now we have to advance
			// the wal File.
			if currentWalIndex >= len(lp.walIDs)-1 {
				// WAL is completed but we still need to check vlog is corruped or not.
				// because WAL and vlog is one to one mapping.
				// we'll check whether we need truncation only if there is no truncation for wal file.
				if !truncateNeeded {
					// check whether we iterated till the valid offset.
					truncateNeeded, err = isTruncateNeeded(vlogIterator.validOffset, vlogFile)
					if err != nil {
						return y.Wrapf(err, "Error while checking truncation for the vlog file %s",
							walFile.path)
					}
				}
				break
			}
			// advance both the wal and vlog.
			currentWalIndex++
			currentVlogIndex++
			walFile = &logFile{
				fid:         uint32(lp.walIDs[currentWalIndex]),
				path:        walFilePath(lp.opt.ValueDir, uint32(lp.walIDs[currentWalIndex])),
				loadingMode: lp.opt.ValueLogLoadingMode,
				registry:    lp.keyRegistry,
			}
			err = walFile.open(walFile.path, flags)
			if err != nil {
				return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
					lp.walIDs[currentWalIndex])
			}

			if walFile.fileOffset() < vlogHeaderSize {
				// we simply change the flag and advance vlog so that vlog id will be advanced.
				truncateNeeded = true
			} else {
				walIterator, err = newLogIterator(walFile, vlogHeaderSize, lp.opt)
				walEntries, walCommitTs, walErr = walIterator.iterateEntries()
			}

			// we'll check whether we need truncation only if there is no truncation for wal file.
			if !truncateNeeded {
				// check whether we iterated till the valid offset.
				truncateNeeded, err = isTruncateNeeded(vlogIterator.validOffset, vlogFile)
				if err != nil {
					return y.Wrapf(err, "Error while checking truncation for the vlog file %s",
						walFile.path)
				}
			}

			// close the current vlogs file.
			err = vlogFile.fd.Close()
			if err != nil {
				return y.Wrapf(err, "Error while closing the vlog file %s in replay", vlogFile.path)
			}
			// advance for the next vlog file.
			vlogFile = &logFile{
				fid:         uint32(lp.walIDs[currentVlogIndex]),
				path:        vlogFilePath(lp.opt.ValueDir, uint32(lp.walIDs[currentVlogIndex])),
				loadingMode: lp.opt.ValueLogLoadingMode,
				registry:    lp.keyRegistry,
			}
			err = vlogFile.open(vlogFile.path, flags)
			if err != nil {
				return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
					lp.walIDs[currentVlogIndex])
			}
			if vlogFile.fileOffset() < vlogHeaderSize {
				truncateNeeded = true
			} else {
				vlogIterator, err = newLogIterator(vlogFile, vlogHeaderSize, lp.opt)
				vlogEntries, vlogCommitTs, vlogErr = vlogIterator.iterateEntries()
			}
			continue
		}
		// Some error other than truncation and end of file so handle it.
		if walErr != nil || vlogErr != nil {
			msg := ""
			if walErr != nil {
				msg += walErr.Error()
			}
			if vlogErr != nil {
				msg += vlogErr.Error()
			}
			return y.Wrapf(errors.New(msg), "Error while replay log")
		}
		// Both batch entries are not of same txn. So we need truncate here.
		if vlogCommitTs != walCommitTs {
			truncateNeeded = true
			break
		}
		replayed := false
		// Insert the entries back to LSM.
		for _, e := range walEntries {
			// Inserting empty value pointer since the value pointer are not going to lsm.
			if err := replayFn(*e, valuePointer{}); err != nil {
				return y.Wrapf(err, "Error while inserting entry to lsm.")
			}
			replayed = true
		}
		for _, e := range vlogEntries {
			vp := valuePointer{
				Offset: e.offset,
				Len:    uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size),
				Fid:    vlogFile.fid,
			}
			if err := replayFn(*e, vp); err != nil {
				return y.Wrapf(err, "Error while inserting entry to lsm.")
			}
			replayed = true
		}
		isGc := vlogCommitTs == math.MaxUint64

		if replayed && !isGc {
			// we replayed all the entries here. so marking finish txn so the entries for the
			// this txn goes to LSM. We can't send finish mark without replaying atleast one entry.
			// so this case exist.
			// we set finish mark only for txn entries not for gc entries.
			e := &Entry{
				Key:   y.KeyWithTs(txnKeyVlog, walCommitTs),
				Value: []byte(strconv.FormatUint(walCommitTs, 10)),
				meta:  bitFinTxn,
			}
			if err := replayFn(*e, valuePointer{}); err != nil {
				return y.Wrapf(err, "Error while inserting finish mark to lsm.")
			}
		}
		// Advance for next batch of txn entries.
		walEntries, walCommitTs, walErr = walIterator.iterateEntries()
		vlogEntries, vlogCommitTs, vlogErr = vlogIterator.iterateEntries()
	}

	if truncateNeeded {
		if !lp.opt.Truncate || lp.opt.ReadOnly {
			return ErrTruncateNeeded
		}
		// Here not handling any corruption in the middle. It is expected that all the log file before
		// are in good state. In previous implementation, the log files are deleted if truncation
		// enabled. we can do the same if necessary.

		// we can truncate only last file.
		y.AssertTrue(len(lp.walIDs)-1 == currentWalIndex)
		y.AssertTrue(len(lp.vlogIDs)-1 == currentVlogIndex)

		// wal file and vlog files are closed, we need to open it now.
		walFile := &logFile{
			fid:         uint32(lp.walIDs[currentWalIndex]),
			path:        walFilePath(lp.opt.ValueDir, uint32(lp.walIDs[currentWalIndex])),
			loadingMode: lp.opt.ValueLogLoadingMode,
			registry:    lp.keyRegistry,
		}
		err = walFile.open(walFile.path, flags)
		if err != nil {
			return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
				lp.walIDs[currentWalIndex])
		}
		vlogFile = &logFile{
			fid:         uint32(lp.walIDs[currentVlogIndex]),
			path:        vlogFilePath(lp.opt.ValueDir, uint32(lp.walIDs[currentVlogIndex])),
			loadingMode: lp.opt.ValueLogLoadingMode,
			registry:    lp.keyRegistry,
		}
		err = vlogFile.open(vlogFile.path, flags)
		if err != nil {
			return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
				lp.walIDs[currentVlogIndex])
		}
		walStat, err := walFile.fd.Stat()
		if err != nil {
			return y.Wrapf(err, "Error while retriving wal file %d stat", walFile.fid)
		}
		vlogStat, err := vlogFile.fd.Stat()
		if err != nil {
			return y.Wrapf(err, "Error while retriving vlog file %d stat", vlogFile.fid)
		}
		if walStat.Size() < vlogHeaderSize || vlogStat.Size() < vlogHeaderSize {
			// which means the whole file is corrupted so we need to bootstarp both the log file.
			if err = walFile.bootstrap(); err != nil {
				return y.Wrapf(err, "Error while bootstraping wal file %d", walFile.fid)
			}
			if err = vlogFile.bootstrap(); err != nil {
				return y.Wrapf(err, "Error while bootstraping vlog file %d", vlogFile.fid)
			}
			// we have bootstraped the files properly. We'll close it now the log files. logmanager will
			// open again and use it.
			if err = walFile.fd.Close(); err != nil {
				return y.Wrapf(err, "Error whole closing wal file %d", walFile.fid)
			}

			return vlogFile.fd.Close()
		}
		// Now we have to figure out, what offset that need to truncated for the wal and vlog.
		offset := uint32(0)
		// if ts is zero, then that file is corrupted.
		if walCommitTs == 0 {
			// wal file is corrupted so the offset is the valid offset.
			offset = walIterator.validOffset
		} else {
			// wal is not corrupted. so the batch for the current transaction is corrupted in val.
			// so truncating to the last batch offset.
			offset = walIterator.previousOffset
		}

		// None of the log files are mmaped so far, so it is good to truncate here.
		if err = walFile.fd.Truncate(int64(offset)); err != nil {
			return y.Wrapf(err, "Error while truncating wal file %d", walFile.fid)
		}
		walFile.offset = offset
		offset = uint32(0)
		if vlogCommitTs == 0 {
			// vlog file is corrupted so the offset is the valid offset.
			offset = vlogIterator.validOffset
		} else {
			// vlog is not corrupted. so the batch for the current transaction is corrupted in wal.
			// so truncating to the last batch offset.
			offset = vlogIterator.previousOffset
		}
		// we'll calculate the offset, by using the same mechanism which we used for wal
		if err = vlogFile.fd.Truncate(int64(offset)); err != nil {
			return y.Wrapf(err, "Error while truncating vlog file %d", vlogFile.fid)
		}
		vlogFile.offset = offset
	}
	return nil
}

// logIterator is used to iterate batch of transaction entries in the log file.
// It is used for replay.
type logIterator struct {
	entryReader    *safeRead
	reader         *bufio.Reader
	validOffset    uint32
	previousOffset uint32
}

// newLogIterator will return the log iterator.
func newLogIterator(log *logFile, offset uint32, opt Options) (*logIterator, error) {
	stat, err := log.fd.Stat()
	if err != nil {
		return nil, err
	}
	if opt.ReadOnly {
		if int64(offset) != stat.Size() {
			// We're not at the end of the file. We'd need to replay the entries, or
			// possibly truncate the file.
			return nil, ErrReplayNeeded
		}
	}
	_, err = log.fd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &logIterator{
		entryReader: &safeRead{
			k:            make([]byte, 10),
			v:            make([]byte, 10),
			recordOffset: offset,
			decrypter: &logDecrypter{
				baseIV:  log.baseIV,
				dataKey: log.dataKey,
			},
		},
		previousOffset: offset,
		reader:         bufio.NewReader(log.fd),
		validOffset:    offset,
	}, nil
}

// iterateEntries will iterate entries batch by batch. The batch end of transaction is
// determined by finish txn mark.
func (iterator *logIterator) iterateEntries() ([]*Entry, uint64, error) {
	var commitTs uint64
	var entries []*Entry
	for {
		e, err := iterator.entryReader.Entry(iterator.reader)
		if err != nil {
			return nil, 0, err
		}
		// advance the reader offset
		entryLen := uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size)
		iterator.entryReader.recordOffset += entryLen

		// This is txn entries.
		if e.meta&bitTxn > 0 {
			txnTs := y.ParseTs(e.Key)
			if commitTs == 0 {
				commitTs = txnTs
			}
			if commitTs != txnTs {
				// we got an entry here without finish mark so, revinding the state.
				entries = []*Entry{}
				return entries, 0, errTruncate
			}
			entries = append(entries, e)
			continue
		}
		// Here it is finish txn mark.
		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				entries = []*Entry{}
				return entries, 0, err
			}
			if commitTs == 0 && txnTs == math.MaxUint64 {
				// we got finish mark for gc. so no need to check commitTs != txnTs
				iterator.validOffset = iterator.entryReader.recordOffset
				commitTs = math.MaxUint64
				break
			}
			// If there is no entries means no entries from the current txn is not part
			// of log files. we only got finish mark. so we're not checking commitTs != txnTs
			if len(entries) != 0 && commitTs != txnTs {
				entries = []*Entry{}
				return entries, 0, errTruncate
			}
			// We got finish mark for this entry batch. Now, the iteration for this entry batch
			// is done so stoping the iteration for this ts.
			commitTs = txnTs
			iterator.previousOffset = iterator.validOffset
			iterator.validOffset = iterator.entryReader.recordOffset
			break
		}

		// This entries are from gc. so appending to the entries as it is.
		entries = append(entries, e)
	}
	return entries, commitTs, nil
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
			lf, err := manager.rotateLog(VLOG)
			if err != nil {
				return y.Wrapf(err, "Error while creating new vlog file %d", manager.maxVlogID)
			}
			vlog = lf
			atomic.AddInt32(&manager.db.logRotates, 1)
			lf, err = manager.rotateLog(WAL)
			if err != nil {
				return y.Wrapf(err, "Error while creating new wal file %d", manager.maxWalID)
			}
			wal = lf
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
func (manager *logManager) rotateLog(logtype logType) (*logFile, error) {
	manager.Lock()
	defer manager.Unlock()
	// close the current log file
	fid := uint32(0)
	// get the path and fid based on the log type.
	switch logtype {
	case WAL:
		manager.maxWalID++
		fid = manager.maxWalID
		break
	case VLOG:
		manager.maxVlogID++
		fid = manager.maxVlogID
	}
	lf, err := manager.createlogFile(fid, logtype)
	if err != nil {
		return nil, y.Wrapf(err, "Error while creating log file %d of log type %d", fid, logtype)
	}
	// switch the log file according to the type
	switch logtype {
	case WAL:
		// we don't mmap wal so just close it.
		if err = manager.wal.fd.Close(); err != nil {
			return nil, y.Wrapf(err, "Error while closing WAL file in rotateLog %d", manager.wal.fid)
		}
		manager.wal = lf
		break
	case VLOG:
		// Here we mmaped the file so don't close it. This log file is part of vlog filesMap and it is used by
		// value pointer. doneWriting will take take care of unmmap, truncate and mmap it back.
		if err = manager.vlog.doneWriting(manager.vlog.fileOffset()); err != nil {
			return nil, y.Wrapf(err, "Error while doneWriting vlog %d", manager.vlog.fid)
		}
		manager.vlog = lf
		// update the files map.
		manager.filesLock.Lock()
		defer manager.filesLock.Unlock()
		manager.vlogFileMap[lf.fid] = lf
	}
	return lf, nil
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

		maxFid := atomic.LoadUint32(&manager.maxVlogID)
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
