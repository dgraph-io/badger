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
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

type logType uint32

const (
	VLOG logType = iota
	WAL
)

type logManager struct {
	opt                  Options
	wal                  *logFile
	vlog                 *logFile
	db                   *DB
	walWritten           uint32
	vlogWritten          uint32
	elog                 trace.EventLog
	maxWalID             uint32
	maxVlogID            uint32
	filesLock            sync.RWMutex
	vlogFileMap          map[uint32]*logFile
	lfDiscardStats       *lfDiscardStats
	garbageCh            chan struct{}
	vlogFilesTobeDeleted []uint32
	sync.RWMutex
	numActiveIterators int32
}

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

	// if manager.maxWalID == 0 && walhead.Fid > 0 {
	// 	// wal file should have file number in increasing order. if the maxWalHead is
	// 	// set to zero means. all the memtables are persisted and log files are flushed
	// 	// so advancing the maxWalId to walHead.
	// 	manager.maxWalID = walhead.Fid
	// }

	// We filtered all the WAL file that needs to replayed. Now, We're going
	// to pick vlog files that needs to be replayed.
	vlogFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".vlog")
	if err != nil {
		return nil, y.Wrapf(err, "Error while populating vlog files")
	}
	// filter the vlog files that needs to be replayed.
	filteredVlogIDs := []uint32{}
	for fid := range vlogFiles {
		//
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
	err = replayer.replay(replayFn)
	if err != nil {
		return nil, y.Wrapf(err, "Error while replaying log")
	}

	if manager.maxWalID == 0 {
		// No WAL files and vlog file so advancing both the ids.
		y.AssertTrue(manager.maxVlogID == 0)
		manager.maxWalID++
		wal, err := manager.createlogFile(manager.maxWalID,
			WAL)
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
		if fid < manager.maxVlogID {
			if err = vlogFile.init(); err != nil {
				return nil, y.Wrapf(err, "Error while init vlog file %d", vlogFile.fid)
			}
		}
		manager.vlogFileMap[fid] = vlogFile
	}

	if manager.opt.ReadOnly {
		// No need for wal file in read only mode.
		return manager, nil
	}

	// if manager.maxWalID == walhead.Fid || walhead.Fid == 0 {
	// 	// Last persisted SST's wal so need to create new WAL file.
	// 	manager.maxWalID++
	// 	wal, err := manager.createNewWal()
	// 	if err != nil {
	// 		return manager, err
	// 	}
	// 	manager.wal = wal
	// 	return manager, nil
	// }
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

type logReplayer struct {
	walIDs      []uint32
	vlogIDs     []uint32
	vhead       valuePointer
	opt         Options
	keyRegistry *KeyRegistry
	whead       valuePointer
}

func (lp *logReplayer) replay(replayFn logEntry) error {
	// NOTES: what to truncate. how we truncate?
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
		fid:         uint32(lp.vlogIDs[0]),
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
		vlogOffset = lp.vhead.Offset
	}
	if vlogFile.fileOffset() < vlogOffset {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.vlogIDs) == 1)
		truncateNeeded = true
	}
	currentWalIndex := 0
	vlogIterator, err := newLogIterator(vlogFile, vlogOffset)
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
		walOffset = lp.whead.Offset
	}
	if walFile.fileOffset() < walOffset {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.walIDs) == 1)
		truncateNeeded = true
	}
	walIterator, err := newLogIterator(walFile, walOffset)
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
		if walErr == errTruncate || vlogErr == errTruncate {
			truncateNeeded = true
			break
		}

		// Advance wal if we reach end of the current wal file
		if walErr == io.ErrUnexpectedEOF || walErr == io.EOF {
			var err error
			// check whether we iterated till the valid offset.
			truncateNeeded, err = isTruncateNeeded(walIterator.validOffset, walFile)
			if err != nil {
				return y.Wrapf(err, "Error while checking truncation for the wal file %s",
					walFile.path)
			}
			if truncateNeeded {
				break
			}
			// close the log file.
			err = walFile.fd.Close()
			if err != nil {
				return y.Wrapf(err, "Error while closing the WAL file %s in replay", walFile.path)
			}
			// We successfully iterated till the end of the file. Now we have to advance
			// the wal File.
			if currentWalIndex < len(lp.walIDs) {
				break
			}
			currentWalIndex++
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
			if walFile.fileOffset() < vlogHeaderSize {
				truncateNeeded = true
				break
			}
			walIterator, err = newLogIterator(walFile, vlogHeaderSize)
			walEntries, walCommitTs, walErr = walIterator.iterateEntries()
			continue
		}
		// Advance vlog if we reach the end of this present log file.
		if vlogErr == io.ErrUnexpectedEOF || vlogErr == io.EOF {
			var err error
			// check whether we iterated till the valid offset.
			truncateNeeded, err = isTruncateNeeded(vlogIterator.validOffset, vlogFile)
			if err != nil {
				return y.Wrapf(err, "Error while checking truncation for the vlog file %s",
					walFile.path)
			}
			if truncateNeeded {
				break
			}
			// close the log file.
			err = vlogFile.fd.Close()
			if err != nil {
				return y.Wrapf(err, "Error while closing the vlog file %s in replay", vlogFile.path)
			}
			// We successfully iterated till the end of the file. Now we have to advance
			// the wal File.
			if currentVlogIndex < len(lp.vlogIDs) {
				break
			}
			currentVlogIndex++
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
				break
			}
			vlogIterator, err = newLogIterator(vlogFile, vlogHeaderSize)
			vlogEntries, vlogCommitTs, vlogErr = vlogIterator.iterateEntries()
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
		if !lp.opt.Truncate {
			return ErrTruncateNeeded
		}
		// Here not handling any corruption in the middle. It is expected that all the log file before
		// are in good state. In previous implementation, the log files are deleted if truncation
		// enabled. we can do the same if necessary.

		// we can truncate only last file.
		y.AssertTrue(len(lp.walIDs)-1 == currentWalIndex)
		y.AssertTrue(len(lp.vlogIDs)-1 == currentVlogIndex)
		if lp.opt.ReadOnly {
			return ErrTruncateNeeded
		}
		offset := uint32(0)
		if walCommitTs == 0 {
			// wal file is corrupted so the offset is the valid offset.
			offset = walIterator.validOffset
		} else {
			// wal is not corrupted. so the batch for the current transaction is corrupted in val.
			// so truncating to the last batch offset.
			offset = walIterator.previousOffset
		}
		// None of the log files are mmaped so far, so it is good to truncate here.
		var err error
		if err = walFile.fd.Truncate(int64(offset)); err != nil {
			return y.Wrapf(err, "Error while truncating wal file %d", walFile.fid)
		}
		walFile.offset = offset
		offset = uint32(0)
		if vlogCommitTs == 0 {
			// wal file is corrupted so the offset is the valid offset.
			offset = vlogIterator.validOffset
		} else {
			// wal is not corrupted. so the batch for the current transaction is corrupted in val.
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

type logIterator struct {
	entryReader    *safeRead
	reader         *bufio.Reader
	validOffset    uint32
	previousOffset uint32
}

func newLogIterator(log *logFile, offset uint32) (*logIterator, error) {
	_, err := log.fd.Seek(int64(offset), io.SeekStart)
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
				commitTs = math.MaxInt64
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

func (lm *logManager) write(reqs []*request) error {
	vlogBuf := &bytes.Buffer{}
	walBuf := &bytes.Buffer{}
	// get the wal and vlog files, because files may be rotated while db flush.
	// so get the current log files.
	lm.RLock()
	wal := lm.wal
	vlog := lm.vlog
	lm.RUnlock()
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
		rotate := vlog.fileOffset()+uint32(vlogBuf.Len()) > uint32(lm.opt.ValueLogFileSize) ||
			lm.walWritten > uint32(lm.opt.ValueLogMaxEntries)
		if rotate {
			// we need to rotate both the files here. Because, the trasaction entries have to corresponding entries.
			// This is needed while doing truncation. For example, one vlog file courrupted in the middle. So we delete the
			// vlog file if there is truncation. Then we replay the next vlog file, with different timestamp. the wal file
			// will have lesser timestamp. There, we miss the order. So, it is important to keep WAL and vlog mapping.
			lf, err := lm.rotateLog(VLOG)
			if err != nil {
				return y.Wrapf(err, "Error while creating new vlog file %d", lm.maxVlogID)
			}
			vlog = lf
			atomic.AddInt32(&lm.db.logRotates, 1)
			lf, err = lm.rotateLog(WAL)
			if err != nil {
				return y.Wrapf(err, "Error while creating new wal file %d", lm.maxWalID)
			}
			wal = lf
			// reset written entries.
			lm.walWritten = 0
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
				_, err := wal.encode(b.Entries[j], walBuf, entryOffset)
				if err != nil {
					return y.Wrapf(err, "Error while encoding entry for WAL %d", lm.wal.fid)
				}
				p.Offset = entryOffset
				p.Fid = wal.fid
				p.log = WAL
				b.Ptrs = append(b.Ptrs, p)
				walWritten++
				continue inner
			}
			// Since the value size is bigger, So we're writing to vlog.
			entryOffset = vlog.fileOffset() + uint32(vlogBuf.Len())
			p.Offset = entryOffset
			entryLen, err := vlog.encode(b.Entries[j], vlogBuf, entryOffset)
			if err != nil {
				return y.Wrapf(err, "Error while encoding entry for vlog %d", lm.vlog.fid)
			}
			p.Len = uint32(entryLen)
			p.Fid = vlog.fid
			p.log = VLOG
			b.Ptrs = append(b.Ptrs, p)
			vlogWritten++
		}
		y.AssertTrue(len(b.Entries) == len(b.Ptrs))
		// update written metrics
		atomic.AddUint32(&lm.walWritten, walWritten)
		atomic.AddUint32(&lm.vlogWritten, vlogWritten)
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		writeNow :=
			vlog.fileOffset()+uint32(vlogBuf.Len()) > uint32(lm.opt.ValueLogFileSize) ||
				lm.walWritten > uint32(lm.opt.ValueLogMaxEntries)
		if writeNow {
			if err := toDisk(); err != nil {
				return err
			}
		}
	}
	return toDisk()
}

func (lm *logManager) Read(vp valuePointer, s *y.Slice) ([]byte, func(), error) {
	// ASK: do we need this check?
	// Check for valid offset if we are reading to writable log.
	//maxFid := atomic.LoadUint32(&lm.maxVlogID)
	// if vp.Fid == maxFid && vp.Offset >= lm.vlog.fileOffset() {
	// 	return nil, nil, errors.Errorf(
	// 		"Invalid value pointer offset: %d greater than current offset: %d",
	// 		vp.Offset, lm.vlog.fileOffset())
	// }
	fmt.Println(vp.Offset)
	buf, lf, err := lm.readValueBytes(vp, s)
	// log file is locked so, decide whether to lock immediately or let the caller to
	// unlock it, after caller uses it.
	cb := lm.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
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
func (lm *logManager) getUnlockCallback(lf *logFile) func() {
	if lf == nil {
		return nil
	}
	if lm.opt.ValueLogLoadingMode == options.MemoryMap {
		return lf.lock.RUnlock
	}
	lf.lock.RUnlock()
	return nil
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (lm *logManager) getFileRLocked(fid uint32) (*logFile, error) {
	lm.filesLock.RLock()
	defer lm.filesLock.RUnlock()
	ret, ok := lm.vlogFileMap[fid]
	if !ok {
		// log file has gone away, will need to retry the operation.
		return nil, ErrRetry
	}
	ret.lock.RLock()
	return ret, nil
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (lm *logManager) readValueBytes(vp valuePointer, s *y.Slice) ([]byte, *logFile, error) {
	lf, err := lm.getFileRLocked(vp.Fid)
	if err != nil {
		return nil, nil, err
	}
	buf, err := lf.read(vp, s)
	return buf, lf, err
}

func (lm *logManager) Close() error {
	lm.elog.Printf("Stopping garbage collection of values.")
	defer lm.elog.Finish()

	var err error
	for id, f := range lm.vlogFileMap {
		f.lock.Lock() // We won’t release the lock.
		if munmapErr := f.munmap(); munmapErr != nil && err == nil {
			err = munmapErr
		}

		maxFid := atomic.LoadUint32(&lm.maxVlogID)
		if !lm.opt.ReadOnly && id == maxFid {
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
	return err
}

func (lm *logManager) sync(uint32) error {
	return nil
}

func (lm *logManager) dropAll() (int, error) {
	// We don't want to block dropAll on any pending transactions. So, don't worry about iterator
	// count.
	var count int
	deleteAll := func() error {
		lm.filesLock.Lock()
		defer lm.filesLock.Unlock()
		for _, lf := range lm.vlogFileMap {
			if err := lm.deleteLogFile(lf, VLOG); err != nil {
				return err
			}
			count++
		}
		lm.vlogFileMap = make(map[uint32]*logFile)
		return nil
	}
	var err error
	if err = deleteAll(); err != nil {
		return count, err
	}
	// close the current wal.
	lm.Lock()
	defer lm.Unlock()
	if err = lm.wal.fd.Close(); err != nil {
		return count, y.Wrapf(err, "Erro while closing wal file %d", lm.wal.fid)
	}

	// delete the current log file.
	if err = lm.deleteLogFile(lm.wal, WAL); err != nil {
		return count, y.Wrapf(err, "Error while removing wal file %d", lm.wal.fid)
	}
	// delete all the wal files in the directory.
	walFiles, err := y.PopulateFilesForSuffix(lm.opt.ValueDir, ".log")
	if err != nil {
		return count, y.Wrapf(err, "Error while obtaining all wal files detail in dropAll")
	}
	for fid := range walFiles {
		path := walFilePath(lm.opt.ValueDir, fid)
		if err = os.Remove(path); err != nil {
			return count, y.Wrapf(err, "Error while deleting wal file %s", path)
		}
	}
	atomic.StoreUint32(&lm.maxWalID, 1)
	atomic.StoreUint32(&lm.maxVlogID, 1)
	lm.db.opt.Infof("Value logs and WAl are  deleted. Creating value log and wal file")
	// create wal file.
	wal, err := lm.createlogFile(1, WAL)
	if err != nil {
		return count, y.Wrapf(err, "Error while creating wal file %d", 1)
	}
	lm.wal = wal
	// create vlog file.
	vlog, err := lm.createlogFile(1, VLOG)
	if err != nil {
		return count, y.Wrapf(err, "Error while creating vlog file %d", 1)
	}
	lm.vlog = vlog
	return count, nil
}
func (lm *logManager) incrIteratorCount() {
	atomic.AddInt32(&lm.numActiveIterators, 1)
}

func (lm *logManager) decrIteratorCount() error {
	num := atomic.AddInt32(&lm.numActiveIterators, -1)
	if num != 0 {
		return nil
	}

	lm.filesLock.Lock()
	lfs := make([]*logFile, 0, len(lm.vlogFilesTobeDeleted))
	for _, id := range lm.vlogFilesTobeDeleted {
		lfs = append(lfs, lm.vlogFileMap[id])
		delete(lm.vlogFileMap, id)
	}
	lm.vlogFilesTobeDeleted = nil
	lm.filesLock.Unlock()

	for _, lf := range lfs {
		if err := lm.deleteLogFile(lf, VLOG); err != nil {
			return err
		}
	}
	return nil
}

func (lm *logManager) deleteLogFile(lf *logFile, logtype logType) error {
	if lf == nil {
		return nil
	}
	lf.lock.Lock()
	defer lf.lock.Unlock()
	var path string
	switch logtype {
	case WAL:
		path = walFilePath(lm.opt.ValueDir, lf.fid)
	case VLOG:
		path = vlogFilePath(lm.opt.ValueDir, lf.fid)
		if err := lf.munmap(); err != nil {
			_ = lf.fd.Close()
			return err
		}
	}
	if err := lf.fd.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}
func (lm *logManager) iteratorCount() int {
	return int(atomic.LoadInt32(&lm.numActiveIterators))
}

func (lm *logManager) flushDiscardStats() {
	defer lm.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[uint32]int64) ([]byte, error) {
		lm.lfDiscardStats.Lock()
		defer lm.lfDiscardStats.Unlock()
		for fid, count := range stats {
			lm.lfDiscardStats.m[fid] += count
			lm.lfDiscardStats.updatesSinceFlush++
		}

		if lm.lfDiscardStats.updatesSinceFlush > discardStatsFlushThreshold {
			encodedDS, err := json.Marshal(lm.lfDiscardStats.m)
			if err != nil {
				return nil, err
			}
			lm.lfDiscardStats.updatesSinceFlush = 0
			return encodedDS, nil
		}
		return nil, nil
	}

	process := func(stats map[uint32]int64) error {
		encodedDS, err := mergeStats(stats)
		if err != nil || encodedDS == nil {
			return err
		}

		entries := []*Entry{{
			Key:   y.KeyWithTs(lfDiscardStatsKey, 1),
			Value: encodedDS,
		}}
		req, err := lm.db.sendToWriteCh(entries)
		// No special handling of ErrBlockedWrites is required as err is just logged in
		// for loop below.
		if err != nil {
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := lm.lfDiscardStats.closer
	for {
		select {
		case <-closer.HasBeenClosed():
			// For simplicity just return without processing already present in stats in flushChan.
			return
		case stats := <-lm.lfDiscardStats.flushChan:
			if err := process(stats); err != nil {
				lm.opt.Errorf("unable to process discardstats with error: %s", err)
			}
		}
	}
}

func (lm *logManager) updateDiscardStats(stats map[uint32]int64) {
	select {
	case lm.lfDiscardStats.flushChan <- stats:
	default:
		lm.opt.Warningf("updateDiscardStats called: discard stats flushChan full, " +
			"returning without pushing to flushChan")
	}
}

func (lm *logManager) rotateLog(logtype logType) (*logFile, error) {
	lm.Lock()
	defer lm.Unlock()
	// close the current log file
	fid := uint32(0)
	// get the path and fid based on the log type.
	switch logtype {
	case WAL:
		lm.maxWalID++
		fid = lm.maxWalID
		break
	case VLOG:
		lm.maxVlogID++
		fid = lm.maxVlogID
	}
	lf, err := lm.createlogFile(fid, logtype)
	if err != nil {
		return nil, y.Wrapf(err, "Error while creating log file %d of log type %d", fid, logtype)
	}
	// switch the log file according to the type
	switch logtype {
	case WAL:
		// we don't mmap wal so just close it.
		if err = lm.wal.fd.Close(); err != nil {
			return nil, y.Wrapf(err, "Error while closing WAL file in rotateLog %d", lm.wal.fid)
		}
		lm.wal = lf
		break
	case VLOG:
		// Here we mmaped the file so don't close it. This log file is part of vlog filesMap and it is used by
		// value pointer. doneWriting will take take care of unmmap, truncate and mmap it back.
		if err = lm.vlog.doneWriting(lm.vlog.fileOffset()); err != nil {
			return nil, y.Wrapf(err, "Error while doneWriting vlog %d", lm.vlog.fid)
		}
		lm.vlog = lf
		// update the files map.
		lm.filesLock.Lock()
		defer lm.filesLock.Unlock()
		lm.vlogFileMap[lf.fid] = lf
	}
	return lf, nil
}

func (manager *logManager) currentWalID() uint32 {
	return atomic.LoadUint32(&manager.maxWalID)
}

func (manager *logManager) deleteWal(ID uint32) error {
	return os.Remove(walFilePath(manager.opt.ValueDir, ID))
}

// sortedFids returns the file id's not pending deletion, sorted.  Assumes we have shared access to
// filesMap.
func (manager *logManager) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range manager.vlogFilesTobeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	ret := make([]uint32, 0, len(manager.vlogFileMap))
	for fid := range manager.vlogFileMap {
		if _, ok := toBeDeleted[fid]; !ok {
			ret = append(ret, fid)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func (manager *logManager) deleteVlogLogFile(lf *logFile) error {
	if lf == nil {
		return nil
	}
	lf.lock.Lock()
	defer lf.lock.Unlock()

	path := vlogFilePath(manager.opt.ValueDir, lf.fid)
	if err := lf.munmap(); err != nil {
		_ = lf.fd.Close()
		return err
	}
	if err := lf.fd.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}

func (manager *logManager) waitOnGC(lc *y.Closer) {
	defer lc.Done()

	<-lc.HasBeenClosed() // Wait for lc to be closed.

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	manager.garbageCh <- struct{}{}
}

func (manager *logManager) runGC(discardRatio float64, head valuePointer) error {
	select {
	case manager.garbageCh <- struct{}{}:
		// Pick a log file for GC.
		tr := trace.New("Badger.ValueLog", "GC")
		tr.SetMaxEvents(100)
		defer func() {
			tr.Finish()
			<-manager.garbageCh
		}()

		var err error
		files := manager.pickLog(head, tr)
		if len(files) == 0 {
			tr.LazyPrintf("PickLog returned zero results.")
			return ErrNoRewrite
		}
		tried := make(map[uint32]bool)
		for _, lf := range files {
			if _, done := tried[lf.fid]; done {
				continue
			}
			tried[lf.fid] = true
			err = manager.doRunGC(lf, discardRatio, tr)
			if err == nil {
				return manager.deleteMoveKeysFor(lf.fid, tr)
			}
		}
		return err
	default:
		return ErrRejected
	}
}

func (manager *logManager) deleteMoveKeysFor(fid uint32, tr trace.Trace) error {
	db := manager.db
	var result []*Entry
	var count, pointers uint64
	tr.LazyPrintf("Iterating over move keys to find invalids for fid: %d", fid)
	err := db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.InternalAccess = true
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()

		for itr.Seek(badgerMove); itr.ValidForPrefix(badgerMove); itr.Next() {
			count++
			item := itr.Item()
			if item.meta&bitValuePointer == 0 {
				continue
			}
			pointers++
			var vp valuePointer
			vp.Decode(item.vptr)
			if vp.Fid == fid {
				e := &Entry{Key: y.KeyWithTs(item.Key(), item.Version()), meta: bitDelete}
				result = append(result, e)
			}
		}
		return nil
	})
	if err != nil {
		tr.LazyPrintf("Got error while iterating move keys: %v", err)
		tr.SetError()
		return err
	}
	tr.LazyPrintf("Num total move keys: %d. Num pointers: %d", count, pointers)
	tr.LazyPrintf("Number of invalid move keys found: %d", len(result))
	batchSize := 10240
	for i := 0; i < len(result); {
		end := i + batchSize
		if end > len(result) {
			end = len(result)
		}
		if err := db.batchSet(result[i:end]); err != nil {
			if err == ErrTxnTooBig {
				batchSize /= 2
				tr.LazyPrintf("Dropped batch size to %d", batchSize)
				continue
			}
			tr.LazyPrintf("Error while doing batchSet: %v", err)
			tr.SetError()
			return err
		}
		i += batchSize
	}
	tr.LazyPrintf("Move keys deletion done.")
	return nil
}

func (manager *logManager) pickLog(head valuePointer, tr trace.Trace) (files []*logFile) {
	manager.filesLock.RLock()
	defer manager.filesLock.RUnlock()
	fids := manager.sortedFids()
	if len(fids) <= 1 {
		tr.LazyPrintf("Only one or less value log file.")
		return nil
	} else if head.Fid == 0 {
		tr.LazyPrintf("Head pointer is at zero.")
		return nil
	}

	// Pick a candidate that contains the largest amount of discardable data
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}
	manager.lfDiscardStats.Lock()
	for _, fid := range fids {
		if fid >= head.Fid {
			break
		}
		if manager.lfDiscardStats.m[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = manager.lfDiscardStats.m[fid]
		}
	}
	manager.lfDiscardStats.Unlock()

	if candidate.fid != math.MaxUint32 { // Found a candidate
		tr.LazyPrintf("Found candidate via discard stats: %v", candidate)
		files = append(files, manager.vlogFileMap[candidate.fid])
	} else {
		tr.LazyPrintf("Could not find candidate via discard stats. Randomly picking one.")
	}

	// Fallback to randomly picking a log file
	var idxHead int
	for i, fid := range fids {
		if fid == head.Fid {
			idxHead = i
			break
		}
	}
	if idxHead == 0 { // Not found or first file
		tr.LazyPrintf("Could not find any file.")
		return nil
	}
	idx := rand.Intn(idxHead) // Don’t include head.Fid. We pick a random file before it.
	if idx > 0 {
		idx = rand.Intn(idx + 1) // Another level of rand to favor smaller fids.
	}
	tr.LazyPrintf("Randomly chose fid: %d", fids[idx])
	files = append(files, manager.vlogFileMap[fids[idx]])
	return files
}

func (manager *logManager) rewrite(f *logFile, tr trace.Trace) error {
	maxFid := atomic.LoadUint32(&manager.maxVlogID)
	y.AssertTruef(uint32(f.fid) < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)
	tr.LazyPrintf("Rewriting fid: %d", f.fid)

	wb := make([]*Entry, 0, 1000)
	var size int64

	y.AssertTrue(manager.db != nil)
	var count, moved int
	fe := func(e *Entry) error {
		count++
		if count%100000 == 0 {
			tr.LazyPrintf("Processing entry %d", count)
		}

		vs, err := manager.db.get(e.Key)
		if err != nil {
			return err
		}
		if discardEntry(*e, vs) {
			return nil
		}

		// Value is still present in value log.
		if len(vs.Value) == 0 {
			return errors.Errorf("Empty value: %+v", vs)
		}
		var vp valuePointer
		vp.Decode(vs.Value)

		if vp.Fid > f.fid {
			return nil
		}
		if vp.Offset > e.offset {
			return nil
		}
		if vp.Fid == f.fid && vp.Offset == e.offset {
			moved++
			// This new entry only contains the key, and a pointer to the value.
			ne := new(Entry)
			ne.meta = 0 // Remove all bits. Different keyspace doesn't need these bits.
			ne.UserMeta = e.UserMeta
			ne.ExpiresAt = e.ExpiresAt

			// Create a new key in a separate keyspace, prefixed by moveKey. We are not
			// allowed to rewrite an older version of key in the LSM tree, because then this older
			// version would be at the top of the LSM tree. To work correctly, reads expect the
			// latest versions to be at the top, and the older versions at the bottom.
			if bytes.HasPrefix(e.Key, badgerMove) {
				ne.Key = append([]byte{}, e.Key...)
			} else {
				ne.Key = make([]byte, len(badgerMove)+len(e.Key))
				n := copy(ne.Key, badgerMove)
				copy(ne.Key[n:], e.Key)
			}

			ne.Value = append([]byte{}, e.Value...)
			es := int64(ne.estimateSize(manager.opt.ValueThreshold))
			// Ensure length and size of wb is within transaction limits.
			if int64(len(wb)+3) >= manager.opt.maxBatchCount ||
				size+es >= manager.opt.maxBatchSize {
				tr.LazyPrintf("request has %d entries, size %d", len(wb), size)
				// set finish mark for wal
				wb = append(wb, &Entry{
					Key:      y.KeyWithTs(txnKey, math.MaxUint64),
					Value:    []byte(strconv.FormatUint(math.MaxUint64, 10)),
					meta:     bitFinTxn,
					forceWal: true,
				})
				// set finish mark for vlog
				wb = append(wb, &Entry{
					Key:   y.KeyWithTs(txnKeyVlog, math.MaxUint64),
					Value: []byte(strconv.FormatUint(math.MaxUint64, 10)),
					meta:  bitFinTxn,
				})
				if err := manager.db.batchSet(wb); err != nil {
					return err
				}
				size = 0
				wb = wb[:0]
			}
			wb = append(wb, ne)
			size += es
		} else {
			manager.db.opt.Warningf("This entry should have been caught. %+v\n", e)
		}
		return nil
	}

	iterator, err := newLogIterator(f, vlogHeaderSize)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for vlog %d in logmanager.rewrite", f.fid)
	}
	for {
		entries, _, err := iterator.iterateEntries()
		if err == io.EOF {
			break
		} else if err != nil {
			return y.Wrapf(err, "Error while iterating entries for the vlog file %d", f.fid)
		}
		for _, e := range entries {
			if err := fe(e); err != nil {
				return y.Wrapf(err, "Error while rewriting entry")
			}
		}
	}

	tr.LazyPrintf("request has %d entries, size %d", len(wb), size)
	batchSize := 1024
	var loops int
	for i := 0; i < len(wb); {
		// TODO: asdf.
		loops++
		if batchSize == 0 {
			manager.db.opt.Warningf("We shouldn't reach batch size of zero.")
			return ErrNoRewrite
		}
		end := i + batchSize
		if end > len(wb) {
			end = len(wb)
		}
		batch := wb[i:end]
		// set finish mark for this batch.
		batch = append(batch, &Entry{
			Key:      y.KeyWithTs(txnKey, math.MaxUint64),
			Value:    []byte(strconv.FormatUint(math.MaxUint64, 10)),
			meta:     bitFinTxn,
			forceWal: true,
		})
		batch = append(batch, &Entry{
			Key:   y.KeyWithTs(txnKeyVlog, math.MaxUint64),
			Value: []byte(strconv.FormatUint(math.MaxUint64, 10)),
			meta:  bitFinTxn,
		})
		if err := manager.db.batchSet(batch); err != nil {
			if err == ErrTxnTooBig {
				// Decrease the batch size to half.
				batchSize = batchSize / 2
				tr.LazyPrintf("Dropped batch size to %d", batchSize)
				continue
			}
			return err
		}
		i += batchSize
	}
	tr.LazyPrintf("Processed %d entries in %d loops", len(wb), loops)
	tr.LazyPrintf("Total entries: %d. Moved: %d", count, moved)
	tr.LazyPrintf("Removing fid: %d", f.fid)
	var deleteFileNow bool
	// Entries written to LSM. Remove the older file now.
	{
		manager.filesLock.Lock()
		// Just a sanity-check.
		if _, ok := manager.vlogFileMap[f.fid]; !ok {
			manager.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", f.fid)
		}
		if manager.iteratorCount() == 0 {
			delete(manager.vlogFileMap, f.fid)
			deleteFileNow = true
		} else {
			manager.vlogFilesTobeDeleted = append(manager.vlogFilesTobeDeleted, f.fid)
		}
		manager.filesLock.Unlock()
	}

	if deleteFileNow {
		if err := manager.deleteVlogLogFile(f); err != nil {
			return err
		}
	}

	return nil
}

func (manager *logManager) doRunGC(lf *logFile, discardRatio float64, tr trace.Trace) (err error) {
	// Update stats before exiting
	defer func() {
		if err == nil {
			manager.lfDiscardStats.Lock()
			delete(manager.lfDiscardStats.m, lf.fid)
			manager.lfDiscardStats.Unlock()
		}
	}()

	type reason struct {
		total   float64
		discard float64
		count   int
	}

	fi, err := lf.fd.Stat()
	if err != nil {
		tr.LazyPrintf("Error while finding file size: %v", err)
		tr.SetError()
		return err
	}

	// Set up the sampling window sizes.
	sizeWindow := float64(fi.Size()) * 0.1                             // 10% of the file as window.
	sizeWindowM := sizeWindow / (1 << 20)                              // in MBs.
	countWindow := int(float64(manager.opt.ValueLogMaxEntries) * 0.01) // 1% of num entries.
	tr.LazyPrintf("Size window: %5.2f. Count window: %d.", sizeWindow, countWindow)

	// Pick a random start point for the log.
	skipFirstM := float64(rand.Int63n(fi.Size())) // Pick a random starting location.
	skipFirstM -= sizeWindow                      // Avoid hitting EOF by moving back by window.
	skipFirstM /= float64(mi)                     // Convert to MBs.
	tr.LazyPrintf("Skip first %5.2f MB of file of size: %d MB", skipFirstM, fi.Size()/mi)
	var skipped float64

	var r reason
	start := time.Now()
	y.AssertTrue(manager.db != nil)
	s := new(y.Slice)
	var numIterations int
	iterator, err := newLogIterator(lf, vlogHeaderSize)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator vlog %d", lf.fid)
	}
	for {
		entries, _, err := iterator.iterateEntries()
		if err == io.EOF {
			break
		} else if err != nil {
			return y.Wrapf(err, "Error while iterating entries in vlog %d", lf.fid)
		}
		for _, e := range entries {
			vp := valuePointer{
				Len:    uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size),
				Fid:    lf.fid,
				Offset: e.offset,
			}
			numIterations++
			esz := float64(vp.Len) / (1 << 20) // in MBs.
			if skipped < skipFirstM {
				skipped += esz
				return nil
			}

			// Sample until we reach the window sizes or exceed 10 seconds.
			if r.count > countWindow {
				tr.LazyPrintf("Stopping sampling after %d entries.", countWindow)
				return errStop
			}
			if r.total > sizeWindowM {
				tr.LazyPrintf("Stopping sampling after reaching window size.")
				return errStop
			}
			if time.Since(start) > 10*time.Second {
				tr.LazyPrintf("Stopping sampling after 10 seconds.")
				return errStop
			}
			r.total += esz
			r.count++

			vs, err := manager.db.get(e.Key)
			if err != nil {
				return err
			}
			if discardEntry(*e, vs) {
				r.discard += esz
				return nil
			}

			// Value is still present in value log.
			y.AssertTrue(len(vs.Value) > 0)
			vp.Decode(vs.Value)

			if vp.Fid > lf.fid {
				// Value is present in a later log. Discard.
				r.discard += esz
				return nil
			}
			if vp.Offset > e.offset {
				// Value is present in a later offset, but in the same log.
				r.discard += esz
				return nil
			}
			if vp.Fid == lf.fid && vp.Offset == e.offset {
				// This is still the active entry. This would need to be rewritten.

			} else {
				manager.elog.Printf("Reason=%+v\n", r)
				buf, lf, err := manager.readValueBytes(vp, s)
				// we need to decide, whether to unlock the lock file immediately based on the
				// loading mode. getUnlockCallback will take care of it.
				cb := manager.getUnlockCallback(lf)
				if err != nil {
					runCallback(cb)
					return errStop
				}
				ne, err := lf.decodeEntry(buf, vp.Offset)
				if err != nil {
					runCallback(cb)
					return errStop
				}
				ne.print("Latest Entry Header in LSM")
				e.print("Latest Entry in Log")
				runCallback(cb)
				return errors.Errorf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.",
					vp, vs.Meta)
			}
		}
	}

	tr.LazyPrintf("Fid: %d. Skipped: %5.2fMB Num iterations: %d. Data status=%+v\n",
		lf.fid, skipped, numIterations, r)

	// If we couldn't sample at least a 1000 KV pairs or at least 75% of the window size,
	// and what we can discard is below the threshold, we should skip the rewrite.
	if (r.count < countWindow && r.total < sizeWindowM*0.75) || r.discard < discardRatio*r.total {
		tr.LazyPrintf("Skipping GC on fid: %d", lf.fid)
		return ErrNoRewrite
	}
	if err = manager.rewrite(lf, tr); err != nil {
		return err
	}
	tr.LazyPrintf("Done rewriting.")
	return nil
}

// populateDiscardStats populates vlog.lfDiscardStats.
// This function will be called while initializing valueLog.
func (lm *logManager) populateDiscardStats() error {
	key := y.KeyWithTs(lfDiscardStatsKey, math.MaxUint64)
	var statsMap map[uint32]int64
	var val []byte
	var vp valuePointer
	for {
		vs, err := lm.db.get(key)
		if err != nil {
			return err
		}
		// Value doesn't exist.
		if vs.Meta == 0 && len(vs.Value) == 0 {
			lm.opt.Debugf("Value log discard stats empty")
			return nil
		}
		vp.Decode(vs.Value)
		// Entry stored in LSM tree.
		if vs.Meta&bitValuePointer == 0 {
			val = y.SafeCopy(val, vs.Value)
			break
		}
		// Read entry from value log.
		result, cb, err := lm.Read(vp, new(y.Slice))
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
	lm.opt.Debugf("Value Log Discard stats: %v", statsMap)
	lm.lfDiscardStats.flushChan <- statsMap
	return nil
}
