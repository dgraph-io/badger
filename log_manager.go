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
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

type logManager struct {
	opt            Options
	wal            *logFile
	vlog           *logFile
	db             *DB
	entriesWritten uint32
	elog           trace.EventLog
}

func openLogManager(db *DB, vhead valuePointer, walhead uint64,
	replayFn logEntry) error {
	manager := &logManager{
		opt:  db.opt,
		db:   db,
		elog: y.NoEventLog,
	}
	if manager.opt.EventLogging {
		manager.elog = trace.NewEventLog("Badger", "LogManager")
	}
	walFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".log")
	if err != nil {
		return y.Wrapf(err, "Error while populating map in openLogManager")
	}
	// filter the wal files that needs to be replayed.
	filteredWALIDs := []uint64{}
	for fid := range walFiles {
		if fid < walhead {
			// Delete the wal file if is not needed any more.
			if !db.opt.ReadOnly {
				path := walFilePath(manager.opt.ValueDir, uint32(fid))
				if err := os.Remove(path); err != nil {
					return y.Wrapf(err, "Error while removing log file %d", fid)
				}
			}
			continue
		}
		filteredWALIDs = append(filteredWALIDs, fid)
	}

	// We filtered all the WAL file that needs to replayed. Now, We're going
	// to pick vlog files that needs to be replayed.
	vlogFiles, err := y.PopulateFilesForSuffix(db.opt.ValueDir, ".vlog")
	// filter the vlog files that needs to be replayed.
	filteredVlogIDs := []uint64{}
	for fid := range vlogFiles {
		if fid < uint64(vhead.Fid) {
			// Skip the vlog files that we don't need to replay.
			continue
		}
		filteredVlogIDs = append(filteredVlogIDs, fid)
	}

	return nil
}

func (manager *logManager) replayLog(walIDs, vlogIDs []uint64) {
	// Sort both the ids
	sort.Slice(walIDs, func(i, j int) bool { return walIDs[i] < walIDs[j] })
	sort.Slice(vlogIDs, func(i, j int) bool { return vlogIDs[i] < vlogIDs[j] })

}

type logReplayer struct {
	walIDs      []uint64
	vlogIDs     []uint64
	vhead       valuePointer
	opt         Options
	keyRegistry *KeyRegistry
}

func (lp *logReplayer) replay(replayFn logEntry) error {
	var flags uint32
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

	if vlogFile.fileOffset() < 20 {
		if err := vlogFile.bootstrap(); err != nil {
			return y.Wrapf(err, "Error while bootstraping vlog file %d", vlogFile.fid)
		}
	}
	vlogOffset := uint32(vlogHeaderSize)
	if vlogFile.fid == lp.vhead.Fid {
		vlogOffset = lp.vhead.Offset
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
	if walFile.fileOffset() < vlogHeaderSize {
		if err := walFile.bootstrap(); err != nil {
			return y.Wrapf(err, "Error while bootstraping wal file %d", walFile.fid)
		}
	}
	walIterator, err := newLogIterator(walFile, vlogHeaderSize)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for the wal file %s", walFile.path)
	}
	walEntries, walCommitTs, walErr := walIterator.iterateEntries()
	vlogEntries, vlogCommitTs, vlogErr := vlogIterator.iterateEntries()
	truncateNeeded := false
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
		if walErr == io.EOF {
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
				return y.Wrapf(err, "Error while closing the WAL file %d in replay", walFile.path)
			}
			// We successfully iterated till the end of the file. Now we have to advance
			// the wal File.
			if len(lp.walIDs) < currentWalIndex {
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
		if vlogErr == io.EOF {
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
				return y.Wrapf(err, "Error while closing the vlog file %d in replay", vlogFile.path)
			}
			// We successfully iterated till the end of the file. Now we have to advance
			// the wal File.
			if len(lp.vlogIDs) < currentVlogIndex {
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
			vlogEntries, vlogCommitTs, vlogErr = walIterator.iterateEntries()
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
		// Insert the entries back to LSM.
		for _, e := range walEntries {
			// Inserting empty value pointer since the value pointer are not going to lsm.
			if err := replayFn(*e, valuePointer{}); err != nil {
				return y.Wrapf(err, "Error while inserting entry to lsm.")
			}
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
		}
		// Advance for next batch of txn entries.
		walEntries, walCommitTs, walErr = walIterator.iterateEntries()
		vlogEntries, vlogCommitTs, vlogErr = walIterator.iterateEntries()
	}

	if truncateNeeded {
		panic("Sup implement this guy.")
	}
	return nil
}

type logIterator struct {
	entryReader *safeRead
	reader      *bufio.Reader
	validOffset uint32
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
				commitTs = 0
				entries = []*Entry{}
				return entries, 0, errTruncate
			}
			entries = append(entries, e)
			continue
		}
		// Here it is finish txn mark.
		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil || commitTs != txnTs {
				commitTs = 0
				entries = []*Entry{}
				return entries, 0, errTruncate
			}
			// We got finish mark for this entry batch. Now, the iteration for this entry batch
			// is done so stoping the iteration for this ts.
			iterator.validOffset = iterator.entryReader.recordOffset
			break
		}
	}
	return entries, commitTs, nil
}

func (manager *logManager) write(reqs []*request) error {
	vlogBuf := &bytes.Buffer{}
	walBuf := &bytes.Buffer{}
	// Process each request.
	for i := range reqs {
		var written int
		b := reqs[i]
		// Process this batch.
		// last two entries are end entries for vlog and WAL. so igoring that.
		txnEntries := b.Entries[0 : len(b.Entries)-2]
		for j := range txnEntries {
			e := b.Entries[j]
			if e.skipVlog {
				b.Ptrs = append(b.Ptrs, valuePointer{})
			}
			var p valuePointer
			var entryOffset uint32
			if manager.db.shouldWriteValueToLSM(*e) {
				// value size is less than threshold. So writing to WAL
				entryOffset = manager.wal.fileOffset() + uint32(walBuf.Len())
				_, err := manager.wal.encode(e, walBuf, entryOffset)
				if err != nil {
					return y.Wrapf(err, "Error while encoding entry for WAL %d", manager.wal.fid)
				}
				// This entry is going to persist in sst. So, appending empty val pointer.
				b.Ptrs = append(b.Ptrs, p)
				written++
				continue
			}
			// Since the value size is bigger, So we're writing to vlog.
			entryOffset = manager.vlog.fileOffset() + uint32(vlogBuf.Len())
			p.Offset = entryOffset
			entryLen, err := manager.vlog.encode(e, vlogBuf, entryOffset)
			if err != nil {
				return y.Wrapf(err, "Error while encoding entry for vlog %d", manager.vlog.fid)
			}
			p.Len = uint32(entryLen)
			b.Ptrs = append(b.Ptrs, p)
			written++
		}

		// Write the entry offset to the respective buf.
		// Write end entry to wal buf.
		entryOffset := manager.wal.fileOffset() + uint32(walBuf.Len())
		_, err := manager.wal.encode(txnEntries[len(txnEntries)-2], walBuf, entryOffset)
		if err != nil {
			return y.Wrapf(err, "Error while encoding end entry for WAL %d", manager.wal.fid)
		}
		written++
		// Write end entry to vlog buf.
		entryOffset = manager.vlog.fileOffset() + uint32(vlogBuf.Len())
		_, err = manager.vlog.encode(txnEntries[len(txnEntries)-1], vlogBuf, entryOffset)
		if err != nil {
			return y.Wrapf(err, "Error while encoding eng entry for vlog %d", manager.vlog.fid)
		}
		written++
		manager.entriesWritten += uint32(written)
	}
	// Persist the log to the disk.
	// TODO: make it concurrent. Golang, should give us async interface :(
	var err error
	if err = manager.wal.writeLog(walBuf); err != nil {
		return y.Wrapf(err, "Error while writing log to WAL %d", manager.wal.fid)
	}
	if err = manager.vlog.writeLog(vlogBuf); err != nil {
		return y.Wrapf(err, "Error while writing log to vlog %d", manager.vlog.fid)
	}
	return nil
}
