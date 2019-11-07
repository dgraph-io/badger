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
	"hash/crc32"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/pkg/errors"
)

// logReplayer is used to replay all the log.
type logReplayer struct {
	walIDs      []uint32
	vlogIDs     []uint32
	vhead       valuePointer
	opt         Options
	keyRegistry *KeyRegistry
	whead       valuePointer
}

func (lp *logReplayer) openLogFile(ltype logType, index int) (*logFile, error) {
	lFile := &logFile{
		loadingMode: lp.opt.ValueLogLoadingMode,
		registry:    lp.keyRegistry,
	}
	switch ltype {
	case VLOG:
		lFile.fid = uint32(lp.vlogIDs[index])
		lFile.path = vlogFilePath(lp.opt.ValueDir, uint32(lp.walIDs[index]))
	case WAL:
		lFile.fid = uint32(lp.walIDs[index])
		lFile.path = walFilePath(lp.opt.ValueDir, uint32(lp.walIDs[index]))
	}
	var flags uint32
	switch {
	case lp.opt.ReadOnly:
		// If we have read only, we don't need SyncWrites.
		flags |= y.ReadOnly
		// Set sync flag.
	case lp.opt.SyncWrites:
		flags |= y.Sync
	}
	if err := lFile.open(lFile.path, flags); err != nil {
		return nil, y.Wrapf(err, "Error while opening log file of type: %d and ID: %d in logReplayer",
			ltype, lp.walIDs[index])
	}
	return lFile, nil
}

// replay will take replayFn as input and replayed will replay all the entries to the
// memtable.
func (lp *logReplayer) replay(replayFn logEntry) error {
	// No need to replay if all the SST's are flushed properly.
	if len(lp.walIDs) == 0 {
		y.AssertTrue(len(lp.vlogIDs) == 0)
		return nil
	}

	var (
		vlogFile, walFile *logFile
		truncateNeeded    bool
		err               error
		currentVlogIndex  int
		currentWalIndex   int
	)
	if vlogFile, err = lp.openLogFile(VLOG, currentVlogIndex); err != nil {
		return err
	}
	vlogOffset := uint32(vlogHeaderSize)
	// At this point, the first file to be replayed should be the one that contains the vlog head.
	y.AssertTruef(vlogFile.fid == lp.vhead.Fid,
		"vlog.fid: %d vhead.fid: %d", vlogFile.fid, lp.vhead.Fid)
	if vlogFile.fileOffset() < vlogOffset {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.vlogIDs) == 1)
		truncateNeeded = true
	}
	// Move vlogOffset to the end of vhead entry.
	vlogOffset = lp.vhead.Offset + lp.vhead.Len
	vlogIterator, err := newLogIterator(vlogFile, vlogOffset, lp.opt)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for the vlog file %s", vlogFile.path)
	}
	if walFile, err = lp.openLogFile(WAL, currentWalIndex); err != nil {
		return err
	}
	// At this point, the first file to be replayed should be the one that contains the WAL head.
	y.AssertTruef(walFile.fid == lp.whead.Fid,
		"wal.fid: %d whead.fid: %d", walFile.fid, lp.whead.Fid)
	if walFile.fileOffset() < uint32(vlogHeaderSize) {
		// we only bootstarp last log file and there is no log file to replay.
		y.AssertTrue(len(lp.walIDs) == 1)
		truncateNeeded = true
	}
	// Move walOffset to the end of whead entry.
	walOffset := lp.whead.Offset + lp.whead.Len
	walIterator, err := newLogIterator(walFile, walOffset, lp.opt)
	if err != nil {
		return y.Wrapf(err, "Error while creating log iterator for the wal file %s", walFile.path)
	}

	isTruncateNeeded := func(validOffset uint32, log *logFile) (bool, error) {
		info, err := log.fd.Stat()
		if err != nil {
			return false, err
		}
		return info.Size() != int64(validOffset), nil
	}
	walEntries, walCommitTs, walErr := walIterator.iterateEntries()
	vlogEntries, vlogCommitTs, vlogErr := vlogIterator.iterateEntries()
	for {
		if walErr == errTruncate || vlogErr == errTruncate || truncateNeeded {
			truncateNeeded = true
			break
		}

		// If any of the log reaches EOF we need to advance both the log file because vlog and wal has 1 to 1 mapping
		// isTruncateNeeded check will take care of truncation.
		if walErr == io.ErrUnexpectedEOF || walErr == io.EOF || vlogErr == io.ErrUnexpectedEOF || vlogErr == io.EOF {
			// check whether we iterated till the valid offset.
			if truncateNeeded, err = isTruncateNeeded(walIterator.validOffset, walFile); err != nil {
				return y.Wrapf(err, "Error while checking truncation for the wal file %s",
					walFile.path)
			}
			// close the log file.
			if err = walFile.fd.Close(); err != nil {
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
			if walFile, err = lp.openLogFile(WAL, currentWalIndex); err != nil {
				return err
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
			if err = vlogFile.fd.Close(); err != nil {
				return y.Wrapf(err, "Error while closing the vlog file %s in replay", vlogFile.path)
			}

			if vlogFile, err = lp.openLogFile(VLOG, currentVlogIndex); err != nil {
				return err
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
		if walFile, err = lp.openLogFile(WAL, currentWalIndex); err != nil {
			return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
				lp.walIDs[currentWalIndex])
		}
		if vlogFile, err = lp.openLogFile(VLOG, currentVlogIndex); err != nil {
			return y.Wrapf(err, "Error while opening WAL file %d in logReplayer",
				lp.walIDs[currentVlogIndex])
		}

		var walStat, vlogStat os.FileInfo
		if walStat, err = walFile.fd.Stat(); err != nil {
			return y.Wrapf(err, "Error while retriving wal file %d stat", walFile.fid)
		}
		if vlogStat, err = vlogFile.fd.Stat(); err != nil {
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
