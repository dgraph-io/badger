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
	manager     *logManager
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

	var truncateNeeded bool

	walIterator, err := newLogIterator(&logIteratorOptions{
		keyRegistry: lp.keyRegistry,
		logIds:      lp.walIDs,
		dbOpt:       lp.opt,
		logtype:     WAL,
		head:        lp.whead,
	})
	if err != nil {
		return err
	}
	vlogIterator, err := newLogIterator(&logIteratorOptions{
		keyRegistry: lp.keyRegistry,
		logIds:      lp.vlogIDs,
		dbOpt:       lp.opt,
		logtype:     VLOG,
		head:        lp.vhead,
	})
	if err != nil {
		return err
	}
	walEntries, walCommitTs, walErr := walIterator.iterateEntries()
	vlogEntries, vlogCommitTs, vlogErr := vlogIterator.iterateEntries()
	for {
		if walErr == errTruncate || vlogErr == errTruncate || truncateNeeded {
			// We could have errTruncate from iterator.But, truncateNeeded is still false.
			// So, it is important to set
			truncateNeeded = true
			break
		}
		// If any of the log reaches EOF we need to advance both the log file because vlog and wal
		// has 1 to 1 mapping isTruncateNeeded check will take care of truncation.
		if walErr == io.ErrUnexpectedEOF || walErr == io.EOF || vlogErr == io.ErrUnexpectedEOF ||
			vlogErr == io.EOF {
			var err error
			// check whether we iterated till the valid offset before advancing next log file.
			if truncateNeeded, err = walIterator.isTruncateNeeded(); err != nil {
				return y.Wrapf(err, "Error while checking truncation for the wal file %s",
					walIterator.currentLogFile.path)
			}
			if truncateNeeded {
				break
			}
			// befor advance vlog we need to check truncation in vlog as well.
			if truncateNeeded, err = vlogIterator.isTruncateNeeded(); err != nil {
				return y.Wrapf(err, "Error while checking truncation for the wal file %s",
					vlogIterator.currentLogFile.path)
			}
			if truncateNeeded {
				break
			}
			// close the log file of wal and vlog.
			if err = walIterator.closeLogFile(); err != nil {
				return y.Wrapf(
					err,
					"Error while closing the WAL file %s in replay", walIterator.currentLogFile.path)
			}
			if err = vlogIterator.closeLogFile(); err != nil {
				return y.Wrapf(
					err,
					"Error while closing the WAL file %s in replay", vlogIterator.currentLogFile.path)
			}

			// check whether we can advance both the iterator. When rotating log file there may
			// be a case only wal file created and not vlog file. Because, badger may crash after
			// wal file creation.
			if !walIterator.canAdvance() {
				// No more log files to replay so breaking it.
				break
			}
			// advance wal log file.
			if err = walIterator.advanceLogFile(); err != nil {
				return err
			}
			if !vlogIterator.canAdvance() {
				// There is no one to one mapping for the current wal file. so we need truncation
				// here
				truncateNeeded = true
				break
			}
			// advance vlog log file.
			if err = walIterator.advanceLogFile(); err != nil {
				return err
			}
			walEntries, walCommitTs, walErr = walIterator.iterateEntries()
			vlogEntries, vlogCommitTs, vlogErr = vlogIterator.iterateEntries()
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
				Fid:    vlogIterator.currentLogFile.fid,
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
		if err = lp.handleTruncation(&truncationOptions{
			walCommitTs, vlogCommitTs, walIterator, vlogIterator}); err != nil {
			return y.Wrapf(err, "Error while handling truncation in replayer")
		}
		return nil
	}
	return nil
}

type truncationOptions struct {
	walCommitTs  uint64
	vlogCommitTs uint64
	walIterator  *logIterator
	vlogIterator *logIterator
}

func (lp *logReplayer) handleTruncation(opt *truncationOptions) error {
	// Here not handling any corruption in the middle. It is expected that all the log file before
	// are in good state. In previous implementation, the log files are deleted if truncation
	// enabled. we can do the same if necessary.

	// we can truncate only last file.
	if len(opt.walIterator.logIds)-1 != opt.walIterator.currentLogIndex ||
		len(opt.vlogIterator.logIds)-1 != opt.vlogIterator.currentLogIndex {
		return errors.New(`Looks like log files are corrupted in the middle, so badger can't 
			replay the logs`)
	}

	// Check whether do we have one to one mapping between log files.
	if len(opt.walIterator.logIds) != len(opt.vlogIterator.logIds) {

		y.AssertTrue(len(opt.walIterator.logIds)-1 == len(opt.vlogIterator.logIds))

		valid, err := opt.walIterator.currentLogFile.validLogFile()
		if err != nil {
			return y.Wrapf(err, "Error while checking validLogFile in replay")
		}
		if !valid {
			if err = opt.walIterator.currentLogFile.bootstrap(); err != nil {
				return y.Wrapf(
					err, "Error while bootstrapping log file %s", opt.walIterator.currentLogFile.path)
			}
		}
		// create corresponding vlog file for wal file.
		lf := &logFile{
			fid:         opt.walIterator.currentLogFile.fid,
			path:        vlogFilePath(lp.opt.ValueDir, opt.walIterator.currentLogFile.fid),
			loadingMode: lp.opt.ValueLogLoadingMode,
			registry:    lp.keyRegistry,
		}
		if lf.fd, err = y.CreateSyncedFile(lf.path, lp.opt.SyncWrites); err != nil {
			return errFile(err, lf.path, "Create value log file")
		}
		if err = lf.bootstrap(); err != nil {
			return err
		}

		// Close both the log files
		if err = opt.walIterator.closeLogFile(); err != nil {
			return y.Wrapf(err, "Error while closing wal file in replayer")
		}
		// closing vlog file
		if err = lf.fd.Close(); err != nil {
			return y.Wrapf(err, "Error while closing vlog file %s in replayer", lf.path)
		}
		return nil
	}
	validWAL, err := opt.walIterator.currentLogFile.validLogFile()
	if err != nil {
		return y.Wrapf(
			err,
			"Error while checking validLogFile in replay for the log file: %s",
			opt.walIterator.currentLogFile.path)
	}
	validVLOG, err := opt.vlogIterator.currentLogFile.validLogFile()
	if err != nil {
		return y.Wrapf(
			err,
			"Error while checking validLogFile in replay for the log file: %s",
			opt.vlogIterator.currentLogFile.path)
	}
	if !validWAL || !validVLOG {
		// If any of the log file is entirely corrupted, we need to bootstrap both the log file.
		if err = opt.walIterator.currentLogFile.bootstrap(); err != nil {
			return y.Wrapf(
				err, "Error while bootstrapping log file %s", opt.walIterator.currentLogFile.path)
		}
		if err = opt.vlogIterator.currentLogFile.bootstrap(); err != nil {
			return y.Wrapf(
				err, "Error while bootstrapping log file %s", opt.vlogIterator.currentLogFile.path)
		}
	} else {
		// Now we have to find which is right offset that need to be truncate for both wal and vlog.
		offset := uint32(0)
		// if ts is zero, then that file is corrupted.
		if opt.walCommitTs == 0 {
			// wal file is corrupted so the offset is the valid offset.
			offset = opt.walIterator.currentTxnOffset
		} else {
			// wal is not corrupted. so the batch for the current transaction is corrupted in val.
			// so truncating to the last batch offset.
			offset = opt.walIterator.previousTxnOffset
		}
		// None of the log files are mmaped so far, so it is good to truncate here.
		if err = opt.walIterator.currentLogFile.fd.Truncate(int64(offset)); err != nil {
			return y.Wrapf(
				err, "Error while truncating wal file %d", opt.walIterator.currentLogFile.fid)
		}
		if opt.vlogCommitTs == 0 {
			// vlog file is corrupted so the offset is the valid offset.
			offset = opt.vlogIterator.currentTxnOffset
		} else {
			// vlog is not corrupted. so the batch for the current transaction is corrupted in wal.
			// so truncating to the last batch offset.
			offset = opt.vlogIterator.previousTxnOffset
		}
		// we'll calculate the offset, by using the same mechanism which we used for wal
		if err = opt.walIterator.currentLogFile.fd.Truncate(int64(offset)); err != nil {
			return y.Wrapf(
				err, "Error while truncating vlog file %d", opt.walIterator.currentLogFile.fid)
		}
	}
	// Close both the log files
	if err = opt.walIterator.closeLogFile(); err != nil {
		return y.Wrapf(err, "Error while closing wal file in replayer")
	}
	// closing vlog file
	if err = opt.vlogIterator.closeLogFile(); err != nil {
		return y.Wrapf(err, "Error while closing vlog file in replayer")
	}
	return nil
}

// logIterator is used to iterate batch of transaction entries in the log file.
// It is used for replay.
type logIterator struct {
	entryReader       *safeRead
	logReader         *bufio.Reader
	currentTxnOffset  uint32
	previousTxnOffset uint32
	logtype           logType
	logIds            []uint32
	opt               Options
	currentLogIndex   int
	keyRegistry       *KeyRegistry
	currentLogFile    *logFile
}

type logIteratorOptions struct {
	keyRegistry *KeyRegistry
	logIds      []uint32
	dbOpt       Options
	logtype     logType
	head        valuePointer
}

// newLogIterator will return the log iterator.
func newLogIterator(opt *logIteratorOptions) (*logIterator, error) {
	itr := &logIterator{
		logIds:      opt.logIds,
		keyRegistry: opt.keyRegistry,
		logtype:     opt.logtype,
		opt:         opt.dbOpt,
	}
	// Calcuate start offset to replay.
	y.AssertTrue(opt.logIds[0] == opt.head.Fid)
	offset := opt.head.Offset + opt.head.Len
	// Open first log file.
	lf, err := itr.openCurrentLogFile()
	if err != nil {
		return nil, err
	}
	stat, err := lf.fd.Stat()
	if err != nil {
		return nil, err
	}
	if itr.opt.ReadOnly {
		if int64(offset) != stat.Size() {
			// We're not at the end of the file. We'd need to replay the entries, or
			// possibly truncate the file.
			return nil, ErrReplayNeeded
		}
	}
	_, err = lf.fd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &logIterator{
		entryReader: &safeRead{
			k:            make([]byte, 10),
			v:            make([]byte, 10),
			recordOffset: offset,
			decrypter: &logDecrypter{
				baseIV:  lf.baseIV,
				dataKey: lf.dataKey,
			},
		},
		previousTxnOffset: offset,
		logReader:         bufio.NewReader(lf.fd),
		currentTxnOffset:  offset,
		currentLogFile:    lf,
	}, nil
}

func (itr *logIterator) openCurrentLogFile() (*logFile, error) {
	y.AssertTrue(itr.currentLogIndex < len(itr.logIds))
	lFile := &logFile{
		loadingMode: itr.opt.ValueLogLoadingMode,
		registry:    itr.keyRegistry,
	}
	lFile.fid = uint32(itr.logIds[itr.currentLogIndex])
	switch itr.logtype {
	case VLOG:
		lFile.path = vlogFilePath(itr.opt.ValueDir, lFile.fid)
	case WAL:
		lFile.path = walFilePath(itr.opt.ValueDir, lFile.fid)
	}
	var flags uint32
	switch {
	case itr.opt.ReadOnly:
		// If we have read only, we don't need SyncWrites.
		flags |= y.ReadOnly
		// Set sync flag.
	case itr.opt.SyncWrites:
		flags |= y.Sync
	}
	if err := lFile.open(lFile.path, flags); err != nil {
		return nil, y.Wrapf(
			err,
			"Error while opening log file of type: %d and path: %s in logReplayer",
			itr.logtype, lFile.path)
	}
	fi, err := lFile.fd.Stat()
	if err != nil {
		return nil, errFile(err, lFile.path, "Unable to run file.Stat")
	}
	if fi.Size() < vlogHeaderSize {
		return nil, errTruncate
	}
	return lFile, nil
}

func (itr *logIterator) Entry() (*Entry, error) {
	return itr.entryReader.Entry(itr.logReader)
}

func (itr *logIterator) isTruncateNeeded() (bool, error) {
	info, err := itr.currentLogFile.fd.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() != int64(itr.currentTxnOffset), nil
}

func (itr *logIterator) canAdvance() bool {
	return itr.currentLogIndex < len(itr.logIds)
}

func (itr *logIterator) closeLogFile() error {
	if err := itr.currentLogFile.fd.Close(); err != nil {
		return y.Wrapf(
			err, "Error while closing log file %s in log iterator", itr.currentLogFile.path)
	}
	return nil
}

func (itr *logIterator) advanceLogFile() error {
	// close the current log file
	if err := itr.closeLogFile(); err != nil {
		return err
	}
	// advance the log log Index.
	itr.currentLogIndex++
	lf, err := itr.openCurrentLogFile()
	if err != nil {
		return err
	}
	// rebuild the state for next log file.
	itr.currentLogFile = lf
	itr.logReader = bufio.NewReader(lf.fd)
	itr.entryReader = &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: vlogHeaderSize,
		decrypter: &logDecrypter{
			baseIV:  lf.baseIV,
			dataKey: lf.dataKey,
		},
	}
	itr.previousTxnOffset = vlogHeaderSize
	itr.previousTxnOffset = vlogHeaderSize
	return nil
}

// iterateEntries will iterate entries batch by batch. The batch end of transaction is
// determined by finish txn mark.
func (itr *logIterator) iterateEntries() ([]*Entry, uint64, error) {
	var commitTs uint64
	var entries []*Entry
	for {
		e, err := itr.Entry()
		if err != nil {
			return nil, 0, err
		}
		// advance the reader offset
		entryLen := uint32(int(e.hlen) + len(e.Key) + len(e.Value) + crc32.Size)
		itr.entryReader.recordOffset += entryLen

		// This is txn entries.
		if e.meta&bitTxn > 0 {
			txnTs := y.ParseTs(e.Key)
			if commitTs == 0 {
				commitTs = txnTs
			}
			if commitTs != txnTs {
				// We got an entry that has txn timestamp different than what we were expecting.
				// This wouldn't happen unless the data is lost/corrupted.
				return nil, 0, errTruncate
			}
			entries = append(entries, e)
			continue
		}
		// Here it is finish txn mark.
		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				return nil, 0, err
			}
			if commitTs == 0 && txnTs == math.MaxUint64 {
				// we got finish mark for gc. so no need to check commitTs != txnTs
				itr.currentTxnOffset = itr.entryReader.recordOffset
				commitTs = math.MaxUint64
				break
			}
			// If there is no entries means no entries from the current txn is not part
			// of log files. we only got finish mark. so we're not checking commitTs != txnTs
			if len(entries) != 0 && commitTs != txnTs {
				return nil, 0, errTruncate
			}
			y.AssertTrue(commitTs == txnTs)
			// We got finish mark for this entry batch. Now, the iteration for this entry batch
			// is done so stoping the iteration for this ts.
			commitTs = txnTs
			itr.previousTxnOffset = itr.currentTxnOffset
			itr.currentTxnOffset = itr.entryReader.recordOffset
			break
		}

		// This entries are from gc. so appending to the entries as it is.
		entries = append(entries, e)
	}
	return entries, commitTs, nil
}
