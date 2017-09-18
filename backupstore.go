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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

const (
	backupMagicText        string = "BACKBADG"
	backupMagicVersion     uint64 = 1
	backupManifestFilename        = "BACKUPMANIFEST"
)

// CreateBackupStore creates a directory and initializes a new backup store in that directory.
func CreateBackupStore(path string) (err error) {
	if err := os.Mkdir(path, 0755); err != nil {
		return errors.Wrap(err, "Mkdir in CreateBackupStore")
	}
	var initialStatus = protos.BackupStatus{
		Backups: []*protos.BackupStatusItem{},
	}
	return writeBackupStatus(path, initialStatus)
}

// ReadBackupStatus reads the BackupStatus description from the backup directory.
func ReadBackupStatus(path string) (protos.BackupStatus, error) {
	data, err := ioutil.ReadFile(filepath.Join(path, backupManifestFilename))
	if err != nil {
		return protos.BackupStatus{}, errors.Wrap(err, "cannot ReadFile in ReadBackupStatus")
	}
	var ret protos.BackupStatus
	if err := ret.Unmarshal(data); err != nil {
		return protos.BackupStatus{}, errors.Wrap(err, "unmarshal in ReadBackupStatus")
	}
	return ret, nil
}

func backupFileName(id uint64) string {
	return fmt.Sprintf("backup-%d", id)
}

// RestoreBackup streams all changes in increasing key order to itemCh.  Does so in batches.  Omits
// changes with cas counter <= thresholdCasCounter.  Pass 0 for thresholdCASCounter to include all
// changes (because 1 is the minimum possible cas counter value).
func RestoreBackup(path string, thresholdCASCounter uint64, consumer func(protos.BackupItem) error) (err error) {
	// TODO: If we remove thresholdCASCounter, we could filter away deletes.
	status, err := ReadBackupStatus(path)
	if err != nil {
		return err
	}

	ids := []uint64{}
	for _, backup := range status.Backups {
		if backup.MaxCASCounter <= thresholdCASCounter {
			continue
		}
		ids = append(ids, backup.BackupID)
	}

	// Sort by descending ID (because MergeIterator expects order by increasing age)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	const memUsage = 2 << 30
	bufferSize := memUsage / len(ids)

	fileIters := []y.Iterator{}
	defer func() {
		for _, iter := range fileIters {
			_ = iter.Close()
		}
	}()

	for _, id := range ids {
		iter, err := makeBackupFileIterator(path, id, bufferSize)
		if err != nil {
			return err
		}
		fileIters = append(fileIters, iter)
	}

	// In MergeIterator, keys returned by the iterators closer to the beginning of the array take
	// precedence.
	mergeIter := y.NewMergeIterator(fileIters, false)
	fileIters = nil
	defer func() {
		if closeErr := mergeIter.Close(); err == nil {
			err = closeErr
		}
	}()

	for mergeIter.Rewind(); mergeIter.Valid(); mergeIter.Next() {
		key := mergeIter.Key()
		value := mergeIter.Value()
		err := consumer(protos.BackupItem{
			Key:        key,
			CASCounter: value.CASCounter,
			HasValue:   value.Meta == 0, // or value.Meta != BitDelete
			UserMeta:   uint32(value.UserMeta),
			Value:      value.Value,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type backupFileIterator struct {
	fp         *os.File
	reader     *bufio.Reader
	firstKey   []byte
	firstValue y.ValueStruct
	err        error
}

func makeBackupFileIterator(path string, id uint64, bufferSize int) (*backupFileIterator, error) {
	fp, err := os.Open(filepath.Join(path, backupFileName(id)))
	if err != nil {
		return nil, err
	}
	return &backupFileIterator{fp: fp}, nil
}

func (bit *backupFileIterator) Rewind() {
	if bit.err != nil {
		bit.err = nil
	}
	_, err := bit.fp.Seek(0, os.SEEK_SET)
	if err != nil {
		bit.err = err
		bit.firstKey = nil
		bit.firstValue = y.ValueStruct{}
		return
	}
	bit.reader = bufio.NewReader(bit.fp)
	bit.Next()
}

func (bit *backupFileIterator) Key() []byte {
	return bit.firstKey
}

func (bit *backupFileIterator) Value() y.ValueStruct {
	return bit.firstValue
}

func (bit *backupFileIterator) Next() {
	if bit.err != nil {
		return
	}
	len, err := binary.ReadUvarint(bit.reader)
	if err != nil {
		bit.err = err
		bit.firstKey = nil
		bit.firstValue = y.ValueStruct{}
		return
	}

	data := make([]byte, len)
	if _, err := io.ReadFull(bit.reader, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		bit.err = err
		bit.firstKey = nil
		bit.firstValue = y.ValueStruct{}
		return
	}

	var item protos.BackupItem
	if err := item.Unmarshal(data); err != nil {
		bit.err = err
		bit.firstKey = nil
		bit.firstValue = y.ValueStruct{}
		return
	}
	bit.firstKey = item.Key
	var meta byte
	if !item.HasValue {
		meta = BitDelete
	}
	bit.firstValue = y.ValueStruct{
		// This is typically a vptr but in our case it's a value -- we're just using MergeIterator.
		Value:      item.Value,
		Meta:       meta,
		UserMeta:   uint8(item.UserMeta),
		CASCounter: item.CASCounter,
	}
}

func (bit *backupFileIterator) Valid() bool {
	return bit.firstKey != nil
}

func (bit *backupFileIterator) Close() error {
	err := bit.err
	if err == io.EOF {
		err = nil
	}
	if closeErr := bit.fp.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (bit *backupFileIterator) Seek([]byte) {
	panic("Seek not implemented")
}

var (
	// ErrBackupKeysOutOfOrder means you tried storing a backup without supplying the keys in order.
	ErrBackupKeysOutOfOrder = errors.New("backup keys out of order")
	// ErrBackupAborted means the user aborted the backup.
	ErrBackupAborted = errors.New("backup aborted")
)

// NewBackup stores a new backup onto an existing backup store.  producer is deemed finished when
// it produces an empty slice (or error).  Keys must be distinct and arrive in increasing order
// (across all slices).
func NewBackup(path string, maxCASCounter uint64, producer func() ([]protos.BackupItem, error)) (err error) {
	// TODO: Some sort of flocking to prevent multiple people from trying to add a backup
	// simultaneously?
	status, err := ReadBackupStatus(path)
	if err != nil {
		return err
	}

	maxBackupID := uint64(0)
	for _, backup := range status.Backups {
		if backup.MaxCASCounter > maxCASCounter {
			return fmt.Errorf("backup %d exists with counter value %d (higher than %d)",
				backup.BackupID, backup.MaxCASCounter, maxCASCounter)
		}
		if maxBackupID < backup.BackupID {
			// I suppose this conditional is always hit because the backups are in sorted order.
			maxBackupID = backup.BackupID
		}
	}

	f, err := ioutil.TempFile(path, "partial-backup-")
	if err != nil {
		return errors.Wrap(err, "making partial-backup temp file")
	}
	defer func() {
		if f != nil {
			if closeErr := f.Close(); err == nil {
				err = errors.Wrap(closeErr, "closing file")
			}
		}
	}()

	// Just give it a generous buffer, even though file is not in sync mode.
	writer := bufio.NewWriterSize(f, 1<<20)

	prevKey := []byte{} // used to enforce key ordering
	first := true
	for {
		items, err := producer()
		if err != nil {
			fileName := f.Name()
			_ = f.Close()
			f = nil
			_ = os.Remove(fileName)
			return err
		}
		if items == nil {
			break
		}

		for _, item := range items {
			if bytes.Compare(prevKey, item.Key) >= 0 && !first {
				return ErrBackupKeysOutOfOrder
			}
			data, err := item.Marshal()
			if err != nil {
				return errors.Wrap(err, "NewBackup marshaling")
			}
			var lenBuf [binary.MaxVarintLen64]byte
			lenLen := binary.PutUvarint(lenBuf[:], uint64(len(data)))
			if _, err := writer.Write(lenBuf[:lenLen]); err != nil {
				return errors.Wrap(err, "NewBackup writing")
			}
			if _, err := writer.Write(data); err != nil {
				return errors.Wrap(err, "NewBackup writing")
			}
			first = false
		}
	}

	if err := writer.Flush(); err != nil {
		return errors.Wrap(err, "NewBackup flushing")
	}

	newBackupID := maxBackupID + 1
	err = syncCloseRename(f, f.Name(), path, backupFileName(newBackupID))
	f = nil
	if err != nil {
		return err
	}

	status.Backups = append(status.Backups, &protos.BackupStatusItem{
		BackupID:      newBackupID,
		MaxCASCounter: maxCASCounter,
	})

	return writeBackupStatus(path, status)
}

func writeBackupStatus(path string, status protos.BackupStatus) (err error) {
	data, err := status.Marshal()
	if err != nil {
		return errors.Wrap(err, "writeBackupStatus marshaling")
	}
	f, err := ioutil.TempFile(path, "partial-manifest-")
	if err != nil {
		return errors.Wrap(err, "writeBackupStatus opening TempFile")
	}
	defer func() {
		if f != nil {
			// Error case hit already
			_ = f.Close()
		}
	}()
	writer := bufio.NewWriter(f)
	if _, err := writer.Write(data); err != nil {
		return errors.Wrap(err, "writeBackupStatus writing")
	}
	if err := writer.Flush(); err != nil {
		return errors.Wrap(err, "writeBackupStatus flushing")
	}
	err = syncCloseRename(f, f.Name(), path, backupManifestFilename)
	f = nil
	return err
}

// Always closes the file
func syncCloseRename(f *os.File, oldPath, newDir, newFilename string) (err error) {
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.Wrap(err, "cannot sync file")
	}
	err = f.Close()
	f = nil
	if err != nil {
		return errors.Wrap(err, "cannot close file")
	}
	err = os.Rename(oldPath, filepath.Join(newDir, newFilename))
	if err != nil {
		return errors.Wrap(err, "renaming file")
	}
	err = syncDir(newDir)
	if err != nil {
		return errors.Wrap(err, "syncing dir")
	}
	return nil
}
