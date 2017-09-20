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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/protos"
	"github.com/pkg/errors"
)

/*
What does a "backup store" look like?

It consists of a directory with a BACKUPMANIFEST file (which we rewrite every time) and a bunch of
files "backup-1", "backup-2", etc. Every new incremental backup adds a new one of these, and
rewrites the BACKUPMANIFEST file with all the info about backup files.
*/

const (
	backupManifestFilename = "BACKUPMANIFEST"
)

var (
	// ErrBackupKeysOutOfOrder means you tried storing a backup without supplying the keys in order.
	ErrBackupKeysOutOfOrder = errors.New("backup keys out of order")
	// ErrBackupAborted means the user aborted the backup.
	ErrBackupAborted = errors.New("backup aborted")
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

func BackupFileName(id uint64) string {
	return fmt.Sprintf("backup-%06d", id)
}

// NewBackup stores a new backup onto an existing backup store.  producer is deemed finished when
// it produces an empty slice (or error).  Keys must be distinct and arrive in increasing order
// (across all slices).
func NewBackup(path string, counter uint64,
	producer func() ([]protos.BackupItem, error)) (err error) {
	status, err := ReadBackupStatus(path)
	if err != nil {
		return err
	}

	maxBackupID := uint64(0)
	for _, backup := range status.Backups {
		if backup.Counter > counter {
			return fmt.Errorf("backup %d exists with counter value %d (higher than %d)",
				backup.BackupID, backup.Counter, counter)
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

		var lenBuf [binary.MaxVarintLen64]byte
		for _, item := range items {
			if bytes.Compare(prevKey, item.Key) >= 0 && !first {
				return ErrBackupKeysOutOfOrder
			}
			prevKey = item.Key
			data, err := item.Marshal()
			if err != nil {
				return errors.Wrap(err, "NewBackup marshaling")
			}
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
	err = syncCloseRename(f, f.Name(), path, BackupFileName(newBackupID))
	f = nil
	if err != nil {
		return err
	}

	status.Backups = append(status.Backups, &protos.BackupStatusItem{
		BackupID: newBackupID,
		Counter:  counter,
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
