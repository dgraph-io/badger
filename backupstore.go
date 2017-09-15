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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/protos"
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
		return err
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
		return protos.BackupStatus{}, err
	}
	var ret protos.BackupStatus
	if err := ret.Unmarshal(data); err != nil {
		return protos.BackupStatus{}, err
	}
	return ret, nil
}

func backupFileName(id uint64) string {
	return fmt.Sprintf("backup-%d", id)
}

// RestoreBackup streams all changes in increasing key order to itemCh.  Does so in batches.  Omits
// changes with cas counter <= thresholdCasCounter.  Pass 0 for thresholdCASCounter to include all
// changes (because 1 is the minimum possible cas counter value).
func RestoreBackup(path string, thresholdCASCounter uint64, itemCh <-chan []protos.BackupItem) (err error) {
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

	// TODO: Implement.
	return nil
}

var (
	// ErrBackupKeysOutOfOrder means you tried storing a backup without supplying the keys in order.
	ErrBackupKeysOutOfOrder = errors.New("backup keys out of order")
	// ErrBackupAborted means the user aborted the backup.
	ErrBackupAborted = errors.New("backup aborted")
)

// BackupMessage holds a message passed to NewBackup.  'finished'
type BackupMessage struct {
	// if true, items is nil and we're done backing up.  If false, items must be non-nil (or the
	// channel will be deemed closed and backup will fail).
	finished bool
	items    []protos.BackupItem
}

// NewBackup stores a new backup onto an existing backup store.  itemCh is deemed finished when it
// produces an empty slice.  Keys must be distinct and arrive in increasing order.  (Each slice
// must be in order, and the last key of the current slice must be less than the first key of the
// next slice.)
func NewBackup(path string, maxCASCounter uint64, itemCh <-chan BackupMessage) (err error) {
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
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	// Just give it a generous buffer, even though file is not in sync mode.
	writer := bufio.NewWriterSize(f, 1<<20)

	prevKey := []byte{} // used to enforce key ordering
	first := true
	for msg := range itemCh {
		if msg.finished {
			break
		}
		if msg.items == nil {
			fileName := f.Name()
			_ = f.Close()
			f = nil
			_ = os.Remove(fileName)
			return ErrBackupAborted
		}
		for _, item := range msg.items {
			if bytes.Compare(prevKey, item.Key) >= 0 && !first {
				return ErrBackupKeysOutOfOrder
			}
			data, err := item.Marshal()
			if err != nil {
				return err
			}
			if _, err := writer.Write(data); err != nil {
				return err
			}
			first = false
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	newBackupID := maxBackupID + 1
	err = syncCloseRename(f, f.Name(), filepath.Join(path, backupFileName(newBackupID)))
	f = nil
	if err != nil {
		return err
	}
	// TODO: Sync dir?  Yadda yadda.

	status.Backups = append(status.Backups, &protos.BackupStatusItem{
		BackupID:      newBackupID,
		MaxCASCounter: maxCASCounter,
	})

	return writeBackupStatus(path, status)
}

func writeBackupStatus(path string, status protos.BackupStatus) (err error) {
	// TODO: dir file syncing?  Locking?
	data, err := status.Marshal()
	if err != nil {
		return err
	}
	f, err := ioutil.TempFile(path, "partial-manifest-")
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			// Error case hit already
			_ = f.Close()
		}
	}()
	writer := bufio.NewWriter(f)
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	err = syncCloseRename(f, f.Name(), filepath.Join(path, backupManifestFilename))
	f = nil
	return err
}

// Always closes the file
func syncCloseRename(f *os.File, oldPath string, newPath string) (err error) {
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	err = f.Close()
	f = nil
	if err != nil {
		return err
	}
	return os.Rename(oldPath, newPath)
}
