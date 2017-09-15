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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/dgraph-io/badger/protos"
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
	tmpName := f.Name()
	if f != nil {

	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	f = nil
	if err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(path, tmpName),
		filepath.Join(path, backupManifestFilename)); err != nil {
		return err
	}
	return nil
}

var backupFilenameRegex = regexp.MustCompile("^backup-([1-9][0-9])+-([1-9][0-9]+)$")

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

// NewBackup stores a new backup onto an existing backup store.  itemCh is finished when it returns
// an empty slice.
func NewBackup(path string, maxCASCounter uint64, itemCh <-chan []protos.BackupItem) (err error) {
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
	tmpName := f.Name()
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	for items := range itemCh {
		if items == nil {
			break
		}
		for _, item := range items {
			data, err := item.Marshal()
			if err != nil {
				return err
			}
			if _, err := f.Write(data); err != nil {
				return err
			}
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	f = nil
	if err != nil {
		return err
	}

	newBackupID := maxBackupID + 1
	if err := os.Rename(filepath.Join(path, tmpName),
		filepath.Join(path, backupFileName(newBackupID))); err != nil {
		return err
	}

	// TODO: Sync dir?  Yadda yadda.

	status.Backups = append(status.Backups, &protos.BackupStatusItem{
		BackupID:      newBackupID,
		MaxCASCounter: maxCASCounter,
	})

	return writeBackupStatus(path, status)
}
