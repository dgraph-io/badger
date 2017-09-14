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
	"encoding/binary"

	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/y"
)

const (
	backupMagicText    string = "BACKBADG"
	backupMagicVersion uint64 = 1
)

// CreateBackupStore creates a new backup store in the given file.
func CreateBackupStore(path string) (err error) {
	f, err := y.CreateSyncedFile(path, false)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	// TODO: Flock file?  Flock everywhere?
	buf := []byte{}
	buf = append(buf, backupMagicText...)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], backupMagicVersion)
	buf = append(buf, tmp[:]...)

	if _, err := f.Write(buf); err != nil {
		return err
	}

	return nil
}

// NewBackup stores a new backup onto an existing backup store.  itemCh is finished when it returns
// an empty slice.
func NewBackup(path string, itemCh <-chan []protos.BackupItem) error {
	f, err := y.OpenExistingSyncedFile(path, false)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	return nil
}
