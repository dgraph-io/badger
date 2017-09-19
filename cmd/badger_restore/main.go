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

/*
badger_restore

Restores a Badger backup.
*/
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/protos"
)

func main() {
	backupDirFlag := flag.String("backup_repo", "", "The backup repository")
	dirFlag := flag.String("dir", "", "The directory to restore the Badger DB at")
	valueDirFlag := flag.String("value-dir", "",
		"The restored Badger database's value log directory, if different from the index directory")

	flag.Parse()

	backupDir := *backupDirFlag
	restoreDir := *dirFlag
	restoreValueDir := *valueDirFlag

	if backupDir == "" || restoreDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	if restoreValueDir == "" {
		restoreValueDir = restoreDir

		fmt.Printf("Restoring from backup at '%s' into Badger repo '%s'...\n",
			backupDir, restoreDir)
	} else {
		fmt.Printf("Restoring from backup at '%s' into Badger repo '%s', value directory '%s'...\n",
			backupDir, restoreDir, restoreValueDir)
	}

	cancel := make(chan struct{})
	itemCh := make(chan []protos.BackupItem, 100)

	errCanceled := errors.New("canceled")

	errCh1 := make(chan error)
	go func() {
		opts := badger.DefaultOptions
		opts.Dir = restoreDir
		opts.ValueDir = restoreValueDir
		opts.SyncWrites = false
		errCh1 <- badger.BuildKVFromBackup(&opts, func() ([]protos.BackupItem, error) {
			select {
			case <-cancel:
				return nil, errCanceled
			case items := <-itemCh:
				return items, nil
			}
		})
	}()

	errCh2 := make(chan error)
	go func() {
		errCh2 <- badger.RetrieveBackup(backupDir, func(item protos.BackupItem) error {
			select {
			case <-cancel:
				return errCanceled
			case itemCh <- []protos.BackupItem{item}:
				return nil
			}
		})
	}()

	var err error
	select {
	case err1 := <-errCh1:
		if err1 != nil {
			err = err1
			close(cancel)
			<-errCh2
		} else {
			err = <-errCh2
		}
	case err2 := <-errCh2:
		if err2 != nil {
			err = err2
			close(cancel)
			<-errCh1
		} else {
			err = <-errCh1
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Backup complete.")
	return
}
