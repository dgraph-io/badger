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

package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

var backupFile, prefix string
var dump, truncate bool

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup Badger database.",
	Long: `Backup Badger database to a file in a version-agnostic manner.

Iterates over each key-value pair, encodes it along with its metadata and
version in protocol buffers and writes them to a file. This file can later be
used by the restore command to create an identical copy of the
database.`,
	RunE: handle,
}

func init() {
	RootCmd.AddCommand(backupCmd)
	backupCmd.Flags().StringVarP(&backupFile, "backup-file", "f",
		"badger.bak", "File to backup to")
	backupCmd.Flags().StringVarP(&prefix, "hex-prefix", "p",
		"", "Only choose keys with this HEX prefix.")
	backupCmd.Flags().BoolVarP(&dump, "dump", "d",
		false, "Dump to stdout.")
	backupCmd.Flags().BoolVarP(&truncate, "truncate", "t",
		false, "Allow value log truncation if required.")
}

func handle(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.Truncate = truncate
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if dump {
		return writeToStdout(db)
	}
	return doBackup(db)
}

func writeToStdout(db *badger.DB) error {
	var kp []byte
	if len(prefix) > 0 {
		var err error
		kp, err = hex.DecodeString(prefix)
		if err != nil {
			return err
		}
		fmt.Printf("Only choosing keys with prefix: %X\n", kp)
	}

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		var lastKey []byte
		for it.Seek(kp); it.ValidForPrefix(kp); it.Next() {
			item := it.Item()
			if bytes.Equal(item.Key(), lastKey) {
				continue
			}
			lastKey = lastKey[:0]
			if item.IsDeletedOrExpired() {
				continue
			}
			val, err := item.Value()
			if err != nil {
				continue
			}
			keyNoPrefix := item.Key()[len(kp):]
			// hk := hex.EncodeToString(item.Key()[len(kp):])
			// kv, err := strconv.ParseInt("0x"+hk, 0, 64)
			// if err != nil {
			// 	return err
			// }
			fmt.Printf("[%X, %8d] %d\n", keyNoPrefix, item.Version(), len(val))
			if item.DiscardEarlierVersions() {
				lastKey = item.KeyCopy(lastKey)
			}
		}
		return nil
	})
	return err
}

func doBackup(db *badger.DB) error {
	// Create File
	f, err := os.Create(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Run Backup
	_, err = db.Backup(f, 0)
	return err
}
