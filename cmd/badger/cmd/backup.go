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
	"os"

	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

var backupFile string

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup Badger database.",
	Long: `Backup Badger database to a file in a version-agnostic manner.

Iterates over each key-value pair, encodes it along with its metadata and
version in protocol buffers and writes them to a file. This file can later be
used by the restore command to create an identical copy of the
database.`,
	RunE: doBackup,
}

func init() {
	RootCmd.AddCommand(backupCmd)
	backupCmd.Flags().StringVarP(&backupFile, "backup-file", "f",
		"badger.bak", "File to backup to")
}

func doBackup(cmd *cobra.Command, args []string) error {
	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

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
