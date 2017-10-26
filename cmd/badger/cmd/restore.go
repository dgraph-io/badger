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
	"errors"
	"os"
	"path"

	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

var restoreFile string

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore Badger database.",
	Long: `Restore Badger database from a file.

It reads a file generated using the backup command (or by calling the
DB.Backup() API method) and writes each key-value pair found in the file to
the Badger database.

Restore creates a new database, and currently does not work on an already
existing database.`,
	RunE: doRestore,
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.Flags().StringVarP(&restoreFile, "backup-file", "f",
		"badger.bak", "File to restore from")
}

func doRestore(cmd *cobra.Command, args []string) error {
	// Check if the DB already exists
	manifestFile := path.Join(sstDir, badger.ManifestFilename)
	if _, err := os.Stat(manifestFile); err == nil { // No error. File already exists.
		return errors.New("Cannot restore to an already existing database")
	} else if os.IsNotExist(err) {
		// pass
	} else { // Return an error if anything other than the error above
		return err
	}

	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	// Open File
	f, err := os.Open(restoreFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Run restore
	return db.Load(f)
}
