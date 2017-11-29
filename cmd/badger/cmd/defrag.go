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
	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

// backupCmd represents the backup command
var defragCmd = &cobra.Command{
	Use:   "defrag",
	Short: "Defragment Badger Database.",
	Long: `
Perform a full cleanup of the Badger database and reclaim disk space , by
performing the following tasks in order:

* Purge all older versions of each key in the database

* Perform a full garbage collection run, which removes all stale entries from
  the value log, and rewrites it.

* Compact all the levels of the LSM tree.
`,
	RunE: doDefrag,
}

func init() {
	RootCmd.AddCommand(defragCmd)
}

func doDefrag(cmd *cobra.Command, args []string) error {
	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	// TODO make this step optional
	err = db.PurgeOlderVersions()
	if err != nil {
		return err
	}

	err = db.RunValueLogGCOffline()
	if err != nil {
		return err
	}

	if err = db.Close(); err != nil {
		return err
	}

	opts.NumCompactors = 0
	db, err = badger.Open(opts)
	if err != nil {
		return err
	}

	err = db.CompactLSMTreeOffline()
	if err != nil {
		return nil
	}

	db.Close()
	return nil
}
