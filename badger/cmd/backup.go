/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"bufio"
	"math"
	"os"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var bo = struct {
	backupFile  string
	numVersions int
}{}

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
	backupCmd.Flags().StringVarP(&bo.backupFile, "backup-file", "f",
		"badger.bak", "File to backup to")
	backupCmd.Flags().IntVarP(&bo.numVersions, "num-versions", "n",
		0, "Number of versions to keep. A value <= 0 means keep all versions.")
}

func doBackup(cmd *cobra.Command, args []string) error {
	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithNumVersionsToKeep(math.MaxInt32)

	if bo.numVersions > 0 {
		opt.NumVersionsToKeep = bo.numVersions
	}

	// Open DB
	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create File
	f, err := os.Create(bo.backupFile)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(f, 64<<20)
	if _, err = db.Backup(bw, 0); err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	return f.Close()
}
