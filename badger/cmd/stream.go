/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"io"
	"math"
	"os"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Stream DB into another DB with different options",
	Long: `
This command streams the contents of this DB into another DB with the given options.
`,
	RunE: stream,
}

var outDir string
var compressionType uint32

func init() {
	// TODO: Add more options.
	RootCmd.AddCommand(streamCmd)
	streamCmd.Flags().StringVarP(&outDir, "out", "o", "",
		"Path to output DB. The directory should be empty.")
	streamCmd.Flags().BoolVarP(&truncate, "truncate", "", false, "Option to truncate the DBs")
	streamCmd.Flags().BoolVarP(&readOnly, "read_only", "", true,
		"Option to open input DB in read-only mode")
	streamCmd.Flags().IntVarP(&numVersions, "num_versions", "", 0,
		"Option to configure the maximum number of versions per key. "+
			"Values <= 0 will be considered to have the max number of versions.")
	streamCmd.Flags().Uint32VarP(&compressionType, "compression", "", 0,
		"Option to configure the compression type in output DB. "+
			"0 to disable, 1 for Snappy, and 2 for ZSTD.")
}

func stream(cmd *cobra.Command, args []string) error {
	// Check that outDir doesn't exist or is empty.
	if _, err := os.Stat(outDir); err == nil {
		f, err := os.Open(outDir)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = f.Readdirnames(1)
		if err != io.EOF {
			return errors.Errorf("cannot run stream tool on non-empty output directory %s", outDir)
		}
	}

	// Options for input DB.
	if numVersions <= 0 {
		numVersions = math.MaxInt32
	}
	inOpt := badger.DefaultOptions(sstDir).
		WithReadOnly(readOnly).
		WithTruncate(truncate).
		WithValueThreshold(1 << 10 /* 1KB */).
		WithNumVersionsToKeep(numVersions)

	// Options for output DB.
	if compressionType < 0 || compressionType > 2 {
		return errors.Errorf(
			"compression value must be one of 0 (disabled), 1 (Snappy), or 2 (ZSTD)")
	}
	outOpt := inOpt.WithDir(outDir).WithValueDir(outDir).
		WithCompression(options.CompressionType(compressionType)).WithReadOnly(false)

	inDB, err := badger.OpenManaged(inOpt)
	if err != nil {
		return errors.Wrapf(err, "cannot open DB at %s", sstDir)
	}
	defer inDB.Close()
	return inDB.StreamDB(outOpt)
}
