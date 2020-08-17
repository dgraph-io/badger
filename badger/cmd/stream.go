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
	"math"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Stream DB into another DB with different options",
	Long: `
This command streams the contents of this DB into another DB with different options.
`,
	RunE: stream,
}

var inDir string
var outDir string

func init() {
	RootCmd.AddCommand(streamCmd)
	streamCmd.Flags().StringVarP(&inDir, "in", "i", "", "Path to input DB")
	streamCmd.Flags().StringVarP(&outDir, "out", "o", "", "Path to input DB")
	streamCmd.Flags().BoolVarP(&truncate, "truncate", "", false, "Option to truncate the DBs")
	streamCmd.Flags().BoolVarP(&readOnly, "read_only", "", true,
		"Option to open in DB in read-only mode")
}

func stream(cmd *cobra.Command, args[] string) error {
	inOpt := badger.DefaultOptions(inDir).
		WithReadOnly(readOnly).
		WithTruncate(truncate).
		WithValueThreshold(1 << 10 /* 1KB */).
		WithNumVersionsToKeep(math.MaxInt32)

	outOpt := inOpt.WithCompression(options.None).WithReadOnly(false)

	inDB, err := badger.OpenManaged(inOpt)
	if err != nil {
		return errors.Wrapf(err, "cannot open DB at %s", inDir)
	}
	defer inDB.Close()
	return inDB.StreamDB(outDir, outOpt)
}
