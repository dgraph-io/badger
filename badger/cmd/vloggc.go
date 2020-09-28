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
	"fmt"
	"math"
	"sort"

	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/cobra"
)

var gcCmd = &cobra.Command{
	Use:   "gc command is used to clean the vlog files",
	Short: "GC vlog files",
	Long: ` GC command can be used to perform garbage collection of vlog files.
	This command will gc all vlog files that have at least 50% stale data.

	Use --dry-run flag to calculate the amount of stale data in each vlog
	file.
`,
	RunE: gc,
}

type gcOptions struct {
	dryRun     bool
	numVersion int
}

var gopt gcOptions

func init() {
	RootCmd.AddCommand(gcCmd)
	gcCmd.Flags().BoolVarP(&gopt.dryRun, "dry-run", "d", false, "Perform only sampling "+
		"and don't do GC. This will output vlog files and their discard ratio.")
	gcCmd.Flags().IntVarP(&gopt.numVersion, "num-versions", "n", 1, "Number of versions to keep. "+
		"A value <= 0 would keep all the versions.")
}

func gc(cmd *cobra.Command, args []string) error {
	if gopt.numVersion <= 0 {
		gopt.numVersion = math.MaxUint32
	}

	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithNumVersionsToKeep(gopt.numVersion)

	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	defer db.Close()

	if gopt.dryRun {
		res, err := db.SampleVlog()
		if err != nil {
			return err
		}
		sort.Slice(res, func(i, j int) bool { return res[i].DiscardRatio > res[j].DiscardRatio })

		for _, r := range res {
			fmt.Printf("Fid: %d DiscardRatio: %f\n", r.Fid, r.DiscardRatio)
		}
	} else {
		return db.GCVlog()
	}
	return nil
}
