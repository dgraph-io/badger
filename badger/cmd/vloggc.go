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
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/spf13/cobra"
)

var gcCmd = &cobra.Command{
	Use:   "gc command is used to clean the vlog files",
	Short: "GC vlog files",
	Long: ` GC command can be used to perform garbage collection of vlog files.
	This command will gc all vlog files unless --fid flag is specified. Calling
	GC on a vlog file that doesn't have enough stale data would create move
	keys for the valid data (that was moved) and this will affect the read performance.

	Use --sample-only flag to calculate the amount of stale data in each vlog
	file and then perform GC on set of files using the --fid flag.
`,
	RunE: gc,
}

type gcOptions struct {
	sampleOnly bool
	fid        string
	numVersion int
	timeout    string
}

var gopt gcOptions

func init() {
	RootCmd.AddCommand(gcCmd)
	gcCmd.Flags().BoolVarP(&gopt.sampleOnly, "sample-only", "s", false, "Perform only sampling "+
		"and don't do GC. This can be used to find the files that need to be gc'ed.")
	gcCmd.Flags().StringVarP(&gopt.fid, "fids", "f", "",
		"Perform GC/Sampling on the specified fids only.")
	gcCmd.Flags().IntVarP(&gopt.numVersion, "num-versions", "n", 1, "todo")
	bankTest.Flags().StringVarP(&gopt.timeout, "duration", "d", "10m", "How long to run the test.")
}

func gc(cmd *cobra.Command, args []string) error {
	var fids []uint32
	// Support comma separated fid list.
	if len(gopt.fid) > 0 {
		r := csv.NewReader(strings.NewReader(gopt.fid))
		entries, err := r.ReadAll()
		if err != nil {
			return err
		}

		for _, e := range entries[0] {
			u64, err := strconv.ParseUint(e, 10, 32)
			if err != nil {
				return err
			}
			fids = append(fids, uint32(u64))
		}
	}

	dur, err := time.ParseDuration(duration)
	y.Check(err)

	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithNumVersionsToKeep(gopt.numVersion)

	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	defer db.Close()

	if gopt.sampleOnly {
		res, err := db.SampleVlog(fids, dur)
		if err != nil {
			return err
		}
		sort.Slice(res, func(i, j int) bool { return res[i].DiscardRatio > res[j].DiscardRatio })

		for _, r := range res {
			fmt.Printf("Fid: %d DiscardRatio: %f\n", r.Fid, r.DiscardRatio)
		}
	} else {
		return db.GCVlog(fids, dur)
	}
	return nil
}
