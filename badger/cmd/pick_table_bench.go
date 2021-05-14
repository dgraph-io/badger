/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/spf13/cobra"
)

var pickBenchCmd = &cobra.Command{
	Use:   "picktable",
	Short: "Benchmark pick tables.",
	Long:  `This command simulates pickTables used in iterators.`,
	RunE:  pickTableBench,
}

var (
	pickOpts = struct {
		readOnly   bool
		sampleSize int
		cpuprofile string
	}{}
	keys    [][]byte
	handler levelHandler
)

func init() {
	benchCmd.AddCommand(pickBenchCmd)
	pickBenchCmd.Flags().BoolVar(
		&pickOpts.readOnly, "read-only", true, "If true, DB will be opened in read only mode.")
	pickBenchCmd.Flags().IntVar(
		&pickOpts.sampleSize, "sample-size", 1000000, "Sample size of keys to be used for lookup.")
	pickBenchCmd.Flags().StringVar(
		&pickOpts.cpuprofile, "cpuprofile", "", "Write CPU profile to file.")
}

type group struct {
	prefix []byte
	tables []badger.TableInfo
}

type pickTable struct {
	prefixLen int
	groups    []group
	all       []badger.TableInfo
}

func (pt *pickTable) find(opt iteratorOptions) []badger.TableInfo {
	var all []badger.TableInfo
	if len(opt.Prefix) < pt.prefixLen {
		all = pt.all
	} else {
		keyPrefix := opt.Prefix[:pt.prefixLen]
		idx := sort.Search(len(pt.groups), func(i int) bool {
			// return bytes.Compare(pt.groups[i].prefix
			return bytes.Compare(keyPrefix, pt.groups[i].prefix) >= 0
		})
		if idx == len(pt.groups) {
			return nil
		}
		if idx == -1 {
			panic("handle this")
		}
		// pt.groups[idx] now points to all the tables with a prefix >= keyPrefix.
		// So, we can safely just compare the tables.
		all = pt.groups[idx].tables
	}
	sIdx := sort.Search(len(all), func(i int) bool {
		// table.Biggest >= opt.prefix
		// if opt.Prefix < table.Biggest, then surely it is not in any of the preceding tables.
		return opt.compareToPrefix(all[i].Right) >= 0
	})
	if sIdx == len(all) {
		// Not found.
		return nil
	}
	filtered := all[sIdx:]
	// opt.prefixIsKey == true. This code is optimizing for opt.prefixIsKey part.
	var out []badger.TableInfo
	// hash := y.Hash(opt.Prefix)
	for _, t := range filtered {
		// When we encounter the first table whose smallest key is higher than opt.Prefix, we can
		// stop. This is an IMPORTANT optimization, just considering how often we call
		// NewKeyIterator.
		if opt.compareToPrefix(t.Left) > 0 {
			// if table.Smallest > opt.Prefix, then this and all tables after this can be ignored.
		}
		out = append(out, t)
	}
	return nil
}

func benchmarkPickTableStruct(b *testing.B) {
}

func generateGroups(prefixLen int, tables []badger.TableInfo) []group {
	var groups []group
	for left := 0; left < len(tables)-1; {
		k := y.ParseKey(tables[left].Right)
		// TODO: Ensure min, max calculation is considering y.ParseKey.
		prefix := k[:prefixLen]

		// Lets find how many tables have this prefix.
		right := left + 1
		for right < len(tables) {
			rt := tables[right]
			if !bytes.HasPrefix(rt.Right, prefix) {
				break
			}
			right++
		}
		fmt.Printf("Prefix: %x. Num tables: %d. Jumping to right: %d\n", prefix, right-left, right)
		groups = append(groups, group{
			prefix: prefix,
			tables: tables[left:right],
		})
		left = right
		if right >= len(tables) {
			break
		}
	}
	return groups
}

func pickTableBench(cmd *cobra.Command, args []string) error {
	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithReadOnly(pickOpts.readOnly)
	fmt.Printf("Opening badger with options = %+v\n", opt)
	db, err := badger.OpenManaged(opt)
	if err != nil {
		return y.Wrapf(err, "unable to open DB")
	}
	defer db.Close()

	tables := db.Tables()
	// boundaries := getBoundaries(db)
	// tables := genTables(boundaries)
	tables = handler.init(tables)

	minLen := math.MaxInt32
	maxLen := 0
	for i, t := range tables {
		fmt.Printf("Table [%4d] %X\n", i, t.Right)
		sz := len(t.Right)
		if sz < minLen {
			minLen = sz
		}
		if sz > maxLen {
			maxLen = sz
		}
	}
	fmt.Printf("Tables min len: %d max: %d\n", minLen, maxLen)
	prefixLen := minLen
	var pt pickTable
	for i := 0; i < 20; i++ {
		groups := generateGroups(prefixLen, tables)
		fmt.Printf("Found %d groups with prefixLen: %d\n", len(groups), prefixLen)
		if len(groups) < 200 {
			pt.prefixLen = prefixLen
			pt.groups = groups
			pt.all = tables
			for i, g := range groups {
				fmt.Printf("[%02d] Prefix: %x. Num Tables: %d\n", i, g.prefix, len(g.tables))
			}
			break
		}
		prefixLen--
	}

	keys, err = getSampleKeys(db, pickOpts.sampleSize)
	y.Check(err)
	fmt.Printf("\nBenchmarking PickTables. Num keys found: %d. GOMAXPROCS: %d\n",
		len(keys), runtime.GOMAXPROCS(0))

	fmt.Printf("Matching...\n")
	iopts := iteratorOptions{prefixIsKey: true}
	for _, key := range keys {
		iopts.Prefix = key
		exp := handler.pickTables(iopts)
		got := pt.find(iopts)
		fmt.Printf("key: %x exp: %d got: %d\n", key, len(exp), len(got))
		if len(exp) != len(got) {
			panic("don't match")
		}
		// for i := range exp {
		// 	if exp[i] != got[i] {
		// 		panic("don't match at table level")
		// 	}
		// }
	}
	fmt.Println("DONE")
	os.Exit(1)

	if len(pickOpts.cpuprofile) > 0 {
		f, err := os.Create(pickOpts.cpuprofile)
		y.Check(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	for i := 0; i < 10; i++ {
		res := testing.Benchmark(benchmarkPickTables)
		fmt.Printf("Iteration [%d]: %v\n", i, res)
	}
	fmt.Println()
	return nil
}

func benchmarkPickTables(b *testing.B) {
	iopts := iteratorOptions{prefixIsKey: true}
	// We should run this benchmark serially because we care about the latency
	// numbers, not throughput numbers.
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		iopts.Prefix = key
		_ = handler.pickTables(iopts)
	}
}

// See badger.IteratorOptions (iterator.go)
type iteratorOptions struct {
	prefixIsKey bool   // If set, use the prefix for bloom filter lookup.
	Prefix      []byte // Only iterate over this given prefix.
	SinceTs     uint64 // Only read data that has version > SinceTs.
}

// See compareToPrefix in iterator.go
func (opt *iteratorOptions) compareToPrefix(key []byte) int {
	// We should compare key without timestamp. For example key - a[TS] might be > "aa" prefix.
	key = y.ParseKey(key)
	if len(key) > len(opt.Prefix) {
		key = key[:len(opt.Prefix)]
	}
	return bytes.Compare(key, opt.Prefix)
}

// See levelHandler in level_handler.go
type levelHandler struct {
	tables []badger.TableInfo
}

func (s *levelHandler) init(tables []badger.TableInfo) []badger.TableInfo {
	fmt.Println("Initializing level handler...")
	out := tables[:0]
	for _, t := range tables {
		if t.Level != 6 {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		return y.CompareKeys(out[i].Right, out[j].Right) < 0
	})
	s.tables = out
	return s.tables
}

// This implementation is based on the implementation in master branch.
func (s *levelHandler) pickTables(opt iteratorOptions) []badger.TableInfo {
	all := s.tables
	if len(opt.Prefix) == 0 {
		out := make([]badger.TableInfo, len(all))
		copy(out, all)
		return out
	}
	sIdx := sort.Search(len(all), func(i int) bool {
		// table.Biggest >= opt.prefix
		// if opt.Prefix < table.Biggest, then surely it is not in any of the preceding tables.
		return opt.compareToPrefix(all[i].Right) >= 0
	})
	if sIdx == len(all) {
		// Not found.
		return nil
	}

	filtered := all[sIdx:]
	fmt.Printf("num filtered tables: %d\n", len(filtered))
	if !opt.prefixIsKey {
		eIdx := sort.Search(len(filtered), func(i int) bool {
			return opt.compareToPrefix(filtered[i].Right) > 0
		})
		out := make([]badger.TableInfo, len(filtered[:eIdx]))
		copy(out, filtered[:eIdx])
		return out
	}

	// opt.prefixIsKey == true. This code is optimizing for opt.prefixIsKey part.
	var out []badger.TableInfo
	// hash := y.Hash(opt.Prefix)
	for _, t := range filtered {
		// When we encounter the first table whose smallest key is higher than opt.Prefix, we can
		// stop. This is an IMPORTANT optimization, just considering how often we call
		// NewKeyIterator.
		if opt.compareToPrefix(t.Left) > 0 {
			// if table.Smallest > opt.Prefix, then this and all tables after this can be ignored.
			fmt.Printf("table [%x -> %x] %x\n", t.Left, t.Right, opt.Prefix)
			fmt.Printf("left: %v right: %v\n", opt.compareToPrefix(t.Left),
				opt.compareToPrefix(t.Right))
			break
		}
		out = append(out, t)
	}
	return out
}

// // Sorts the boundaries and creates mock table out of them.
// func genTables(boundaries [][]byte) []*table.Table {
// 	buildTable := func(k1, k2 []byte) *table.Table {
// 		opts := table.Options{
// 			ChkMode: options.NoVerification,
// 		}
// 		b := table.NewTableBuilder(opts)
// 		defer b.Close()
// 		// Add one key so that we can open this table.
// 		b.Add(k1, y.ValueStruct{}, 0)
// 		b.Add(k2, y.ValueStruct{}, 0)
// 		tab, err := table.OpenInMemoryTable(b.Finish(), 0, &opts)
// 		y.Check(err)
// 		return tab
// 	}

// 	sort.Slice(boundaries, func(i, j int) bool {
// 		return bytes.Compare(boundaries[i], boundaries[j]) < 0
// 	})
// 	out := make([]*table.Table, 0, len(boundaries))
// 	for i := range boundaries {
// 		var j int
// 		if i != 0 {
// 			j = i - 1
// 		}
// 		out = append(out, buildTable(boundaries[i], boundaries[j]))
// 	}
// 	fmt.Printf("Created %d mock tables.\n", len(out))
// 	return out
// }

// func getBoundaries(db *badger.DB) []TableInfo {
// 	fmt.Println("Getting the table boundaries...")
// 	tables := db.Tables()

// 	out := make([][]byte, 0, 2*len(tables))
// 	for _, t := range tables {
// 		out = append(out, t.Left, t.Right)
// 	}
// 	return out
// }
