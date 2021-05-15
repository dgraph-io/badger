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
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
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
	groupsMap map[uint64]group
}

var numpicks, numfinds uint32

func (pt *pickTable) findX(opt iteratorOptions) []badger.TableInfo {
	// fmt.Printf("picktable findX called with key: %x\n", opt.Prefix)
	if len(opt.Prefix) < pt.prefixLen {
		return pickTables(pt.all, opt)
	}
	pre := opt.Prefix[:pt.prefixLen]
	keyPrefix := z.MemHash(pre)
	g, has := pt.groupsMap[keyPrefix]
	if !has {
		return pickTables(pt.all, opt)
	}
	all := g.tables
	// fmt.Printf("key prefix: %x. len(groups): %d\n", keyPrefix, len(pt.groups))
	// var idx int
	// for i, g := range pt.groups {
	// 	// fmt.Printf("[%d] key prefix: %x g.prefix: %x\n", i, keyPrefix, g.prefix)
	// 	if bytes.Compare(keyPrefix, g.prefix) <= 0 {
	// 		idx = i
	// 		break
	// 	}
	// }
	// idx := sort.Search(len(pt.groups), func(i int) bool {
	// 	// return bytes.Compare(pt.groups[i].prefix
	// 	return bytes.Compare(keyPrefix, pt.groups[i].prefix) <= 0
	// })
	// fmt.Printf("idx=%d\n", idx)
	// if idx == len(pt.groups) {
	// 	return nil
	// }
	// if idx == -1 {
	// 	panic("handle this")
	// }
	// pt.groups[idx] now points to all the tables with a prefix >= keyPrefix.
	// So, we can safely just compare the tables.
	keySuffix := opt.Prefix[pt.prefixLen:]

	// Further reduce the all.
	idx := sort.Search(len(all), func(i int) bool {
		t := all[i]
		right := t.Right[pt.prefixLen:]
		key := y.ParseKey(right)
		if len(key) > len(keySuffix) {
			key = key[:len(keySuffix)]
		}
		return bytes.Compare(key, keySuffix) >= 0
	})
	if idx != -1 && idx < len(all) {
		numfinds++
		all = all[idx:]
	}
	// for i, t := range all {
	// 	// fmt.Printf("key: %x table right key: %x bytes.Compare: %d opt.compare: %d\n",
	// 	// 	keySuffix, key, bytes.Compare(key, keySuffix),
	// 	// 	opt.compareToPrefix(all[i].Right))
	// 	if bytes.Compare(key, keySuffix) >= 0 {
	// 		all = all[i:]
	// 		// fmt.Printf("sidx should be: %d\n", i)
	// 		break
	// 	}
	// }
	return pickTables(all, opt)

	// fmt.Printf("all tables: %d\n", len(all))
	// for i := 0; i < 10 && i < len(all); i++ {
	// 	fmt.Printf("table [%d] [%x -> %x]\n", i, all[i].Left, all[i].Right)
	// }
	// sIdx := sort.Search(len(all), func(i int) bool {
	// 	// table.Biggest >= opt.prefix
	// 	// if opt.Prefix < table.Biggest, then surely it is not in any of the preceding tables.
	// 	right := all[i].Right[pt.prefixLen:]
	// 	key := y.ParseKey(right)
	// 	if len(key) > len(keySuffix) {
	// 		key = key[:len(keySuffix)]
	// 	}
	// 	return bytes.Compare(key, keySuffix) >= 0
	// })
	// if sIdx == len(all) {
	// 	// Not found.
	// 	fmt.Printf("sidx == len(all)\n")
	// 	return nil
	// }
	// filtered := all[sIdx:]
	// // opt.prefixIsKey == true. This code is optimizing for opt.prefixIsKey part.
	// var out []badger.TableInfo
	// // hash := y.Hash(opt.Prefix)
	// // fmt.Printf("PT num filtered tables: %d\n", len(filtered))
	// for _, t := range filtered {
	// 	// When we encounter the first table whose smallest key is higher than opt.Prefix, we can
	// 	// stop. This is an IMPORTANT optimization, just considering how often we call
	// 	// NewKeyIterator.
	// 	if opt.compareToPrefix(t.Left) > 0 {
	// 		// if table.Smallest > opt.Prefix, then this and all tables after this can be ignored.
	// 		break
	// 	}
	// 	// fmt.Printf("table [%x -> %x] %x\n", t.Left, t.Right, opt.Prefix)
	// 	// fmt.Printf("left: %v right: %v\n", opt.compareToPrefix(t.Left),
	// 	// 	opt.compareToPrefix(t.Right))
	// 	out = append(out, t)
	// }
	// return out
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

var pt pickTable

type tableMap struct {
	m map[uint64]struct{}
}

var tagm = make(map[uint64]*tableMap)

func intersect(dst, src map[uint64]struct{}) {
	for id := range dst {
		if _, has := src[id]; !has {
			delete(dst, id)
		}
	}
	return
}

func findTables(opts iteratorOptions) int {
	pk, err := y.Parse(opts.Prefix)
	y.Check(err)

	tags := pk.Tags()
	// fmt.Printf("key: %x tags: %v\n", opts.Prefix, tags)
	outm := make(map[uint64]struct{})
	for _, tag := range tags {
		m, ok := tagm[tag]
		if !ok {
			// This key doesn't exist.
			fmt.Printf("Tag %d not found\n", tag)
			return 0
		}
		if len(outm) == 0 {
			for id := range m.m {
				outm[id] = struct{}{}
			}
		} else {
			intersect(outm, m.m)
		}
	}
	// fmt.Printf("key: %80x Found tables: %d\n", opts.Prefix, len(outm))
	return len(outm)
}

func benchmarkTagm(b *testing.B) {
	iopts := iteratorOptions{prefixIsKey: true}

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		iopts.Prefix = key
		findTables(iopts)
		// _ = pickTables(handler.tables, iopts)
	}
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

	// minLen := math.MaxInt32
	// maxLen := 0

	for i, t := range tables {
		if t.Level != 6 {
			continue
		}

		fmt.Printf("Table [%4d] %X\n", i, t.Right)
		fmt.Printf("Tags: %+v\n", t.Tags)

		// lp, err := y.Parse(t.Left)
		// if err != nil {
		// 	fmt.Printf("error while parsing left: %v\n", err)
		// 	continue
		// }
		// rp, err := y.Parse(t.Right)
		// if err != nil {
		// 	fmt.Printf("error while parsing right: %v\n", err)
		// 	continue
		// }

		// ltags := lp.Tags()
		// rtags := rp.Tags()

		// all := append(ltags, rtags...)
		for _, tag := range t.Tags {
			m, ok := tagm[tag]
			if !ok {
				m = &tableMap{
					m: make(map[uint64]struct{}),
				}
				tagm[tag] = m
			}
			m.m[t.ID] = struct{}{}
		}

		// fmt.Printf("left parsed: %+v tags: %+v\n", lp, ltags)
		// fmt.Printf("right parsed: %+v tags: %+v\n", rp, rtags)

		// sz := len(t.Right) - 8
		// if sz < minLen {
		// 	minLen = sz
		// }
		// if sz > maxLen {
		// 	maxLen = sz
		// }
	}
	for tag, m := range tagm {
		fmt.Printf("tag: %d len(m): %d\n", tag, len(m.m))
	}
	fmt.Printf("Num tags: %d\n", len(tagm))
	// fmt.Printf("Tables min len: %d max: %d\n", minLen, maxLen)

	// prefixLen := minLen
	// prefixLen := 16
	// for i := 0; i < 20; i++ {
	// 	groups := generateGroups(prefixLen, tables)
	// 	fmt.Printf("Found %d groups with prefixLen: %d\n", len(groups), prefixLen)
	// 	if len(groups) < 100 {
	// 		pt.prefixLen = prefixLen
	// 		pt.groups = groups
	// 		pt.all = tables
	// 		pt.groupsMap = make(map[uint64]group)
	// 		for i, g := range groups {
	// 			pt.groupsMap[z.MemHash(g.prefix)] = g
	// 			fmt.Printf("[%02d] Prefix: %x. Num Tables: %d\n", i, g.prefix, len(g.tables))
	// 		}
	// 		break
	// 	}
	// 	prefixLen--
	// }

	keys, err = getSampleKeys(db, pickOpts.sampleSize)
	y.Check(err)
	fmt.Printf("\nBenchmarking PickTables. Num keys found: %d. GOMAXPROCS: %d\n",
		len(keys), runtime.GOMAXPROCS(0))

	fmt.Printf("Matching...\n")
	iopts := iteratorOptions{prefixIsKey: true}
	var count int
	for _, key := range keys {
		iopts.Prefix = key
		exp := pickTables(tables, iopts)
		num := findTables(iopts)
		if num < len(exp) {
			panic(fmt.Sprintf("num: %d exp: %d. don't match", num, len(exp)))
		}
		// got := pt.findX(iopts)
		// fmt.Printf("key: %x exp: %d got: %d\n", key, len(exp), len(got))
		// if len(exp) != len(got) {
		// 	panic("don't match")
		// }
		// for i := range exp {
		// 	if exp[i].ID != got[i].ID {
		// 		panic("don't match at table level")
		// 	}
		// }
		count++
	}
	fmt.Printf("Matched: %d OK\n", count)
	fmt.Printf("numpicks: %d numfinds: %d\n", numpicks, numfinds)
	// os.Exit(1)

	if len(pickOpts.cpuprofile) > 0 {
		f, err := os.Create(pickOpts.cpuprofile)
		y.Check(err)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// for i := 0; i < 10; i++ {
	// 	res := testing.Benchmark(benchmarkPickTables)
	// 	fmt.Printf("Iteration [%d]: %v\n", i, res)
	// }

	for i := 0; i < 10; i++ {
		res := testing.Benchmark(benchmarkTagm)
		fmt.Printf("Tag Iteration [%d]: %v\n", i, res)
	}
	fmt.Println()
	return nil
}

func benchmarkPickTableStruct(b *testing.B) {
	iopts := iteratorOptions{prefixIsKey: true}
	// We should run this benchmark serially because we care about the latency
	// numbers, not throughput numbers.
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		iopts.Prefix = key
		_ = pt.findX(iopts)
	}
}

func benchmarkPickTables(b *testing.B) {
	iopts := iteratorOptions{prefixIsKey: true}
	// We should run this benchmark serially because we care about the latency
	// numbers, not throughput numbers.
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		iopts.Prefix = key
		_ = pickTables(handler.tables, iopts)
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
func pickTables(all []badger.TableInfo, opt iteratorOptions) []badger.TableInfo {
	// fmt.Printf("levelHandler picktables called with key: %x\n", opt.Prefix)
	numpicks++
	if len(opt.Prefix) == 0 {
		out := make([]badger.TableInfo, len(all))
		copy(out, all)
		return out
	}
	var sIdx int
	if opt.compareToPrefix(all[0].Right) >= 0 {
		// Possible that we already advanced it to here.
		sIdx = 0
	} else {
		sIdx = sort.Search(len(all), func(i int) bool {
			// table.Biggest >= opt.prefix
			// if opt.Prefix < table.Biggest, then surely it is not in any of the preceding tables.
			return opt.compareToPrefix(all[i].Right) >= 0
		})
	}
	if sIdx == len(all) {
		// Not found.
		return nil
	}

	filtered := all[sIdx:]
	// fmt.Printf("num filtered tables: %d\n", len(filtered))
	if !opt.prefixIsKey {
		panic("prefix should be key")
	}

	// opt.prefixIsKey == true. This code is optimizing for opt.prefixIsKey part.
	out := filtered[:0]
	// hash := y.Hash(opt.Prefix)
	for _, t := range filtered {
		// When we encounter the first table whose smallest key is higher than opt.Prefix, we can
		// stop. This is an IMPORTANT optimization, just considering how often we call
		// NewKeyIterator.
		if opt.compareToPrefix(t.Left) > 0 {
			// if table.Smallest > opt.Prefix, then this and all tables after this can be ignored.
			break
		}
		// fmt.Printf("table [%x -> %x] %x\n", t.Left, t.Right, opt.Prefix)
		// fmt.Printf("left: %v right: %v\n", opt.compareToPrefix(t.Left),
		// 	opt.compareToPrefix(t.Right))
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
