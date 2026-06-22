/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

type tableMock struct {
	left, right []byte
}

func (tm *tableMock) Smallest() []byte             { return tm.left }
func (tm *tableMock) Biggest() []byte              { return tm.right }
func (tm *tableMock) DoesNotHave(hash uint32) bool { return false }
func (tm *tableMock) MaxVersion() uint64           { return math.MaxUint64 }

// OutsideVersionRange: the mock has no version-range metadata, so it is never
// skipped (matches the behavior of older real tables).
func (tm *tableMock) OutsideVersionRange(lower, upper uint64) bool { return false }

func TestPickTables(t *testing.T) {
	opt := DefaultIteratorOptions

	within := func(prefix, left, right []byte) {
		opt.Prefix = prefix
		// PickTable expects smallest and biggest to contain timestamps.
		tm := &tableMock{left: y.KeyWithTs(left, 1), right: y.KeyWithTs(right, 1)}
		require.True(t, opt.pickTable(tm), "within failed for %b %b %b\n", prefix, left, right)
	}
	outside := func(prefix, left, right string) {
		opt.Prefix = []byte(prefix)
		// PickTable expects smallest and biggest to contain timestamps.
		tm := &tableMock{left: y.KeyWithTs([]byte(left), 1), right: y.KeyWithTs([]byte(right), 1)}
		require.False(t, opt.pickTable(tm), "outside failed for %b %b %b", prefix, left, right)
	}
	within([]byte("abc"), []byte("ab"), []byte("ad"))
	within([]byte("abc"), []byte("abc"), []byte("ad"))
	within([]byte("abc"), []byte("abb123"), []byte("ad"))
	within([]byte("abc"), []byte("abc123"), []byte("abd234"))
	within([]byte("abc"), []byte("abc123"), []byte("abc456"))
	// Regression test for https://github.com/dgraph-io/badger/issues/992
	within([]byte{0, 0, 1}, []byte{0}, []byte{0, 0, 1})

	outside("abd", "abe", "ad")
	outside("abd", "ac", "ad")
	outside("abd", "b", "e")
	outside("abd", "a", "ab")
	outside("abd", "ab", "abc")
	outside("abd", "ab", "abc123")
}

func TestPickSortTables(t *testing.T) {
	type MockKeys struct {
		small string
		large string
	}
	genTables := func(mks ...MockKeys) []*table.Table {
		out := make([]*table.Table, 0)
		for _, mk := range mks {
			opts := table.Options{ChkMode: options.OnTableAndBlockRead}
			tbl := buildTable(t, [][]string{{mk.small, "some value"},
				{mk.large, "some value"}}, opts)
			defer func() { require.NoError(t, tbl.DecrRef()) }()
			out = append(out, tbl)
		}
		return out
	}
	tables := genTables(MockKeys{small: "a", large: "abc"},
		MockKeys{small: "abcd", large: "cde"},
		MockKeys{small: "cge", large: "chf"},
		MockKeys{small: "glr", large: "gyup"})
	opt := DefaultIteratorOptions
	opt.prefixIsKey = false
	opt.Prefix = []byte("c")
	filtered := opt.pickTables(tables)
	require.Equal(t, 2, len(filtered))
	// build table adds time stamp so removing tailing bytes.
	require.Equal(t, filtered[0].Smallest()[:4], []byte("abcd"))
	require.Equal(t, filtered[1].Smallest()[:3], []byte("cge"))
	tables = genTables(MockKeys{small: "a", large: "abc"},
		MockKeys{small: "abcd", large: "ade"},
		MockKeys{small: "cge", large: "chf"},
		MockKeys{small: "glr", large: "gyup"})
	filtered = opt.pickTables(tables)
	require.Equal(t, 1, len(filtered))
	require.Equal(t, filtered[0].Smallest()[:3], []byte("cge"))
	tables = genTables(MockKeys{small: "a", large: "abc"},
		MockKeys{small: "abcd", large: "ade"},
		MockKeys{small: "cge", large: "chf"},
		MockKeys{small: "ckr", large: "cyup"},
		MockKeys{small: "csfr", large: "gyup"})
	filtered = opt.pickTables(tables)
	require.Equal(t, 3, len(filtered))
	require.Equal(t, filtered[0].Smallest()[:3], []byte("cge"))
	require.Equal(t, filtered[1].Smallest()[:3], []byte("ckr"))
	require.Equal(t, filtered[2].Smallest()[:4], []byte("csfr"))

	opt.Prefix = []byte("aa")
	filtered = opt.pickTables(tables)
	require.Equal(t, y.ParseKey(filtered[0].Smallest()), []byte("a"))
	require.Equal(t, y.ParseKey(filtered[0].Biggest()), []byte("abc"))
}

func TestIterateSinceTs(t *testing.T) {
	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%04d", i))
	}
	val := []byte("OK")
	n := 100000

	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		batch := db.NewWriteBatch()
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			require.NoError(t, batch.Set(bkey(i), val))
		}
		require.NoError(t, batch.Flush())

		maxVs := db.MaxVersion()
		sinceTs := maxVs - maxVs/10
		iopt := DefaultIteratorOptions
		iopt.SinceTs = sinceTs

		require.NoError(t, db.View(func(txn *Txn) error {
			it := txn.NewIterator(iopt)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				i := it.Item()
				require.GreaterOrEqual(t, i.Version(), sinceTs)
			}
			return nil
		}))

	})
}

// TestIterateVersionWindow asserts that a version-bounded scan using UntilTs (an
// upper bound complementing SinceTs) returns IDENTICAL results to an unbounded
// AllVersions scan filtered to the same window, across multi-version data
// including deletes, after the data has been flushed to SSTables. This exercises
// both the table-level and block-level version-range skipping on the read path.
func TestIterateVersionWindow(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := getTestOptions(dir)
	db, err := OpenManaged(opts)
	require.NoError(t, err)

	const nkeys = 200
	bkey := func(i int) []byte { return []byte(fmt.Sprintf("key%05d", i)) }

	// Write three version bands far apart so whole tables/blocks fall outside
	// typical windows. Some keys get deleted at a later version.
	type rec struct {
		key     string
		version uint64
		deleted bool
	}
	var written []rec
	bands := []uint64{10, 1000, 100000}
	for _, base := range bands {
		for i := 0; i < nkeys; i++ {
			ver := base + uint64(i%50)
			txn := db.NewTransactionAt(ver-1, true)
			if i%7 == 0 {
				require.NoError(t, txn.Delete(bkey(i)))
				written = append(written, rec{key: string(bkey(i)), version: ver, deleted: true})
			} else {
				require.NoError(t, txn.Set(bkey(i), []byte(fmt.Sprintf("v%d", ver))))
				written = append(written, rec{key: string(bkey(i)), version: ver})
			}
			require.NoError(t, txn.CommitAt(ver, nil))
		}
	}
	// Flatten to push everything into SSTables so the storage-level skip applies.
	require.NoError(t, db.Flatten(2))

	// reference returns the set of (key,version) pairs (incl. deletes) within the
	// window, matching what an AllVersions+InternalAccess iterator should surface.
	reference := func(lower, upper uint64) map[string]bool {
		m := map[string]bool{}
		for _, r := range written {
			if r.version > lower && r.version <= upper {
				m[fmt.Sprintf("%s@%d", r.key, r.version)] = true
			}
		}
		return m
	}

	readWindow := func(sinceTs, untilTs, readTs uint64) map[string]bool {
		iopt := DefaultIteratorOptions
		iopt.AllVersions = true
		iopt.SinceTs = sinceTs
		iopt.UntilTs = untilTs
		got := map[string]bool{}
		txn := db.NewTransactionAt(readTs, false)
		defer txn.Discard()
		it := txn.NewIterator(iopt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			got[fmt.Sprintf("%s@%d", string(item.KeyCopy(nil)), item.Version())] = true
		}
		return got
	}

	readTs := db.MaxVersion()
	windows := [][2]uint64{
		{0, math.MaxUint64}, // full
		{0, 500},            // only band 1
		{900, 1100},         // only band 2
		{50000, 200000},     // only band 3
		{500, 900},          // gap => empty
		{0, 1049},           // bands 1 and 2
	}
	for _, w := range windows {
		sinceTs, untilTs := w[0], w[1]
		t.Run(fmt.Sprintf("window_%d_%d", sinceTs, untilTs), func(t *testing.T) {
			want := reference(sinceTs, untilTs)
			got := readWindow(sinceTs, untilTs, readTs)
			require.Equal(t, want, got, "bounded window scan must match manual filter")
		})
	}

	require.NoError(t, db.Close())
}

// TestUntilTsLowerBoundKeepsSmallestVersion is an end-to-end regression test for
// the version-window lower-bound bug. badger rejects CommitTs==0, so the smallest
// storable version is 1; we write a key at version 1, flush it to an SSTable, and
// run an UntilTs-only scan (SinceTs=0, UntilTs=1). With the lower bound correctly
// set to 0 (not SinceTs+1=1), the entry's block/table is not pruned and the key
// is returned. (The old code used lower=1, which is fine for a v=1 entry but wrong
// for a v=0 one; this test pins the boundary for the smallest publicly storable
// version, and the unit/table tests cover v=0 directly.)
func TestUntilTsLowerBoundKeepsSmallestVersion(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(getTestOptions(dir))
	require.NoError(t, err)

	txn := db.NewTransactionAt(0, true)
	require.NoError(t, txn.Set([]byte("k"), []byte("v1")))
	require.NoError(t, txn.CommitAt(1, nil))
	require.NoError(t, db.Flatten(1))

	iopt := DefaultIteratorOptions
	iopt.AllVersions = true
	iopt.SinceTs = 0
	iopt.UntilTs = 1

	rtxn := db.NewTransactionAt(math.MaxUint64, false)
	defer rtxn.Discard()
	it := rtxn.NewIterator(iopt)
	defer it.Close()

	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		keys = append(keys, fmt.Sprintf("%s@%d", string(item.KeyCopy(nil)), item.Version()))
	}
	require.Equal(t, []string{"k@1"}, keys, "smallest-version entry must survive an UntilTs-only scan")
	require.NoError(t, db.Close())
}

// TestVersionWindowLowerBound guards the boundary alignment between the storage
// prune window and the authoritative per-entry filter. The per-entry filter only
// excludes v <= SinceTs when SinceTs > 0, so for an UntilTs-only scan version 0
// is in-window and lower MUST be 0 (not 1) or a maxVersion==0 block gets wrongly
// pruned.
func TestVersionWindowLowerBound(t *testing.T) {
	cases := []struct {
		sinceTs, untilTs, readTs uint64
		wantLower, wantUpper     uint64
		wantEnabled              bool
	}{
		// UntilTs-only scan: lower must be 0 so version-0 data stays in-window.
		{sinceTs: 0, untilTs: 100, readTs: math.MaxUint64, wantLower: 0, wantUpper: 100, wantEnabled: true},
		// SinceTs set: lower excludes v <= SinceTs.
		{sinceTs: 10, untilTs: 0, readTs: math.MaxUint64, wantLower: 11, wantUpper: math.MaxUint64, wantEnabled: true},
		{sinceTs: 10, untilTs: 100, readTs: math.MaxUint64, wantLower: 11, wantUpper: 100, wantEnabled: true},
		// readTs caps the upper bound when smaller than UntilTs.
		{sinceTs: 0, untilTs: 100, readTs: 50, wantLower: 0, wantUpper: 50, wantEnabled: true},
		// No bounds: disabled.
		{sinceTs: 0, untilTs: 0, readTs: math.MaxUint64, wantLower: 0, wantUpper: math.MaxUint64, wantEnabled: false},
		// SinceTs at the max: empty window, no overflow.
		{sinceTs: math.MaxUint64, untilTs: 0, readTs: math.MaxUint64, wantLower: math.MaxUint64, wantUpper: 0, wantEnabled: true},
	}
	for _, c := range cases {
		opt := IteratorOptions{SinceTs: c.sinceTs, UntilTs: c.untilTs}
		lower, upper, enabled := opt.versionWindow(c.readTs)
		require.Equal(t, c.wantLower, lower, "lower for since=%d until=%d", c.sinceTs, c.untilTs)
		require.Equal(t, c.wantUpper, upper, "upper for since=%d until=%d", c.sinceTs, c.untilTs)
		require.Equal(t, c.wantEnabled, enabled, "enabled for since=%d until=%d", c.sinceTs, c.untilTs)
	}
}

// TestUntilTsKeepsVersionZeroTable asserts the table picker does NOT prune a
// table whose entire version range is [0,0] on an UntilTs-only scan: version 0
// is in-window because SinceTs==0.
func TestUntilTsKeepsVersionZeroTable(t *testing.T) {
	opt := DefaultIteratorOptions
	opt.SinceTs = 0
	opt.UntilTs = 100
	zeroTable := &prunableTableMock{minV: 0, maxV: 0, hasRange: true}
	require.True(t, opt.pickTable(zeroTable),
		"version-0 table must NOT be pruned on an UntilTs-only scan")
}

// TestUntilTsPrunesTables asserts that the table picker actually drops SSTables
// whose version range lies entirely above the UntilTs upper bound.
func TestUntilTsPrunesTables(t *testing.T) {
	mk := func(minV, maxV uint64, hasRange bool) *prunableTableMock {
		return &prunableTableMock{minV: minV, maxV: maxV, hasRange: hasRange}
	}
	opt := DefaultIteratorOptions
	opt.UntilTs = 100

	// Table entirely above the window => pruned.
	require.False(t, opt.pickTable(mk(500, 600, true)))
	// Table intersecting the window => kept.
	require.True(t, opt.pickTable(mk(50, 120, true)))
	// Table below window (still has versions <= 100) => kept.
	require.True(t, opt.pickTable(mk(1, 50, true)))
	// Old-format table without a range => never pruned, even if it "looks" above.
	require.True(t, opt.pickTable(mk(500, 600, false)))
}

// prunableTableMock implements table.TableInterface with a controllable version
// range, for testing the UntilTs table-pruning predicate.
type prunableTableMock struct {
	minV, maxV uint64
	hasRange   bool
}

func (m *prunableTableMock) Smallest() []byte             { return y.KeyWithTs([]byte("a"), 1) }
func (m *prunableTableMock) Biggest() []byte              { return y.KeyWithTs([]byte("z"), 1) }
func (m *prunableTableMock) DoesNotHave(hash uint32) bool { return false }
func (m *prunableTableMock) MaxVersion() uint64           { return m.maxV }
func (m *prunableTableMock) OutsideVersionRange(lower, upper uint64) bool {
	if !m.hasRange {
		return false
	}
	return m.maxV < lower || m.minV > upper
}

func TestIterateSinceTsWithPendingWrites(t *testing.T) {
	// The pending entries still have version=0. Even IteratorOptions.SinceTs is 0, the entries
	// should be visible.
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		require.NoError(t, txn.Set([]byte("key1"), []byte("value1")))
		require.NoError(t, txn.Set([]byte("key2"), []byte("value2")))
		itr := txn.NewIterator(DefaultIteratorOptions)
		defer itr.Close()
		count := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			count++
		}
		require.Equal(t, 2, count)
	})
}

func TestIteratePrefix(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	testIteratorPrefix := func(t *testing.T, db *DB) {
		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%04d", i))
		}
		val := []byte("OK")
		n := 10000

		batch := db.NewWriteBatch()
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			require.NoError(t, batch.Set(bkey(i), val))
		}
		require.NoError(t, batch.Flush())

		countKeys := func(prefix string) int {
			t.Logf("Testing with prefix: %s", prefix)
			var count int
			opt := DefaultIteratorOptions
			opt.Prefix = []byte(prefix)
			err := db.View(func(txn *Txn) error {
				itr := txn.NewIterator(opt)
				defer itr.Close()
				for itr.Rewind(); itr.Valid(); itr.Next() {
					item := itr.Item()
					err := item.Value(func(v []byte) error {
						require.Equal(t, val, v)
						return nil
					})
					require.NoError(t, err)
					require.True(t, bytes.HasPrefix(item.Key(), opt.Prefix))
					count++
				}
				return nil
			})
			require.NoError(t, err)
			return count
		}

		countOneKey := func(key []byte) int {
			var count int
			err := db.View(func(txn *Txn) error {
				itr := txn.NewKeyIterator(key, DefaultIteratorOptions)
				defer itr.Close()
				for itr.Rewind(); itr.Valid(); itr.Next() {
					item := itr.Item()
					err := item.Value(func(v []byte) error {
						require.Equal(t, val, v)
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, key, item.Key())
					count++
				}
				return nil
			})
			require.NoError(t, err)
			return count
		}

		for i := 0; i <= 9; i++ {
			require.Equal(t, 1, countKeys(fmt.Sprintf("%d%d%d%d", i, i, i, i)))
			require.Equal(t, 10, countKeys(fmt.Sprintf("%d%d%d", i, i, i)))
			require.Equal(t, 100, countKeys(fmt.Sprintf("%d%d", i, i)))
			require.Equal(t, 1000, countKeys(fmt.Sprintf("%d", i)))
		}
		require.Equal(t, 10000, countKeys(""))

		t.Logf("Testing each key with key iterator")
		for i := 0; i < n; i++ {
			require.Equal(t, 1, countOneKey(bkey(i)))
		}
	}

	t.Run("With Default options", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			testIteratorPrefix(t, db)
		})
	})

	t.Run("With Block Offsets in Cache", func(t *testing.T) {
		opts := getTestOptions("")
		opts.IndexCacheSize = 100 << 20
		runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
			testIteratorPrefix(t, db)
		})
	})

	t.Run("With Block Offsets and Blocks in Cache", func(t *testing.T) {
		opts := getTestOptions("")
		opts.BlockCacheSize = 100 << 20
		opts.IndexCacheSize = 100 << 20
		runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
			testIteratorPrefix(t, db)
		})
	})

	t.Run("With Blocks in Cache", func(t *testing.T) {
		opts := getTestOptions("")
		opts.BlockCacheSize = 100 << 20
		runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
			testIteratorPrefix(t, db)
		})
	})

}

// Sanity test to verify the iterator does not crash the db in readonly mode if data does not exist.
func TestIteratorReadOnlyWithNoData(t *testing.T) {
	dir, err := os.MkdirTemp(".", "badger-test")
	y.Check(err)
	defer removeDir(dir)
	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	opts.ReadOnly = true
	db, err = Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	require.NoError(t, db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = []byte("xxx")
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		return nil
	}))
}

// go test -v -run=XXX -bench=BenchmarkIterate -benchtime=3s
// Benchmark with opt.Prefix set ===
// goos: linux
// goarch: amd64
// pkg: github.com/dgraph-io/badger
// BenchmarkIteratePrefixSingleKey/Key_lookups-4         	   10000	    365539 ns/op
// --- BENCH: BenchmarkIteratePrefixSingleKey/Key_lookups-4
//
//	iterator_test.go:147: Inner b.N: 1
//	iterator_test.go:147: Inner b.N: 100
//	iterator_test.go:147: Inner b.N: 10000
//
// --- BENCH: BenchmarkIteratePrefixSingleKey
//
//	iterator_test.go:143: LSM files: 79
//	iterator_test.go:145: Outer b.N: 1
//
// PASS
// ok  	github.com/dgraph-io/badger	41.586s
//
// Benchmark with NO opt.Prefix set ===
// goos: linux
// goarch: amd64
// pkg: github.com/dgraph-io/badger
// BenchmarkIteratePrefixSingleKey/Key_lookups-4         	   10000	    460924 ns/op
// --- BENCH: BenchmarkIteratePrefixSingleKey/Key_lookups-4
//
//	iterator_test.go:147: Inner b.N: 1
//	iterator_test.go:147: Inner b.N: 100
//	iterator_test.go:147: Inner b.N: 10000
//
// --- BENCH: BenchmarkIteratePrefixSingleKey
//
//	iterator_test.go:143: LSM files: 83
//	iterator_test.go:145: Outer b.N: 1
//
// PASS
// ok  	github.com/dgraph-io/badger	41.836s
//
// Only my laptop there's a 20% improvement in latency with ~80 files.
func BenchmarkIteratePrefixSingleKey(b *testing.B) {
	dir, err := os.MkdirTemp(".", "badger-test")
	y.Check(err)
	defer removeDir(dir)
	opts := getTestOptions(dir)
	db, err := Open(opts)
	y.Check(err)
	defer db.Close()

	N := 100000 // Should generate around 80 SSTables.
	val := []byte("OK")
	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%06d", i))
	}

	batch := db.NewWriteBatch()
	for i := 0; i < N; i++ {
		y.Check(batch.Set(bkey(i), val))
	}
	y.Check(batch.Flush())
	var lsmFiles int
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".sst") {
			lsmFiles++
		}
		if err != nil {
			return err
		}
		return nil
	})
	y.Check(err)
	b.Logf("LSM files: %d", lsmFiles)
	b.Logf("Key splits: %v", db.Ranges(nil, 10000))
	b.Logf("Key splits with prefix: %v", db.Ranges([]byte("09"), 10000))

	b.Logf("Outer b.N: %d", b.N)
	b.Run("Key lookups", func(b *testing.B) {
		b.Logf("Inner b.N: %d", b.N)
		for i := 0; i < b.N; i++ {
			key := bkey(rand.Intn(N))
			err := db.View(func(txn *Txn) error {
				opt := DefaultIteratorOptions
				// NOTE: Comment opt.Prefix out here to compare the performance
				// difference between providing Prefix as an option, v/s not. I
				// see a 20% improvement when there are ~80 SSTables.
				opt.Prefix = key
				opt.AllVersions = true

				itr := txn.NewIterator(opt)
				defer itr.Close()

				var count int
				for itr.Seek(key); itr.ValidForPrefix(key); itr.Next() {
					count++
				}
				if count != 1 {
					b.Fatalf("Count must be one key: %s. Found: %d", key, count)
				}
				return nil
			})
			if err != nil {
				b.Fatalf("Error while View: %v", err)
			}
		}
	})
}
