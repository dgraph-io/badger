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

// TestPickTables tests selecting tables matching iterator filtering options
func TestPickTables(t *testing.T) {
	opt := DefaultIteratorOptions

	within := func(rng Range, prefix, left, right []byte) {
		opt.Range = rng.Prefix(prefix)
		opt.inRange = opt.Range
		// PickTable expects smallest and biggest to contain timestamps.
		tm := &tableMock{left: y.KeyWithTs(left, 1), right: y.KeyWithTs(right, 1)}
		require.Truef(t, opt.pickTable(tm), "within failed for %v %v %v\n", prefix, left, right)
	}
	outside := func(rng Range, prefix, left, right string) {
		opt.Range = rng.Prefix([]byte(prefix))
		opt.inRange = opt.Range
		// PickTable expects smallest and biggest to contain timestamps.
		tm := &tableMock{left: y.KeyWithTs([]byte(left), 1), right: y.KeyWithTs([]byte(right), 1)}
		require.Falsef(t, opt.pickTable(tm), "outside failed for %v %v %v", prefix, left, right)
	}
	within(Range{}, []byte("abc"), []byte("ab"), []byte("ad"))
	within(Range{}, []byte("abc"), []byte("abc"), []byte("ad"))
	within(Range{}, []byte("abc"), []byte("abb123"), []byte("ad"))
	within(Range{}, []byte("abc"), []byte("abc123"), []byte("abd234"))
	within(Range{}, []byte("abc"), []byte("abc123"), []byte("abc456"))
	// Regression test for https://github.com/dgraph-io/badger/issues/992
	within(Range{}, []byte{0, 0, 1}, []byte{0}, []byte{0, 0, 1})
	within(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc300")}, []byte("abc"), []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc123")}, []byte("abc"), []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc234"), Right: ([]byte)("abc300")}, []byte("abc"), []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc300")}, nil, []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc123")}, nil, []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc234"), Right: ([]byte)("abc300")}, nil, []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc130"), Right: ([]byte)("abc140")}, nil, []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)("abc130"), Right: ([]byte)("")}, nil, []byte("abc123"), []byte("abc234"))
	within(Range{Left: ([]byte)(""), Right: ([]byte)("abc130")}, nil, []byte("abc123"), []byte("abc234"))

	outside(Range{}, "abd", "abe", "ad")
	outside(Range{}, "abd", "ac", "ad")
	outside(Range{}, "abd", "b", "e")
	outside(Range{}, "abd", "a", "ab")
	outside(Range{}, "abd", "ab", "abc")
	outside(Range{}, "abd", "ab", "abc123")
	outside(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc122")}, "abc", "abc123", "abc234")
	outside(Range{Left: ([]byte)("abc235"), Right: ([]byte)("abc300")}, "abc", "abc123", "abc234")
	outside(Range{Left: ([]byte)("abc000"), Right: ([]byte)("abc122")}, "", "abc123", "abc234")
	outside(Range{Left: ([]byte)("abc235"), Right: ([]byte)("abc300")}, "", "abc123", "abc234")
}

// TestPickSortTables tests selecting tables matching iterator filtering options, including the selection order
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
	opt.Range = Range{}.Prefix([]byte("c"))
	opt.inRange = opt.Range
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

	opt.Range = Range{Left: []byte("c"), Right: []byte("d")}
	opt.inRange = opt.Range
	filtered = opt.pickTables(tables)
	require.Equal(t, 3, len(filtered))
	require.Equal(t, filtered[0].Smallest()[:3], []byte("cge"))
	require.Equal(t, filtered[1].Smallest()[:3], []byte("ckr"))
	require.Equal(t, filtered[2].Smallest()[:4], []byte("csfr"))

	opt.Range = Range{Left: []byte("ckr1"), Right: []byte("ckr3")}
	opt.inRange = opt.Range
	filtered = opt.pickTables(tables)
	require.Equal(t, 1, len(filtered))
	require.Equal(t, filtered[0].Smallest()[:3], []byte("ckr"))

	opt.Range = Range{}.Prefix([]byte("aa"))
	opt.inRange = opt.Range
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

// TestIteratePrefix tests iterating keys having a particular prefix
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

		countKeys := func(prefix, start, end string, strictness RangeStrictness) int {
			var count int
			opt := DefaultIteratorOptions
			opt.Prefix = []byte(prefix)
			opt.Range = Range{Left: []byte(start), Right: []byte(end), Strictness: strictness}

			t.Logf("Testing with prefix: %s and range %v", prefix, opt.Range)

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
			num := fmt.Sprintf("%d", i)

			s := strings.Repeat(num, 1)
			require.Equal(t, 1000, countKeys(s, "", "", RangeStrictnessInexact))
			require.Equal(t, 100, countKeys(s, s+"0", s+"0", RangeStrictnessInexact))
			require.Equal(t, 500, countKeys(s, s+"0", s+"4", RangeStrictnessInexact))
			require.Equal(t, 500, countKeys(s, s+"0", s+"5", RangeStrictnessExact))
			require.Equal(t, 500, countKeys(s, s+"0", s+"500", RangeStrictnessExclusive))

			s = strings.Repeat(num, 2)
			require.Equal(t, 100, countKeys(s, "", "", RangeStrictnessInexact))
			require.Equal(t, 10, countKeys(s, s+"0", s+"0", RangeStrictnessInexact))
			require.Equal(t, 50, countKeys(s, s+"0", s+"4", RangeStrictnessInexact))
			require.Equal(t, 50, countKeys(s, s+"0", s+"5", RangeStrictnessExact))
			require.Equal(t, 50, countKeys(s, s+"0", s+"50", RangeStrictnessExclusive))

			s = strings.Repeat(num, 3)
			require.Equal(t, 10, countKeys(s, "", "", RangeStrictnessInexact))
			require.Equal(t, 1, countKeys(s, s+"0", s+"0", RangeStrictnessInexact))
			require.Equal(t, 5, countKeys(s, s+"0", s+"4", RangeStrictnessInexact))
			require.Equal(t, 5, countKeys(s, s+"0", s+"4", RangeStrictnessExact))
			require.Equal(t, 5, countKeys(s, s+"0", s+"5", RangeStrictnessExclusive))

			s = strings.Repeat(num, 4)
			require.Equal(t, 1, countKeys(s, "", "", RangeStrictnessInexact))
			require.Equal(t, 0, countKeys(s, s+"0", s+"0", RangeStrictnessInexact))
			require.Equal(t, 0, countKeys(s, s+"0", s+"4", RangeStrictnessInexact))
			require.Equal(t, 0, countKeys(s, s+"0", s+"4", RangeStrictnessExact))
			require.Equal(t, 0, countKeys(s, s+"0", s+"4", RangeStrictnessExclusive))
		}
		require.Equal(t, 10000, countKeys("", "", "", RangeStrictnessInexact))

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
	b.Logf("Key splits: %v", db.Ranges(Range{}, 10000))
	b.Logf("Key splits with prefix: %v", db.Ranges(Range{}.Prefix([]byte("09")), 10000))

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
