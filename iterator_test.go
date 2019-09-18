/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

type tableMock struct {
	left, right []byte
}

func (tm *tableMock) Smallest() []byte             { return tm.left }
func (tm *tableMock) Biggest() []byte              { return tm.right }
func (tm *tableMock) DoesNotHave(hash uint64) bool { return false }

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
			f := buildTable(t, [][]string{{mk.small, "some value"}, {mk.large, "some value"}})
			opts := table.Options{LoadingMode: options.MemoryMap,
				ChkMode: options.OnTableAndBlockRead}
			tbl, err := table.OpenTable(f, opts)
			require.NoError(t, err)
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

func TestIteratePrefix(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
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
	})
}

// go test -v -run=XXX -bench=BenchmarkIterate -benchtime=3s
// Benchmark with opt.Prefix set ===
// goos: linux
// goarch: amd64
// pkg: github.com/dgraph-io/badger
// BenchmarkIteratePrefixSingleKey/Key_lookups-4         	   10000	    365539 ns/op
// --- BENCH: BenchmarkIteratePrefixSingleKey/Key_lookups-4
// 	iterator_test.go:147: Inner b.N: 1
// 	iterator_test.go:147: Inner b.N: 100
// 	iterator_test.go:147: Inner b.N: 10000
// --- BENCH: BenchmarkIteratePrefixSingleKey
// 	iterator_test.go:143: LSM files: 79
// 	iterator_test.go:145: Outer b.N: 1
// PASS
// ok  	github.com/dgraph-io/badger	41.586s
//
// Benchmark with NO opt.Prefix set ===
// goos: linux
// goarch: amd64
// pkg: github.com/dgraph-io/badger
// BenchmarkIteratePrefixSingleKey/Key_lookups-4         	   10000	    460924 ns/op
// --- BENCH: BenchmarkIteratePrefixSingleKey/Key_lookups-4
// 	iterator_test.go:147: Inner b.N: 1
// 	iterator_test.go:147: Inner b.N: 100
// 	iterator_test.go:147: Inner b.N: 10000
// --- BENCH: BenchmarkIteratePrefixSingleKey
// 	iterator_test.go:143: LSM files: 83
// 	iterator_test.go:145: Outer b.N: 1
// PASS
// ok  	github.com/dgraph-io/badger	41.836s
//
// Only my laptop there's a 20% improvement in latency with ~80 files.
func BenchmarkIteratePrefixSingleKey(b *testing.B) {
	dir, err := ioutil.TempDir(".", "badger-test")
	y.Check(err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.TableLoadingMode = options.LoadToRAM
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
	b.Logf("Key splits: %v", db.KeySplits(nil))
	b.Logf("Key splits with prefix: %v", db.KeySplits([]byte("09")))

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
