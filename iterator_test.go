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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v3/strie"
	"github.com/dgraph-io/badger/v3/threetrie"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type tableMock struct {
	left, right []byte
}

func (tm *tableMock) Smallest() []byte             { return tm.left }
func (tm *tableMock) Biggest() []byte              { return tm.right }
func (tm *tableMock) DoesNotHave(hash uint32) bool { return false }
func (tm *tableMock) MaxVersion() uint64           { return math.MaxUint64 }

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

// func TestPickSortTables(t *testing.T) {
// 	type MockKeys struct {
// 		small string
// 		large string
// 	}
// 	genTables := func(mks ...MockKeys) []*table.Table {
// 		out := make([]*table.Table, 0)
// 		for _, mk := range mks {
// 			opts := table.Options{ChkMode: options.OnTableAndBlockRead}
// 			tbl := buildTable(t, [][]string{{mk.small, "some value"},
// 				{mk.large, "some value"}}, opts)
// 			out = append(out, tbl)
// 		}
// 		return out
// 	}
// 	tables := genTables(MockKeys{small: "a", large: "abc"},
// 		MockKeys{small: "abcd", large: "cde"},
// 		MockKeys{small: "cge", large: "chf"},
// 		MockKeys{small: "glr", large: "gyup"})
// 	opt := DefaultIteratorOptions
// 	opt.prefixIsKey = false
// 	opt.Prefix = []byte("c")
// 	filtered := opt.pickTables(tables)
// 	require.Equal(t, 2, len(filtered))
// 	// build table adds time stamp so removing tailing bytes.
// 	require.Equal(t, filtered[0].Smallest()[:4], []byte("abcd"))
// 	require.Equal(t, filtered[1].Smallest()[:3], []byte("cge"))
// 	tables = genTables(MockKeys{small: "a", large: "abc"},
// 		MockKeys{small: "abcd", large: "ade"},
// 		MockKeys{small: "cge", large: "chf"},
// 		MockKeys{small: "glr", large: "gyup"})
// 	filtered = opt.pickTables(tables)
// 	require.Equal(t, 1, len(filtered))
// 	require.Equal(t, filtered[0].Smallest()[:3], []byte("cge"))
// 	tables = genTables(MockKeys{small: "a", large: "abc"},
// 		MockKeys{small: "abcd", large: "ade"},
// 		MockKeys{small: "cge", large: "chf"},
// 		MockKeys{small: "ckr", large: "cyup"},
// 		MockKeys{small: "csfr", large: "gyup"})
// 	filtered = opt.pickTables(tables)
// 	require.Equal(t, 3, len(filtered))
// 	require.Equal(t, filtered[0].Smallest()[:3], []byte("cge"))
// 	require.Equal(t, filtered[1].Smallest()[:3], []byte("ckr"))
// 	require.Equal(t, filtered[2].Smallest()[:4], []byte("csfr"))

// 	opt.Prefix = []byte("aa")
// 	filtered = opt.pickTables(tables)
// 	require.Equal(t, y.ParseKey(filtered[0].Smallest()), []byte("a"))
// 	require.Equal(t, y.ParseKey(filtered[0].Biggest()), []byte("abc"))
// }

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

		db.View(func(txn *Txn) error {
			it := txn.NewIterator(iopt)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				i := it.Item()
				require.GreaterOrEqual(t, i.Version(), sinceTs)
			}
			return nil
		})

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
	dir, err := ioutil.TempDir(".", "badger-test")
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

var Idx int

type tableEntry []byte

// type tableEntry struct {
// 	key   []byte  // smallest/biggest key of the table
// }

func tableEntrySize(key []byte) int {
	return 4 + len(key) // ptr + keySz + len(key)
}

func marshalTableEntry(dst []byte, key []byte) {
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(key)))

	n := copy(dst[4:], key)
	y.AssertTrue(len(dst) == 4+n)
}

func (me tableEntry) Size() int {
	return len(me)
}

func (me tableEntry) Ptr() uint64 {
	return binary.BigEndian.Uint64(me[0:8])
}

func (me tableEntry) Key() []byte {
	sz := binary.BigEndian.Uint32(me[0:4])
	return me[4 : 4+sz]
}

// go test -run=^$ -test.bench=BenchmarkGetBiggest -count=10 [N=100, M=100, keySz=40]
// name                               time/op
// GetBiggest/master-16               1.00ms ± 1%
// GetBiggest/slice-iterate-16        6.21ms ± 0%
// GetBiggest/badger-trie-16          9.65ms ± 1%
// GetBiggest/dgraph-trie-16          1.99ms ± 0%
// GetBiggest/dgraph-trie-shuffle-16  1.34ms ± 2%

// Runs benchmark for picktables by mocking the randomly generated keys as tables' biggest.
// N keys of size keySz are generated randomly.
// N*M keys are read. Each of the interval gets mul keys. Last 10-30 bytes of the keys are
// randomly generated. While the first 10 are same as the table's biggest.
func BenchmarkGetBiggest(b *testing.B) {
	N := 100
	M := 100

	// Generate tables' biggest.
	keySz := 40
	keys := make([][]byte, 0, N)
	for i := 0; i < N; i++ {
		key := make([]byte, keySz)
		_, err := rand.Read(key)
		require.NoError(b, err)
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Generate read keys.
	readKeys := make([][]byte, 0, N*M)
	for _, key := range keys {
		for i := 0; i < M; i++ {
			// Randomize last 10-30 bytes
			cnt := rand.Intn(20) + 10
			rkey := make([]byte, cnt)
			_, err := rand.Read(rkey)
			require.NoError(b, err)
			readKeys = append(readKeys, append(key[:keySz-cnt], rkey...))
		}
	}
	rand.Shuffle(len(readKeys), func(i, j int) {
		readKeys[i], readKeys[j] = readKeys[j], readKeys[i]
	})

	b.Run("master", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var idx int
			for _, key := range readKeys {
				idx = sort.Search(len(keys), func(i int) bool {
					return bytes.Compare(keys[i], key) >= 0
				})
			}
			Idx = idx
		}
	})

	b.Run("slice-iterate", func(b *testing.B) {
		buf := z.NewBuffer(1<<20, "pick table test")
		defer buf.Release()
		for _, key := range keys {
			dst := buf.SliceAllocate(tableEntrySize(key))
			marshalTableEntry(dst, key)
		}
		var ErrStop = errors.New("Stop Iteration")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var idx int
			for _, key := range readKeys {
				buf.SliceIterate(func(slice []byte) error {
					te := tableEntry(slice)
					if bytes.Compare(te.Key(), key) >= 0 {
						return ErrStop
					}
					idx++
					return nil
				})
			}
			Idx = idx
		}
		b.StopTimer()
	})

	b.Run("badger-trie", func(b *testing.B) {
		trie := strie.NewTrie()
		for i, key := range keys {
			trie.Add(key, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var idx int
			for _, key := range readKeys {
				idx = trie.Get(key)
			}
			Idx = idx
		}
	})

	b.Run("dgraph-trie", func(b *testing.B) {
		trie := threetrie.NewTrie()
		for i, key := range keys {
			trie.Put(key, uint64(i))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var idx int
			for _, key := range readKeys {
				idx = trie.Get(key)
			}
			Idx = idx
		}
	})

	b.Run("dgraph-trie-shuffle", func(b *testing.B) {
		trie := threetrie.NewTrie()
		type t struct {
			idx int
			key []byte
		}
		// Shuffle the keys before insertion
		ts := make([]t, N)
		for i := 0; i < N; i++ {
			ts[i] = t{i, keys[i]}
		}
		rand.Shuffle(N, func(i, j int) {
			ts[i], ts[j] = ts[j], ts[i]
		})
		for _, tss := range ts {
			trie.Put(tss.key, uint64(tss.idx))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var idx int
			for _, key := range readKeys {
				idx = trie.Get(key)
			}
			Idx = idx
		}
	})
}
