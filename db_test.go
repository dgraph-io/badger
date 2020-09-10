/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/skl"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

var mmap = flag.Bool("vlog_mmap", true, "Specify if value log must be memory-mapped")

// summary is produced when DB is closed. Currently it is used only for testing.
type summary struct {
	fileIDs map[uint64]bool
}

func (s *levelsController) getSummary() *summary {
	out := &summary{
		fileIDs: make(map[uint64]bool),
	}
	for _, l := range s.levels {
		l.getSummary(out)
	}
	return out
}

func (s *levelHandler) getSummary(sum *summary) {
	s.RLock()
	defer s.RUnlock()
	for _, t := range s.tables {
		sum.fileIDs[t.ID()] = true
	}
}

func (s *DB) validate() error { return s.lc.validate() }

func getTestOptions(dir string) Options {
	opt := DefaultOptions(dir).
		WithMaxTableSize(1 << 15). // Force more compaction.
		WithLevelOneSize(4 << 15). // Force more compaction.
		WithSyncWrites(false)
	if !*mmap {
		return opt.WithValueLogLoadingMode(options.FileIO)
	}
	return opt
}

func getItemValue(t *testing.T, item *Item) (val []byte) {
	t.Helper()
	var v []byte
	size := item.ValueSize()
	err := item.Value(func(val []byte) error {
		if val == nil {
			v = nil
		} else {
			v = append([]byte{}, val...)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	if int64(len(v)) != size {
		t.Errorf("incorrect size: expected %d, got %d", len(v), size)
	}
	if v == nil {
		return nil
	}
	another, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, v, another)
	return v
}

func txnSet(t *testing.T, kv *DB, key []byte, val []byte, meta byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.SetEntry(NewEntry(key, val).WithMeta(meta)))
	require.NoError(t, txn.Commit())
}

func txnDelete(t *testing.T, kv *DB, key []byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.Delete(key))
	require.NoError(t, txn.Commit())
}

// Opens a badger db and runs a a test on it.
func runBadgerTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	if opts == nil {
		opts = new(Options)
		*opts = getTestOptions(dir)
	} else {
		opts.Dir = dir
		opts.ValueDir = dir
	}
	db, err := Open(*opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	test(t, db)
}

func TestWrite(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
		}
	})
}

func TestUpdateAndView(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		err := db.Update(func(txn *Txn) error {
			for i := 0; i < 10; i++ {
				entry := NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(txn *Txn) error {
			for i := 0; i < 10; i++ {
				item, err := txn.Get([]byte(fmt.Sprintf("key%d", i)))
				if err != nil {
					return err
				}

				expected := []byte(fmt.Sprintf("val%d", i))
				if err := item.Value(func(val []byte) error {
					require.Equal(t, expected, val,
						"Invalid value for key %q. expected: %q, actual: %q",
						item.Key(), expected, val)
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	})
}

func TestConcurrentWrite(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Not a benchmark. Just a simple test for concurrent writes.
		n := 20
		m := 500
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < m; j++ {
					txnSet(t, db, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
						[]byte(fmt.Sprintf("v%05d_%08d", i, j)), byte(j%127))
				}
			}(i)
		}
		wg.Wait()

		t.Log("Starting iteration")

		opt := IteratorOptions{}
		opt.Reverse = false
		opt.PrefetchSize = 10
		opt.PrefetchValues = true

		txn := db.NewTransaction(true)
		it := txn.NewIterator(opt)
		defer it.Close()
		var i, j int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if k == nil {
				break // end of iteration.
			}

			require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), string(k))
			v := getItemValue(t, item)
			require.EqualValues(t, fmt.Sprintf("v%05d_%08d", i, j), string(v))
			require.Equal(t, item.UserMeta(), byte(j%127))
			j++
			if j == m {
				i++
				j = 0
			}
		}
		require.EqualValues(t, n, i)
		require.EqualValues(t, 0, j)
	})
}

func TestGet(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("key1"), []byte("val1"), 0x08)

		txn := db.NewTransaction(false)
		item, err := txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val1", getItemValue(t, item))
		require.Equal(t, byte(0x08), item.UserMeta())
		txn.Discard()

		txnSet(t, db, []byte("key1"), []byte("val2"), 0x09)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val2", getItemValue(t, item))
		require.Equal(t, byte(0x09), item.UserMeta())
		txn.Discard()

		txnDelete(t, db, []byte("key1"))

		txn = db.NewTransaction(false)
		_, err = txn.Get([]byte("key1"))
		require.Equal(t, ErrKeyNotFound, err)
		txn.Discard()

		txnSet(t, db, []byte("key1"), []byte("val3"), 0x01)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, "val3", getItemValue(t, item))
		require.Equal(t, byte(0x01), item.UserMeta())

		longVal := make([]byte, 1000)
		txnSet(t, db, []byte("key1"), longVal, 0x00)

		txn = db.NewTransaction(false)
		item, err = txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.EqualValues(t, longVal, getItemValue(t, item))
		txn.Discard()
	})
}

func TestGetAfterDelete(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// populate with one entry
		key := []byte("key")
		txnSet(t, db, key, []byte("val1"), 0x00)
		require.NoError(t, db.Update(func(txn *Txn) error {
			err := txn.Delete(key)
			require.NoError(t, err)

			_, err = txn.Get(key)
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		}))
	})
}

func TestTxnTooBig(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		data := func(i int) []byte {
			return []byte(fmt.Sprintf("%b", i))
		}
		//	n := 500000
		n := 1000
		txn := db.NewTransaction(true)
		for i := 0; i < n; {
			if err := txn.SetEntry(NewEntry(data(i), data(i))); err != nil {
				require.NoError(t, txn.Commit())
				txn = db.NewTransaction(true)
			} else {
				i++
			}
		}
		require.NoError(t, txn.Commit())

		txn = db.NewTransaction(true)
		for i := 0; i < n; {
			if err := txn.Delete(data(i)); err != nil {
				require.NoError(t, txn.Commit())
				txn = db.NewTransaction(true)
			} else {
				i++
			}
		}
		require.NoError(t, txn.Commit())
	})
}

func TestForceCompactL0(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 15 << 20
	opts.managedTxns = true
	db, err := Open(opts)
	require.NoError(t, err)

	data := func(i int) []byte {
		return []byte(fmt.Sprintf("%b", i))
	}
	n := 80
	m := 45 // Increasing would cause ErrTxnTooBig
	sz := 32 << 10
	v := make([]byte, sz)
	for i := 0; i < n; i += 2 {
		version := uint64(i)
		txn := db.NewTransactionAt(version, true)
		for j := 0; j < m; j++ {
			require.NoError(t, txn.SetEntry(NewEntry(data(j), v)))
		}
		require.NoError(t, txn.CommitAt(version+1, nil))
	}
	db.Close()

	opts.managedTxns = true
	db, err = Open(opts)
	require.NoError(t, err)
	require.Equal(t, len(db.lc.levels[0].tables), 0)
	require.NoError(t, db.Close())
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return (size >> 20), err
}

// BenchmarkDbGrowth ensures DB does not grow with repeated adds and deletes.
//
// New keys are created with each for-loop iteration. During each
// iteration, the previous for-loop iteration's keys are deleted.
//
// To reproduce continous growth problem due to `badgerMove` keys,
// update `value.go` `discardEntry` line 1628 to return false
//
// Also with PR #1303, the delete keys are properly cleaned which
// further reduces disk size.
func BenchmarkDbGrowth(b *testing.B) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(b, err)
	defer removeDir(dir)

	start := 0
	lastStart := 0
	numKeys := 2000
	valueSize := 1024
	value := make([]byte, valueSize)

	discardRatio := 0.001
	maxWrites := 200
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 64 << 15
	opts.MaxTableSize = 4 << 15
	opts.LevelOneSize = 16 << 15
	opts.NumVersionsToKeep = 1
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	db, err := Open(opts)
	require.NoError(b, err)
	for numWrites := 0; numWrites < maxWrites; numWrites++ {
		txn := db.NewTransaction(true)
		if start > 0 {
			for i := lastStart; i < start; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key[:], uint64(i))
				err := txn.Delete(key)
				if err == ErrTxnTooBig {
					require.NoError(b, txn.Commit())
					txn = db.NewTransaction(true)
				} else {
					require.NoError(b, err)
				}
			}
		}

		for i := start; i < numKeys+start; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key[:], uint64(i))
			err := txn.SetEntry(NewEntry(key, value))
			if err == ErrTxnTooBig {
				require.NoError(b, txn.Commit())
				txn = db.NewTransaction(true)
			} else {
				require.NoError(b, err)
			}
		}
		require.NoError(b, txn.Commit())
		require.NoError(b, db.Flatten(1))
		for {
			err = db.RunValueLogGC(discardRatio)
			if err == ErrNoRewrite {
				break
			} else {
				require.NoError(b, err)
			}
		}
		size, err := dirSize(dir)
		require.NoError(b, err)
		fmt.Printf("Badger DB Size = %dMB\n", size)
		lastStart = start
		start += numKeys
	}

	db.Close()
	size, err := dirSize(dir)
	require.NoError(b, err)
	require.LessOrEqual(b, size, int64(16))
	fmt.Printf("Badger DB Size = %dMB\n", size)
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestGetMore(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		data := func(i int) []byte {
			return []byte(fmt.Sprintf("%b", i))
		}
		//	n := 500000
		n := 10000
		m := 45 // Increasing would cause ErrTxnTooBig
		for i := 0; i < n; i += m {
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.SetEntry(NewEntry(data(j), data(j))))
			}
			require.NoError(t, txn.Commit())
		}
		require.NoError(t, db.validate())

		for i := 0; i < n; i++ {
			txn := db.NewTransaction(false)
			item, err := txn.Get(data(i))
			if err != nil {
				t.Error(err)
			}
			require.EqualValues(t, string(data(i)), string(getItemValue(t, item)))
			txn.Discard()
		}

		// Overwrite
		for i := 0; i < n; i += m {
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.SetEntry(NewEntry(data(j),
					// Use a long value that will certainly exceed value threshold.
					[]byte(fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", j)))))
			}
			require.NoError(t, txn.Commit())
		}
		require.NoError(t, db.validate())

		for i := 0; i < n; i++ {
			expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", i)
			k := data(i)
			txn := db.NewTransaction(false)
			item, err := txn.Get(k)
			if err != nil {
				t.Error(err)
			}
			got := string(getItemValue(t, item))
			if expectedValue != got {

				vs, err := db.get(y.KeyWithTs(k, math.MaxUint64))
				require.NoError(t, err)
				fmt.Printf("wanted=%q Item: %s\n", k, item)
				fmt.Printf("on re-run, got version: %+v\n", vs)

				txn := db.NewTransaction(false)
				itr := txn.NewIterator(DefaultIteratorOptions)
				for itr.Seek(k); itr.Valid(); itr.Next() {
					item := itr.Item()
					fmt.Printf("item=%s\n", item)
					if !bytes.Equal(item.Key(), k) {
						break
					}
				}
				itr.Close()
				txn.Discard()
			}
			require.EqualValues(t, expectedValue, string(getItemValue(t, item)), "wanted=%q Item: %s\n", k, item)
			txn.Discard()
		}

		// "Delete" key.
		for i := 0; i < n; i += m {
			if (i % 10000) == 0 {
				fmt.Printf("Deleting i=%d\n", i)
			}
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.Delete(data(j)))
			}
			require.NoError(t, txn.Commit())
		}
		db.validate()
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				// Display some progress. Right now, it's not very fast with no caching.
				fmt.Printf("Testing i=%d\n", i)
			}
			k := data(i)
			txn := db.NewTransaction(false)
			_, err := txn.Get([]byte(k))
			require.Equal(t, ErrKeyNotFound, err, "should not have found k: %q", k)
			txn.Discard()
		}
	})
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestExistsMore(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		//	n := 500000
		n := 10000
		m := 45
		for i := 0; i < n; i += m {
			if (i % 1000) == 0 {
				t.Logf("Putting i=%d\n", i)
			}
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("%09d", j)),
					[]byte(fmt.Sprintf("%09d", j)))))
			}
			require.NoError(t, txn.Commit())
		}
		db.validate()

		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)
			require.NoError(t, db.View(func(txn *Txn) error {
				_, err := txn.Get([]byte(k))
				require.NoError(t, err)
				return nil
			}))
		}
		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("non-exists"))
			require.Error(t, err)
			return nil
		}))

		// "Delete" key.
		for i := 0; i < n; i += m {
			if (i % 1000) == 0 {
				fmt.Printf("Deleting i=%d\n", i)
			}
			txn := db.NewTransaction(true)
			for j := i; j < i+m && j < n; j++ {
				require.NoError(t, txn.Delete([]byte(fmt.Sprintf("%09d", j))))
			}
			require.NoError(t, txn.Commit())
		}
		db.validate()
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				// Display some progress. Right now, it's not very fast with no caching.
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)

			require.NoError(t, db.View(func(txn *Txn) error {
				_, err := txn.Get([]byte(k))
				require.Error(t, err)
				return nil
			}))
		}
		fmt.Println("Done and closing")
	})
}

func TestIterate2Basic(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%09d", i))
		}
		bval := func(i int) []byte {
			return []byte(fmt.Sprintf("%025d", i))
		}

		// n := 500000
		n := 10000
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			txnSet(t, db, bkey(i), bval(i), byte(i%127))
		}

		opt := IteratorOptions{}
		opt.PrefetchValues = true
		opt.PrefetchSize = 10

		txn := db.NewTransaction(false)
		it := txn.NewIterator(opt)
		{
			var count int
			rewind := true
			t.Log("Starting first basic iteration")
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()
				if rewind && count == 5000 {
					// Rewind would skip /head/ key, and it.Next() would skip 0.
					count = 1
					it.Rewind()
					t.Log("Rewinding from 5000 to zero.")
					rewind = false
					continue
				}
				require.EqualValues(t, bkey(count), string(key))
				val := getItemValue(t, item)
				require.EqualValues(t, bval(count), string(val))
				require.Equal(t, byte(count%127), item.UserMeta())
				count++
			}
			require.EqualValues(t, n, count)
		}

		{
			t.Log("Starting second basic iteration")
			idx := 5030
			for it.Seek(bkey(idx)); it.Valid(); it.Next() {
				item := it.Item()
				require.EqualValues(t, bkey(idx), string(item.Key()))
				require.EqualValues(t, bval(idx), string(getItemValue(t, item)))
				idx++
			}
		}
		it.Close()
	})
}

func TestLoad(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	n := 10000
	{
		kv, err := Open(getTestOptions(dir))
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%09d", i))
			txnSet(t, kv, k, k, 0x00)
		}
		kv.Close()
	}

	kv, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	require.Equal(t, uint64(10001), kv.orc.readTs())

	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.NoError(t, kv.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(k))
			require.NoError(t, err)
			require.EqualValues(t, k, string(getItemValue(t, item)))
			return nil
		}))
	}
	kv.Close()
	summary := kv.lc.getSummary()

	// Check that files are garbage collected.
	idMap := getIDMap(dir)
	for fileID := range idMap {
		// Check that name is in summary.filenames.
		require.True(t, summary.fileIDs[fileID], "%d", fileID)
	}
	require.EqualValues(t, len(idMap), len(summary.fileIDs))

	var fileIDs []uint64
	for k := range summary.fileIDs { // Map to array.
		fileIDs = append(fileIDs, k)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
}

func TestIterateDeleted(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("Key1"), []byte("Value1"), 0x00)
		txnSet(t, db, []byte("Key2"), []byte("Value2"), 0x00)

		iterOpt := DefaultIteratorOptions
		iterOpt.PrefetchValues = false
		txn := db.NewTransaction(false)
		idxIt := txn.NewIterator(iterOpt)
		defer idxIt.Close()

		count := 0
		txn2 := db.NewTransaction(true)
		prefix := []byte("Key")
		for idxIt.Seek(prefix); idxIt.ValidForPrefix(prefix); idxIt.Next() {
			key := idxIt.Item().Key()
			count++
			newKey := make([]byte, len(key))
			copy(newKey, key)
			require.NoError(t, txn2.Delete(newKey))
		}
		require.Equal(t, 2, count)
		require.NoError(t, txn2.Commit())

		for _, prefetch := range [...]bool{true, false} {
			t.Run(fmt.Sprintf("Prefetch=%t", prefetch), func(t *testing.T) {
				txn := db.NewTransaction(false)
				iterOpt = DefaultIteratorOptions
				iterOpt.PrefetchValues = prefetch
				idxIt = txn.NewIterator(iterOpt)

				var estSize int64
				var idxKeys []string
				for idxIt.Seek(prefix); idxIt.Valid(); idxIt.Next() {
					item := idxIt.Item()
					key := item.Key()
					estSize += item.EstimatedSize()
					if !bytes.HasPrefix(key, prefix) {
						break
					}
					idxKeys = append(idxKeys, string(key))
					t.Logf("%+v\n", idxIt.Item())
				}
				require.Equal(t, 0, len(idxKeys))
				require.Equal(t, int64(0), estSize)
			})
		}
	})
}

func TestIterateParallel(t *testing.T) {
	key := func(account int) []byte {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(account))
		return append([]byte("account-"), b[:]...)
	}

	N := 100000
	iterate := func(txn *Txn, wg *sync.WaitGroup) {
		defer wg.Done()
		itr := txn.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		var count int
		for itr.Rewind(); itr.Valid(); itr.Next() {
			count++
			item := itr.Item()
			require.Equal(t, "account-", string(item.Key()[0:8]))
			err := item.Value(func(val []byte) error {
				require.Equal(t, "1000", string(val))
				return nil
			})
			require.NoError(t, err)
		}
		require.Equal(t, N, count)
		itr.Close() // Double close.
	}

	opt := DefaultOptions("")
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		var txns []*Txn
		for i := 0; i < N; i++ {
			wg.Add(1)
			txn := db.NewTransaction(true)
			require.NoError(t, txn.SetEntry(NewEntry(key(i), []byte("1000"))))
			txns = append(txns, txn)
		}
		for _, txn := range txns {
			txn.CommitWith(func(err error) {
				y.Check(err)
				wg.Done()
			})
		}

		wg.Wait()

		// Check that a RW txn can't run multiple iterators.
		txn := db.NewTransaction(true)
		itr := txn.NewIterator(DefaultIteratorOptions)
		require.Panics(t, func() {
			txn.NewIterator(DefaultIteratorOptions)
		})
		require.Panics(t, txn.Discard)
		itr.Close()
		txn.Discard()

		// Run multiple iterators for a RO txn.
		txn = db.NewTransaction(false)
		defer txn.Discard()
		wg.Add(3)
		go iterate(txn, &wg)
		go iterate(txn, &wg)
		go iterate(txn, &wg)
		wg.Wait()
	})
}

func TestDeleteWithoutSyncWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	kv, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	key := []byte("k1")
	// Set a value with size > value threshold so that its written to value log.
	txnSet(t, kv, key, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789FOOBARZOGZOG"), 0x00)
	txnDelete(t, kv, key)
	kv.Close()

	// Reopen KV
	kv, err = Open(DefaultOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	require.NoError(t, kv.View(func(txn *Txn) error {
		_, err := txn.Get(key)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestPidFile(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Reopen database
		_, err := Open(getTestOptions(db.opt.Dir))
		require.Error(t, err)
		require.Contains(t, err.Error(), "Another process is using this Badger database")
	})
}

func TestInvalidKey(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		err := db.Update(func(txn *Txn) error {
			err := txn.SetEntry(NewEntry([]byte("!badger!head"), nil))
			require.Equal(t, ErrInvalidKey, err)

			err = txn.SetEntry(NewEntry([]byte("!badger!"), nil))
			require.Equal(t, ErrInvalidKey, err)

			err = txn.SetEntry(NewEntry([]byte("!badger"), []byte("BadgerDB")))
			require.NoError(t, err)
			return err
		})
		require.NoError(t, err)

		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte("!badger"))
			if err != nil {
				return err
			}
			require.NoError(t, item.Value(func(val []byte) error {
				require.Equal(t, []byte("BadgerDB"), val)
				return nil
			}))
			return nil
		}))
	})
}

func TestIteratorPrefetchSize(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%09d", i))
		}
		bval := func(i int) []byte {
			return []byte(fmt.Sprintf("%025d", i))
		}

		n := 100
		for i := 0; i < n; i++ {
			// if (i % 10) == 0 {
			// 	t.Logf("Put i=%d\n", i)
			// }
			txnSet(t, db, bkey(i), bval(i), byte(i%127))
		}

		getIteratorCount := func(prefetchSize int) int {
			opt := IteratorOptions{}
			opt.PrefetchValues = true
			opt.PrefetchSize = prefetchSize

			var count int
			txn := db.NewTransaction(false)
			it := txn.NewIterator(opt)
			{
				t.Log("Starting first basic iteration")
				for it.Rewind(); it.Valid(); it.Next() {
					count++
				}
				require.EqualValues(t, n, count)
			}
			return count
		}

		var sizes = []int{-10, 0, 1, 10}
		for _, size := range sizes {
			c := getIteratorCount(size)
			require.Equal(t, 100, c)
		}
	})
}

func TestSetIfAbsentAsync(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	kv, _ := Open(getTestOptions(dir))

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}

	f := func(err error) {}

	n := 1000
	for i := 0; i < n; i++ {
		// if (i % 10) == 0 {
		// 	t.Logf("Put i=%d\n", i)
		// }
		txn := kv.NewTransaction(true)
		_, err = txn.Get(bkey(i))
		require.Equal(t, ErrKeyNotFound, err)
		require.NoError(t, txn.SetEntry(NewEntry(bkey(i), nil).WithMeta(byte(i%127))))
		txn.CommitWith(f)
	}

	require.NoError(t, kv.Close())
	kv, err = Open(getTestOptions(dir))
	require.NoError(t, err)

	opt := DefaultIteratorOptions
	txn := kv.NewTransaction(false)
	var count int
	it := txn.NewIterator(opt)
	{
		t.Log("Starting first basic iteration")
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		require.EqualValues(t, n, count)
	}
	require.Equal(t, n, count)
	require.NoError(t, kv.Close())
}

func TestGetSetRace(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {

		data := make([]byte, 4096)
		_, err := rand.Read(data)
		require.NoError(t, err)

		var (
			numOp = 100
			wg    sync.WaitGroup
			keyCh = make(chan string)
		)

		// writer
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				close(keyCh)
			}()

			for i := 0; i < numOp; i++ {
				key := fmt.Sprintf("%d", i)
				txnSet(t, db, []byte(key), data, 0x00)
				keyCh <- key
			}
		}()

		// reader
		wg.Add(1)
		go func() {
			defer wg.Done()

			for key := range keyCh {
				require.NoError(t, db.View(func(txn *Txn) error {
					item, err := txn.Get([]byte(key))
					require.NoError(t, err)
					err = item.Value(nil)
					require.NoError(t, err)
					return nil
				}))
			}
		}()

		wg.Wait()
	})
}

func TestDiscardVersionsBelow(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Write 4 versions of the same key
		for i := 0; i < 4; i++ {
			err := db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte("answer"), []byte(fmt.Sprintf("%d", i))))
			})
			require.NoError(t, err)
		}

		opts := DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false

		// Verify that there are 4 versions, and record 3rd version (2nd from top in iteration)
		db.View(func(txn *Txn) error {
			it := txn.NewIterator(opts)
			defer it.Close()
			var count int
			for it.Rewind(); it.Valid(); it.Next() {
				count++
				item := it.Item()
				require.Equal(t, []byte("answer"), item.Key())
				if item.DiscardEarlierVersions() {
					break
				}
			}
			require.Equal(t, 4, count)
			return nil
		})

		// Set new version and discard older ones.
		err := db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte("answer"), []byte("5")).WithDiscard())
		})
		require.NoError(t, err)

		// Verify that there are only 2 versions left, and versions
		// below ts have been deleted.
		db.View(func(txn *Txn) error {
			it := txn.NewIterator(opts)
			defer it.Close()
			var count int
			for it.Rewind(); it.Valid(); it.Next() {
				count++
				item := it.Item()
				require.Equal(t, []byte("answer"), item.Key())
				if item.DiscardEarlierVersions() {
					break
				}
			}
			require.Equal(t, 1, count)
			return nil
		})
	})
}

func TestExpiry(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Write two keys, one with a TTL
		err := db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte("answer1"), []byte("42")))
		})
		require.NoError(t, err)

		err = db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte("answer2"), []byte("43")).WithTTL(1 * time.Second))
		})
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		// Verify that only unexpired key is found during iteration
		err = db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("answer1"))
			require.NoError(t, err)

			_, err = txn.Get([]byte("answer2"))
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		})
		require.NoError(t, err)

		// Verify that only one key is found during iteration
		opts := DefaultIteratorOptions
		opts.PrefetchValues = false
		err = db.View(func(txn *Txn) error {
			it := txn.NewIterator(opts)
			defer it.Close()
			var count int
			for it.Rewind(); it.Valid(); it.Next() {
				count++
				item := it.Item()
				require.Equal(t, []byte("answer1"), item.Key())
			}
			require.Equal(t, 1, count)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestExpiryImproperDBClose(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogLoadingMode = options.FileIO

	db0, err := Open(opt)
	require.NoError(t, err)

	dur := 1 * time.Hour
	expiryTime := uint64(time.Now().Add(dur).Unix())
	err = db0.Update(func(txn *Txn) error {
		err = txn.SetEntry(NewEntry([]byte("test_key"), []byte("test_value")).WithTTL(dur))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Simulate a crash  by not closing db0, but releasing the locks.
	if db0.dirLockGuard != nil {
		require.NoError(t, db0.dirLockGuard.release())
	}
	if db0.valueDirGuard != nil {
		require.NoError(t, db0.valueDirGuard.release())
	}
	// We need to close vlog to fix the vlog file size. On windows, the vlog file
	// is truncated to 2*MaxVlogSize and if we don't close the vlog file, reopening
	// it would return Truncate Required Error.
	require.NoError(t, db0.vlog.Close())

	require.NoError(t, db0.manifest.close())

	db1, err := Open(opt)
	require.NoError(t, err)
	err = db1.View(func(txn *Txn) error {
		itm, err := txn.Get([]byte("test_key"))
		require.NoError(t, err)
		require.True(t, expiryTime <= itm.ExpiresAt() && itm.ExpiresAt() <= uint64(time.Now().Add(dur).Unix()),
			"expiry time of entry is invalid")
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db1.Close())

}

func randBytes(n int) []byte {
	recv := make([]byte, n)
	in, err := rand.Read(recv)
	if err != nil {
		panic(err)
	}
	return recv[:in]
}

var benchmarkData = []struct {
	key, value []byte
	success    bool // represent if KV should be inserted successfully or not
}{
	{randBytes(100), nil, true},
	{randBytes(1000), []byte("foo"), true},
	{[]byte("foo"), randBytes(1000), true},
	{[]byte(""), randBytes(1000), false},
	{nil, randBytes(1000000), false},
	{randBytes(100000), nil, false},
	{randBytes(1000000), nil, false},
}

func TestLargeKeys(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir).WithValueLogFileSize(1024 * 1024 * 1024))
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		tx := db.NewTransaction(true)
		for _, kv := range benchmarkData {
			k := make([]byte, len(kv.key))
			copy(k, kv.key)

			v := make([]byte, len(kv.value))
			copy(v, kv.value)
			if err := tx.SetEntry(NewEntry(k, v)); err != nil {
				// check is success should be true
				if kv.success {
					t.Fatalf("failed with: %s", err)
				}
			} else if !kv.success {
				t.Fatal("insertion should fail")
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("#%d: batchSet err: %v", i, err)
		}
	}
	require.NoError(t, db.Close())
}

func TestCreateDirs(t *testing.T) {
	dir, err := ioutil.TempDir("", "parent")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(filepath.Join(dir, "badger")))
	require.NoError(t, err)
	db.Close()
	_, err = os.Stat(dir)
	require.NoError(t, err)
}

func TestGetSetDeadlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	fmt.Println(dir)
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir).WithValueLogFileSize(1 << 20))
	require.NoError(t, err)
	defer db.Close()

	val := make([]byte, 1<<19)
	key := []byte("key1")
	require.NoError(t, db.Update(func(txn *Txn) error {
		rand.Read(val)
		require.NoError(t, txn.SetEntry(NewEntry(key, val)))
		return nil
	}))

	timeout, done := time.After(10*time.Second), make(chan bool)

	go func() {
		db.Update(func(txn *Txn) error {
			item, err := txn.Get(key)
			require.NoError(t, err)
			err = item.Value(nil) // This take a RLock on file
			require.NoError(t, err)

			rand.Read(val)
			require.NoError(t, txn.SetEntry(NewEntry(key, val)))
			require.NoError(t, txn.SetEntry(NewEntry([]byte("key2"), val)))
			return nil
		})
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("db.Update did not finish within 10s, assuming deadlock.")
	case <-done:
		t.Log("db.Update finished.")
	}
}

func TestWriteDeadlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir).WithValueLogFileSize(10 << 20))
	require.NoError(t, err)
	defer db.Close()
	print := func(count *int) {
		*count++
		if *count%100 == 0 {
			fmt.Printf("%05d\r", *count)
		}
	}

	var count int
	val := make([]byte, 10000)
	require.NoError(t, db.Update(func(txn *Txn) error {
		for i := 0; i < 1500; i++ {
			key := fmt.Sprintf("%d", i)
			rand.Read(val)
			require.NoError(t, txn.SetEntry(NewEntry([]byte(key), val)))
			print(&count)
		}
		return nil
	}))

	count = 0
	fmt.Println("\nWrites done. Iteration and updates starting...")
	err = db.Update(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.PrefetchValues = false
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			// Using Value() would cause deadlock.
			// item.Value()
			out, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, len(val), len(out))

			key := y.Copy(item.Key())
			rand.Read(val)
			require.NoError(t, txn.SetEntry(NewEntry(key, val)))
			print(&count)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestSequence(t *testing.T) {
	key0 := []byte("seq0")
	key1 := []byte("seq1")

	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		seq0, err := db.GetSequence(key0, 10)
		require.NoError(t, err)
		seq1, err := db.GetSequence(key1, 100)
		require.NoError(t, err)

		for i := uint64(0); i < uint64(105); i++ {
			num, err := seq0.Next()
			require.NoError(t, err)
			require.Equal(t, i, num)

			num, err = seq1.Next()
			require.NoError(t, err)
			require.Equal(t, i, num)
		}
		err = db.View(func(txn *Txn) error {
			item, err := txn.Get(key0)
			if err != nil {
				return err
			}
			var num0 uint64
			if err := item.Value(func(val []byte) error {
				num0 = binary.BigEndian.Uint64(val)
				return nil
			}); err != nil {
				return err
			}
			require.Equal(t, uint64(110), num0)

			item, err = txn.Get(key1)
			if err != nil {
				return err
			}
			var num1 uint64
			if err := item.Value(func(val []byte) error {
				num1 = binary.BigEndian.Uint64(val)
				return nil
			}); err != nil {
				return err
			}
			require.Equal(t, uint64(200), num1)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestSequence_Release(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// get sequence, use once and release
		key := []byte("key")
		seq, err := db.GetSequence(key, 1000)
		require.NoError(t, err)
		num, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(0), num)
		require.NoError(t, seq.Release())

		// we used up 0 and 1 should be stored now
		err = db.View(func(txn *Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, num+1, binary.BigEndian.Uint64(val))
			return nil
		})
		require.NoError(t, err)

		// using it again will lease 1+1000
		num, err = seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1), num)
		err = db.View(func(txn *Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, uint64(1001), binary.BigEndian.Uint64(val))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestTestSequence2(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("key")
		seq1, err := db.GetSequence(key, 2)
		require.NoError(t, err)

		seq2, err := db.GetSequence(key, 2)
		require.NoError(t, err)
		num, err := seq2.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(2), num)

		require.NoError(t, seq2.Release())
		require.NoError(t, seq1.Release())

		seq3, err := db.GetSequence(key, 2)
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			num2, err := seq3.Next()
			require.NoError(t, err)
			require.Equal(t, uint64(i)+3, num2)
		}

		require.NoError(t, seq3.Release())
	})
}

func TestReadOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opts := getTestOptions(dir)

	// Create the DB
	db, err := Open(opts)
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 0x00)
	}

	// Attempt a read-only open while it's open read-write.
	opts.ReadOnly = true
	_, err = Open(opts)
	require.Error(t, err)
	if err == ErrWindowsNotSupported {
		require.NoError(t, db.Close())
		return
	}
	require.Contains(t, err.Error(), "Another process is using this Badger database")
	db.Close()

	// Open one read-only
	opts.ReadOnly = true
	kv1, err := Open(opts)
	require.NoError(t, err)
	defer kv1.Close()

	// Open another read-only
	kv2, err := Open(opts)
	require.NoError(t, err)
	defer kv2.Close()

	// Attempt a read-write open while it's open for read-only
	opts.ReadOnly = false
	_, err = Open(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Another process is using this Badger database")

	// Get a thing from the DB
	txn1 := kv1.NewTransaction(true)
	v1, err := txn1.Get([]byte("key1"))
	require.NoError(t, err)
	b1, err := v1.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, b1, []byte("value1"))
	err = txn1.Commit()
	require.NoError(t, err)

	// Get a thing from the DB via the other connection
	txn2 := kv2.NewTransaction(true)
	v2, err := txn2.Get([]byte("key2000"))
	require.NoError(t, err)
	b2, err := v2.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, b2, []byte("value2000"))
	err = txn2.Commit()
	require.NoError(t, err)

	// Attempt to set a value on a read-only connection
	txn := kv1.NewTransaction(true)
	err = txn.SetEntry(NewEntry([]byte("key"), []byte("value")))
	require.Error(t, err)
	require.Contains(t, err.Error(), "No sets or deletes are allowed in a read-only transaction")
	err = txn.Commit()
	require.NoError(t, err)
}

func TestLSMOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := LSMOnlyOptions(dir)
	dopts := DefaultOptions(dir)
	require.NotEqual(t, dopts.ValueThreshold, opts.ValueThreshold)

	dopts.ValueThreshold = 1 << 16
	_, err = Open(dopts)
	require.Equal(t, ErrValueThreshold, err)

	opts.ValueLogMaxEntries = 100
	db, err := Open(opts)
	require.NoError(t, err)

	value := make([]byte, 128)
	_, err = rand.Read(value)
	for i := 0; i < 500; i++ {
		require.NoError(t, err)
		txnSet(t, db, []byte(fmt.Sprintf("key%d", i)), value, 0x00)
	}
	require.NoError(t, db.Close()) // Close to force compactions, so Value log GC would run.

	db, err = Open(opts)
	require.NoError(t, err)

	defer db.Close()
	require.NoError(t, db.RunValueLogGC(0.2))
}

// This test function is doing some intricate sorcery.
func TestMinReadTs(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 10; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte("x"), []byte("y")))
			}))
		}
		time.Sleep(time.Millisecond)

		readTxn0 := db.NewTransaction(false)
		require.Equal(t, uint64(10), readTxn0.readTs)

		min := db.orc.readMark.DoneUntil()
		require.Equal(t, uint64(9), min)

		readTxn := db.NewTransaction(false)
		for i := 0; i < 10; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte("x"), []byte("y")))
			}))
		}
		require.Equal(t, uint64(20), db.orc.readTs())

		time.Sleep(time.Millisecond)
		require.Equal(t, min, db.orc.readMark.DoneUntil())

		readTxn0.Discard()
		readTxn.Discard()
		time.Sleep(time.Millisecond)
		require.Equal(t, uint64(19), db.orc.readMark.DoneUntil())
		db.orc.readMark.Done(uint64(20)) // Because we called readTs.

		for i := 0; i < 10; i++ {
			db.View(func(txn *Txn) error {
				return nil
			})
		}
		time.Sleep(time.Millisecond)
		require.Equal(t, uint64(20), db.orc.readMark.DoneUntil())
	})
}

func TestGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	t.Logf("Num go: %d", before)
	for i := 0; i < 12; i++ {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			updated := false
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				err := db.Subscribe(ctx, func(kvs *pb.KVList) error {
					require.Equal(t, []byte("value"), kvs.Kv[0].GetValue())
					updated = true
					wg.Done()
					return nil
				}, []byte("key"))
				if err != nil {
					require.Equal(t, err.Error(), context.Canceled.Error())
				}
			}()
			// Wait for the go routine to be scheduled.
			time.Sleep(time.Second)
			err := db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte("key"), []byte("value")))
			})
			require.NoError(t, err)
			wg.Wait()
			cancel()
			require.Equal(t, true, updated)
		})
	}
	require.Equal(t, before, runtime.NumGoroutine())
}

func ExampleOpen() {
	dir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		panic(err)
	}
	defer removeDir(dir)
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("key"))
		// We expect ErrKeyNotFound
		fmt.Println(err)
		return nil
	})

	if err != nil {
		panic(err)
	}

	txn := db.NewTransaction(true) // Read-write txn
	err = txn.SetEntry(NewEntry([]byte("key"), []byte("value")))
	if err != nil {
		panic(err)
	}
	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	err = db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", string(val))
		return nil
	})

	if err != nil {
		panic(err)
	}

	// Output:
	// Key not found
	// value
}

func ExampleTxn_NewIterator() {
	dir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		panic(err)
	}
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	txn := db.NewTransaction(true)

	// Fill in 1000 items
	n := 1000
	for i := 0; i < n; i++ {
		err := txn.SetEntry(NewEntry(bkey(i), bval(i)))
		if err != nil {
			panic(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	opt := DefaultIteratorOptions
	opt.PrefetchSize = 10

	// Iterate over 1000 items
	var count int
	err = db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Counted %d elements", count)
	// Output:
	// Counted 1000 elements
}

func TestSyncForRace(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir).WithSyncWrites(false))
	require.NoError(t, err)
	defer db.Close()

	closeChan := make(chan struct{})
	doneChan := make(chan struct{})

	go func() {
		ticker := time.NewTicker(100 * time.Microsecond)
		for {
			select {
			case <-ticker.C:
				if err := db.Sync(); err != nil {
					require.NoError(t, err)
				}
				db.opt.Debugf("Sync Iteration completed")
			case <-closeChan:
				close(doneChan)
				return
			}
		}
	}()

	sz := 128 << 10 // 5 entries per value log file.
	v := make([]byte, sz)
	rand.Read(v[:rand.Intn(sz)])
	txn := db.NewTransaction(true)
	for i := 0; i < 10000; i++ {
		require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%d", i)), v)))
		if i%3 == 0 {
			require.NoError(t, txn.Commit())
			txn = db.NewTransaction(true)
		}
		if i%100 == 0 {
			db.opt.Debugf("next 100 entries added to DB")
		}
	}
	require.NoError(t, txn.Commit())

	close(closeChan)
	<-doneChan
}

// Earlier, if head is not pointing to latest Vlog file, then at replay badger used to crash with
// index out of range panic. After fix in this commit it should not.
func TestNoCrash(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err, "cannot create badger dir")
	defer removeDir(dir)

	ops := getTestOptions(dir)
	ops.ValueLogMaxEntries = 1
	db, err := Open(ops)
	require.NoError(t, err, "unable to open db")

	// entering 100 entries will generate 100 vlog files
	for i := 0; i < 100; i++ {
		err := db.Update(func(txn *Txn) error {
			entry := NewEntry([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("val-%d", i)))
			return txn.SetEntry(entry)
		})
		require.NoError(t, err, "update to db failed")
	}

	db.Lock()
	// make head to point to second file. We cannot make it point to the first
	// vlog file because we cannot push a zero head pointer.
	db.vhead = valuePointer{1, 0, 0}
	db.Unlock()
	db.Close()

	// reduce size of SSTable to flush early
	ops.MaxTableSize = 1 << 10
	db, err = Open(ops)
	require.Nil(t, err, "error while opening db")
	require.NoError(t, db.Close())
}

func TestForceFlushMemtable(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err, "temp dir for badger count not be created")

	ops := getTestOptions(dir)
	ops.ValueLogMaxEntries = 1
	ops.LogRotatesToFlush = 1

	db, err := Open(ops)
	require.NoError(t, err, "error while openning db")
	defer db.Close()

	for i := 0; i < 3; i++ {
		err = db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key-%d", i)),
				[]byte(fmt.Sprintf("value-%d", i))))
		})
		require.NoError(t, err, "unable to set key and value")
	}
	time.Sleep(1 * time.Second)

	// We want to make sure that memtable is flushed on disk. While flushing memtable to disk,
	// latest head is also stored in it. Hence we will try to read head from disk. To make sure
	// this. we will truncate all memtables.
	db.Lock()
	db.mt.DecrRef()
	for _, mt := range db.imm {
		mt.DecrRef()
	}
	db.imm = db.imm[:0]
	db.mt = skl.NewSkiplist(arenaSize(db.opt)) // Set it up for future writes.
	db.Unlock()

	// get latest value of value log head
	headKey := y.KeyWithTs(head, math.MaxUint64)
	vs, err := db.get(headKey)
	require.NoError(t, err)
	var vptr valuePointer
	vptr.Decode(vs.Value)
	// Since we are inserting 3 entries and ValueLogMaxEntries is 1, there will be 3 rotation. For
	// 1st and 2nd time head flushed with memtable will have fid as 0 and last time it will be 1.
	require.True(t, vptr.Fid == 1, fmt.Sprintf("expected fid: %d, actual fid: %d", 1, vptr.Fid))
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}
