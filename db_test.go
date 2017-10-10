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
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func getTestOptions(dir string) *Options {
	opt := new(Options)
	*opt = DefaultOptions
	opt.MaxTableSize = 1 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	opt.Dir = dir
	opt.ValueDir = dir
	opt.SyncWrites = false
	return opt
}

func getItemValue(t *testing.T, item *Item) (val []byte) {
	v, err := item.Value()
	if err != nil {
		t.Error(err)
	}
	if v == nil {
		return nil
	}
	return v
}

func txnSet(t *testing.T, kv *DB, key []byte, val []byte, meta byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.Set(key, val, meta))
	require.NoError(t, txn.Commit(nil))
}

func txnDelete(t *testing.T, kv *DB, key []byte) {
	txn := kv.NewTransaction(true)
	require.NoError(t, txn.Delete(key))
	require.NoError(t, txn.Commit(nil))
}

func TestWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	for i := 0; i < 100; i++ {
		txnSet(t, kv, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
	}
}

func TestUpdateAndView(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	err = db.Update(func(txn *Txn) error {
		for i := 0; i < 10; i++ {
			err := txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
			if err != nil {
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

			val, err := item.Value()
			if err != nil {
				return err
			}
			expected := []byte(fmt.Sprintf("val%d", i))
			require.Equal(t, expected, val,
				"Invalid value for key %q. expected: %q, actual: %q",
				item.Key(), expected, val)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestConcurrentWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := Open(getTestOptions(dir))
	defer kv.Close()

	// Not a benchmark. Just a simple test for concurrent writes.
	n := 20
	m := 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < m; j++ {
				txnSet(t, kv, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
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

	txn := kv.NewTransaction(true)
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
}

func TestGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := Open(getTestOptions(dir))
	if err != nil {
		t.Error(err)
	}
	defer kv.Close()
	txnSet(t, kv, []byte("key1"), []byte("val1"), 0x08)

	txn := kv.NewTransaction(false)
	item, err := txn.Get([]byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val1", getItemValue(t, item))
	require.Equal(t, byte(0x08), item.UserMeta())
	txn.Discard()

	txnSet(t, kv, []byte("key1"), []byte("val2"), 0x09)

	txn = kv.NewTransaction(false)
	item, err = txn.Get([]byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val2", getItemValue(t, item))
	require.Equal(t, byte(0x09), item.UserMeta())
	txn.Discard()

	txnDelete(t, kv, []byte("key1"))

	txn = kv.NewTransaction(false)
	item, err = txn.Get([]byte("key1"))
	require.Equal(t, ErrKeyNotFound, err)
	txn.Discard()

	txnSet(t, kv, []byte("key1"), []byte("val3"), 0x01)

	txn = kv.NewTransaction(false)
	item, err = txn.Get([]byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val3", getItemValue(t, item))
	require.Equal(t, byte(0x01), item.UserMeta())

	longVal := make([]byte, 1000)
	txnSet(t, kv, []byte("key1"), longVal, 0x00)

	txn = kv.NewTransaction(false)
	item, err = txn.Get([]byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, longVal, getItemValue(t, item))
	txn.Discard()
}

func TestExists(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := Open(getTestOptions(dir))
	if err != nil {
		t.Error(err)
	}
	defer kv.Close()

	// populate with one entry
	txnSet(t, kv, []byte("key1"), []byte("val1"), 0x00)

	tt := []struct {
		key    []byte
		exists bool
	}{
		{
			key:    []byte("key1"),
			exists: true,
		},
		{
			key:    []byte("non-exits"),
			exists: false,
		},
	}

	for _, test := range tt {
		require.NoError(t, kv.View(func(tx *Txn) error {
			_, err := tx.Get(test.key)
			if test.exists {
				require.NoError(t, err)
				return nil
			}
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		}))
	}

}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestGetMore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := Open(getTestOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	data := func(i int) []byte {
		return []byte(fmt.Sprintf("%b", i))
	}
	//	n := 500000
	n := 10000
	m := 49 // Increasing would cause ErrTxnTooBig
	for i := 0; i < n; i += m {
		txn := kv.NewTransaction(true)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Set(data(j), data(j), 0))
		}
		require.NoError(t, txn.Commit(nil))
	}
	require.NoError(t, kv.validate())

	for i := 0; i < n; i++ {
		txn := kv.NewTransaction(false)
		item, err := txn.Get(data(i))
		if err != nil {
			t.Error(err)
		}
		require.EqualValues(t, string(data(i)), string(getItemValue(t, item)))
		txn.Discard()
	}

	// Overwrite
	for i := 0; i < n; i += m {
		txn := kv.NewTransaction(true)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Set(data(j),
				// Use a long value that will certainly exceed value threshold.
				[]byte(fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", j)),
				0x00))
		}
		require.NoError(t, txn.Commit(nil))
	}
	require.NoError(t, kv.validate())

	for i := 0; i < n; i++ {
		expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%9d", i)
		k := data(i)
		txn := kv.NewTransaction(false)
		item, err := txn.Get(k)
		if err != nil {
			t.Error(err)
		}
		got := string(getItemValue(t, item))
		if expectedValue != got {

			vs, err := kv.get(y.KeyWithTs(k, math.MaxUint64))
			require.NoError(t, err)
			fmt.Printf("wanted=%q Item: %s\n", k, item.ToString())
			fmt.Printf("on re-run, got version: %+v\n", vs)

			txn := kv.NewTransaction(false)
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Seek(k); itr.Valid(); itr.Next() {
				item := itr.Item()
				fmt.Printf("item=%s\n", item.ToString())
				if !bytes.Equal(item.Key(), k) {
					break
				}
			}
			itr.Close()
			txn.Discard()
		}
		require.EqualValues(t, expectedValue, string(getItemValue(t, item)), "wanted=%q Item: %s\n", k, item.ToString())
		txn.Discard()
	}

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 10000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		txn := kv.NewTransaction(true)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Delete(data(j)))
		}
		require.NoError(t, txn.Commit(nil))
	}
	kv.validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := data(i)
		txn := kv.NewTransaction(false)
		_, err := txn.Get([]byte(k))
		require.Equal(t, ErrKeyNotFound, err, "should not have found k: %q", k)
		txn.Discard()
	}
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestExistsMore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := Open(getTestOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	//	n := 500000
	n := 10000
	m := 49
	for i := 0; i < n; i += m {
		if (i % 1000) == 0 {
			t.Logf("Putting i=%d\n", i)
		}
		txn := kv.NewTransaction(true)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Set([]byte(fmt.Sprintf("%09d", j)),
				[]byte(fmt.Sprintf("%09d", j)),
				0x00))
		}
		require.NoError(t, txn.Commit(nil))
	}
	kv.validate()

	for i := 0; i < n; i++ {
		if (i % 1000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.NoError(t, kv.View(func(txn *Txn) error {
			_, err := txn.Get([]byte(k))
			require.NoError(t, err)
			return nil
		}))
	}
	require.NoError(t, kv.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("non-exists"))
		require.Error(t, err)
		return nil
	}))

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 1000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		txn := kv.NewTransaction(true)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Delete([]byte(fmt.Sprintf("%09d", j))))
		}
		require.NoError(t, txn.Commit(nil))
	}
	kv.validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)

		require.NoError(t, kv.View(func(txn *Txn) error {
			_, err := txn.Get([]byte(k))
			require.Error(t, err)
			return nil
		}))
	}
	fmt.Println("Done and closing")
}

func TestIterate2Basic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := Open(getTestOptions(dir))
	defer kv.Close()

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
		txnSet(t, kv, bkey(i), bval(i), byte(i%127))
	}

	opt := IteratorOptions{}
	opt.PrefetchValues = true
	opt.PrefetchSize = 10

	txn := kv.NewTransaction(false)
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
}

func TestLoad(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	fmt.Printf("Writing to dir %s\n", dir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	n := 10000
	{
		kv, _ := Open(getTestOptions(dir))
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
	fmt.Printf("FileIDs: %v\n", fileIDs)
}

func TestIterateDeleted(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.SyncWrites = true
	opt.Dir = dir
	opt.ValueDir = dir
	ps, err := Open(&opt)
	require.NoError(t, err)
	defer ps.Close()
	txnSet(t, ps, []byte("Key1"), []byte("Value1"), 0x00)
	txnSet(t, ps, []byte("Key2"), []byte("Value2"), 0x00)

	iterOpt := DefaultIteratorOptions
	iterOpt.PrefetchValues = false
	txn := ps.NewTransaction(false)
	idxIt := txn.NewIterator(iterOpt)
	defer idxIt.Close()

	count := 0
	txn2 := ps.NewTransaction(true)
	prefix := []byte("Key")
	for idxIt.Seek(prefix); idxIt.ValidForPrefix(prefix); idxIt.Next() {
		key := idxIt.Item().Key()
		count++
		newKey := make([]byte, len(key))
		copy(newKey, key)
		require.NoError(t, txn2.Delete(newKey))
	}
	require.Equal(t, 2, count)
	require.NoError(t, txn2.Commit(nil))

	for _, prefetch := range [...]bool{true, false} {
		t.Run(fmt.Sprintf("Prefetch=%t", prefetch), func(t *testing.T) {
			txn := ps.NewTransaction(false)
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
}

func TestDirNotExists(t *testing.T) {
	_, err := Open(getTestOptions("not-exists"))
	require.Error(t, err)
}

func TestDeleteWithoutSyncWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := new(Options)
	*opt = DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	kv, err := Open(opt)
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
	kv, err = Open(opt)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	require.NoError(t, kv.View(func(txn *Txn) error {
		_, err := txn.Get(key)
		require.Error(t, ErrKeyNotFound, err)
		return nil
	}))
}

func TestPidFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	options := getTestOptions(dir)
	kv1, err := Open(options)
	require.NoError(t, err)
	defer kv1.Close()
	_, err = Open(options)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Another process is using this Badger database")
}

func TestBigKeyValuePairs(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	kv, err := Open(opt)
	require.NoError(t, err)

	bigK := make([]byte, maxKeySize+1)
	bigV := make([]byte, opt.ValueLogFileSize+1)
	small := make([]byte, 10)

	txn := kv.NewTransaction(true)
	require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.Set(bigK, small, 0))
	require.Regexp(t, regexp.MustCompile("Value.*exceeded"), txn.Set(small, bigV, 0))

	require.NoError(t, txn.Set(small, small, 0x00))
	require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.Set(bigK, bigV, 0x00))

	require.NoError(t, kv.View(func(txn *Txn) error {
		_, err := txn.Get(small)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))
	require.NoError(t, kv.Close())
}

func TestIteratorPrefetchSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := Open(getTestOptions(dir))
	defer kv.Close()

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
		txnSet(t, kv, bkey(i), bval(i), byte(i%127))
	}

	getIteratorCount := func(prefetchSize int) int {
		opt := IteratorOptions{}
		opt.PrefetchValues = true
		opt.PrefetchSize = prefetchSize

		var count int
		txn := kv.NewTransaction(false)
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
}

func TestSetIfAbsentAsync(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
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
		require.NoError(t, txn.Set(bkey(i), nil, byte(i%127)))
		require.NoError(t, txn.Commit(f))
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
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := Open(getTestOptions(dir))

	data := make([]byte, 4096)
	_, err = rand.Read(data)
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
			txnSet(t, kv, []byte(key), data, 0x00)
			keyCh <- key
		}
	}()

	// reader
	wg.Add(1)
	go func() {
		defer wg.Done()

		for key := range keyCh {
			require.NoError(t, kv.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(key))
				require.NoError(t, err)
				_, err = item.Value()
				require.NoError(t, err)
				return nil
			}))
		}
	}()

	wg.Wait()
}

func ExampleOpen() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := Open(&opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("key"))
		// We expect ErrKeyNotFound
		fmt.Println(err)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	txn := db.NewTransaction(true) // Read-write txn
	err = txn.Set([]byte("key"), []byte("value"), 0)
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit(nil)
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key"))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", string(val))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// Key not found
	// value
}

func ExampleTxn_NewIterator() {
	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(&opts)
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
		err := txn.Set(bkey(i), bval(i), 0)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = txn.Commit(nil)
	if err != nil {
		log.Fatal(err)
	}

	opt := DefaultIteratorOptions
	opt.PrefetchSize = 10

	// Iterate over 1000 items
	var count int
	err = db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Counted %d elements", count)
	// Output:
	// Counted 1000 elements
}
