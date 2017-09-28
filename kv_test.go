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
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"math"
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
	return opt
}

func getItemValue(t *testing.T, item *KVItem) (val []byte) {
	err := item.Value(func(v []byte) error {
		if v == nil {
			return nil
		}
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})

	if err != nil {
		t.Error(err)
	}
	return val
}

func txnSet(t *testing.T, kv *KV, key []byte, val []byte, meta byte) {
	txn, err := kv.NewTransaction(true)
	require.NoError(t, err)
	require.NoError(t, txn.Set(key, val, meta))
	require.NoError(t, txn.Commit(nil))
}

func txnDelete(t *testing.T, kv *KV, key []byte) {
	txn, err := kv.NewTransaction(true)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(key))
	require.NoError(t, txn.Commit(nil))
}

func txnGet(t *testing.T, kv *KV, key []byte) (KVItem, error) {
	txn, err := kv.NewTransaction(false)
	require.NoError(t, err)
	return txn.Get(key)
}

func TestWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	for i := 0; i < 100; i++ {
		txnSet(t, kv, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
	}
}

func TestConcurrentWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := NewKV(getTestOptions(dir))
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

	txn, err := kv.NewTransaction(true)
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
	kv, err := NewKV(getTestOptions(dir))
	if err != nil {
		t.Error(err)
	}
	defer kv.Close()
	txnSet(t, kv, []byte("key1"), []byte("val1"), 0x08)

	item, err := txnGet(t, kv, []byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val1", getItemValue(t, &item))
	require.Equal(t, byte(0x08), item.UserMeta())

	txnSet(t, kv, []byte("key1"), []byte("val2"), 0x09)
	item, err = txnGet(t, kv, []byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val2", getItemValue(t, &item))
	require.Equal(t, byte(0x09), item.UserMeta())

	txnDelete(t, kv, []byte("key1"))
	item, err = txnGet(t, kv, []byte("key1"))
	require.Equal(t, ErrKeyNotFound, err)

	fmt.Printf("here\n")
	txnSet(t, kv, []byte("key1"), []byte("val3"), 0x01)
	item, err = txnGet(t, kv, []byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, "val3", getItemValue(t, &item))
	require.Equal(t, byte(0x01), item.UserMeta())

	longVal := make([]byte, 1000)
	txnSet(t, kv, []byte("key1"), longVal, 0x00)
	item, err = txnGet(t, kv, []byte("key1"))
	require.NoError(t, err)
	require.EqualValues(t, longVal, getItemValue(t, &item))
}

func TestExists(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
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
		_, err := txnGet(t, kv, test.key)
		if test.exists {
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
	}

}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestGetMore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	data := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	//	n := 500000
	n := 10000
	m := 100
	fmt.Println("writing")
	for i := 0; i < n; i += m {
		txn, err := kv.NewTransaction(true)
		require.NoError(t, err)
		for j := i; j < i+m && j < n; j++ {
			require.NoError(t, txn.Set(data(j), data(j), 0))
		}
		require.NoError(t, txn.Commit(nil))
	}
	require.NoError(t, kv.validate())

	fmt.Println("retrieving")
	for i := 0; i < n; i++ {
		item, err := txnGet(t, kv, data(i))
		if err != nil {
			t.Error(err)
		}
		require.EqualValues(t, string(data(i)), string(getItemValue(t, &item)))
	}

	// Overwrite
	fmt.Println("overwriting")
	for i := 0; i < n; i += m {
		txn, err := kv.NewTransaction(true)
		require.NoError(t, err)
		for j := i; j < i+m && j < n; j++ {
			if j == 483 {
				fmt.Printf("overwriting 483 with readts: %d\n", txn.readTs)
			}
			require.NoError(t, txn.Set(data(j),
				// Use a long value that will certainly exceed value threshold.
				[]byte(fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%09d", j)),
				0x00))
		}
		require.NoError(t, txn.Commit(nil))
	}
	require.NoError(t, kv.validate())

	fmt.Println("testing")
	t.Logf("txn read ts %d commit ts %d\n", kv.txnState.readTs(), kv.txnState.commitTs())
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%09d", i)
		item, err := txnGet(t, kv, []byte(k))
		if err != nil {
			t.Error(err)
		}
		got := string(getItemValue(t, &item))
		if expectedValue != got {
			k0 := y.KeyWithTs(k, math.MaxUint64)

			vs, err := kv.get(k0)
			require.NoError(t, err)
			fmt.Printf("wanted=%q Item: %s\n", k, item.ToString())
			fmt.Printf("on re-run, got version: %+v\n", vs)

			txn, err := kv.NewTransaction(false)
			require.NoError(t, err)
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Seek(k0); itr.Valid(); itr.Next() {
				item := itr.Item()
				fmt.Printf("item=%s\n", item.ToString())
				if !bytes.Equal(item.Key(), k) {
					break
				}
			}
			itr.Close()
		}
		require.EqualValues(t, expectedValue, string(getItemValue(t, &item)), "wanted=%q Item: %s\n", k, item.ToString())
	}

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 10000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		txn, err := kv.NewTransaction(true)
		require.NoError(t, err)
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
		k := data(i)
		item, err := txnGet(t, kv, []byte(k))
		require.Equal(t, ErrKeyNotFound, err, "wanted=%q item=%s\n", k, item.ToString())
	}
	fmt.Println("Done and closing")
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestExistsMore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	//	n := 500000
	n := 10000
	m := 100
	for i := 0; i < n; i += m {
		if (i % 1000) == 0 {
			fmt.Printf("Putting i=%d\n", i)
		}
		txn, err := kv.NewTransaction(true)
		require.NoError(t, err)
		for j := i; j < i+m && j < n; j++ {
			txn.Set([]byte(fmt.Sprintf("%09d", j)),
				[]byte(fmt.Sprintf("%09d", j)),
				0x00)
		}
		require.NoError(t, txn.Commit(nil))
	}
	kv.validate()

	for i := 0; i < n; i++ {
		if (i % 1000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		_, err = txnGet(t, kv, []byte(k))
		require.NoError(t, err)
	}
	_, err = txnGet(t, kv, []byte("non-exists"))
	require.Error(t, err)

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 1000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		txn, err := kv.NewTransaction(true)
		require.NoError(t, err)
		for j := i; j < i+m && j < n; j++ {
			txn.Delete([]byte(fmt.Sprintf("%09d", j)))
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
		_, err = txnGet(t, kv, []byte(k))
		require.Error(t, err)
	}
	fmt.Println("Done and closing")
}

func TestIterate2Basic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := NewKV(getTestOptions(dir))
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
		kv.Set(bkey(i), bval(i), byte(i%127))
	}

	opt := IteratorOptions{}
	opt.PrefetchValues = true
	opt.PrefetchSize = 10

	it := kv.NewIterator(opt)
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
		start := bkey(idx)
		for it.Seek(start); it.Valid(); it.Next() {
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
		kv, _ := NewKV(getTestOptions(dir))
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%09d", i))
			kv.Set(k, k, 0x00)
		}
		kv.Close()
	}

	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	var item KVItem
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		if err := kv.Get([]byte(k), &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, k, string(getItemValue(t, &item)))
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
	ps, err := NewKV(&opt)
	require.NoError(t, err)
	defer ps.Close()
	ps.Set([]byte("Key1"), []byte("Value1"), 0x00)
	ps.Set([]byte("Key2"), []byte("Value2"), 0x00)

	iterOpt := DefaultIteratorOptions
	iterOpt.PrefetchValues = false
	idxIt := ps.NewIterator(iterOpt)
	defer idxIt.Close()

	wb := make([]*Entry, 0, 100)
	prefix := []byte("Key")
	for idxIt.Seek(prefix); idxIt.Valid(); idxIt.Next() {
		key := idxIt.Item().Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		wb = EntriesDelete(wb, key)
	}
	require.Equal(t, 2, len(wb))
	ps.BatchSet(wb)

	for _, e := range wb {
		require.NoError(t, e.Error)
	}

	for _, prefetch := range [...]bool{true, false} {
		t.Run(fmt.Sprintf("Prefetch=%t", prefetch), func(t *testing.T) {
			iterOpt = DefaultIteratorOptions
			iterOpt.PrefetchValues = prefetch
			idxIt = ps.NewIterator(iterOpt)

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
	_, err := NewKV(getTestOptions("not-exists"))
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
	kv, err := NewKV(opt)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	key := []byte("k1")
	// Set a value with size > value threshold so that its written to value log.
	require.NoError(t, kv.Set(key, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789FOOBARZOGZOG"), 0x00))
	require.NoError(t, kv.Delete(key))
	kv.Close()

	// Reopen KV
	kv, err = NewKV(opt)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer kv.Close()

	item := KVItem{}
	require.NoError(t, kv.Get(key, &item))
	require.Equal(t, 0, len(getItemValue(t, &item)))
}

func TestSetIfAbsent(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	opt := getTestOptions(dir)
	kv, err := NewKV(opt)
	require.NoError(t, err)

	key := []byte("k1")
	err = kv.SetIfAbsent(key, []byte("val"), 0x00)
	require.NoError(t, err)

	err = kv.SetIfAbsent(key, []byte("val2"), 0x00)
	require.EqualError(t, err, ErrKeyExists.Error())
}

func BenchmarkExists(b *testing.B) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(b, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	if err != nil {
		b.Error(err)
		b.Fail()
	}
	defer kv.Close()

	n := 50000
	m := 100
	for i := 0; i < n; i += m {
		if (i % 10000) == 0 {
			fmt.Printf("Putting i=%d\n", i)
		}
		var entries []*Entry
		for j := i; j < i+m && j < n; j++ {
			entries = append(entries, &Entry{
				Key:   []byte(fmt.Sprintf("%09d", j)),
				Value: []byte(fmt.Sprintf("%09d", j)),
			})
		}
		kv.BatchSet(entries)
		for _, e := range entries {
			require.NoError(b, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()

	// rand.Seed(int64(time.Now().Nanosecond()))

	b.Run("WithGet", func(b *testing.B) {
		b.ResetTimer()
		item := &KVItem{}
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%09d", i%n)
			err := kv.Get([]byte(k), item)
			if err != nil {
				b.Error(err)
			}
			var val []byte
			err = item.Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			})
			if err != nil {
				b.Error(err)
			}
			found := val == nil
			_ = found
		}
	})

	b.Run("WithExists", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// k := fmt.Sprintf("%09d", rand.Intn(n))
			k := fmt.Sprintf("%09d", i%n)
			// k := fmt.Sprintf("%09d", 0)
			found, err := kv.Exists([]byte(k))
			if err != nil {
				b.Error(err)
			}
			_ = found
		}
	})

	fmt.Println("Done and closing")
}

func TestPidFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	options := getTestOptions(dir)
	kv1, err := NewKV(options)
	require.NoError(t, err)
	defer kv1.Close()
	_, err = NewKV(options)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Another process is using this Badger database")
}

func TestBigKeyValuePairs(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	kv, err := NewKV(opt)
	require.NoError(t, err)

	bigK := make([]byte, maxKeySize+1)
	bigV := make([]byte, opt.ValueLogFileSize+1)
	small := make([]byte, 10)

	require.Regexp(t, regexp.MustCompile("Key.*exceeded"), kv.Set(bigK, small, 0).Error())
	require.Regexp(t, regexp.MustCompile("Value.*exceeded"), kv.Set(small, bigV, 0).Error())

	e1 := Entry{Key: small, Value: small}
	e2 := Entry{Key: bigK, Value: bigV}
	err = kv.BatchSet([]*Entry{&e1, &e2})
	require.Nil(t, err)
	require.Nil(t, e1.Error)
	require.Regexp(t, regexp.MustCompile("Key.*exceeded"), e2.Error.Error())

	// make sure e1 was actually set:
	var item KVItem
	require.NoError(t, kv.Get(small, &item))
	require.Equal(t, item.Key(), small)
	require.Equal(t, getItemValue(t, &item), small)

	require.NoError(t, kv.Close())
}

func TestIteratorPrefetchSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := NewKV(getTestOptions(dir))
	defer kv.Close()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	n := 100
	for i := 0; i < n; i++ {
		if (i % 10) == 0 {
			t.Logf("Put i=%d\n", i)
		}
		kv.Set(bkey(i), bval(i), byte(i%127))
	}

	getIteratorCount := func(prefetchSize int) int {
		opt := IteratorOptions{}
		opt.PrefetchValues = true
		opt.PrefetchSize = prefetchSize

		var count int
		it := kv.NewIterator(opt)
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
	kv, _ := NewKV(getTestOptions(dir))

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}

	f := func(err error) {}

	n := 1000
	for i := 0; i < n; i++ {
		if (i % 10) == 0 {
			t.Logf("Put i=%d\n", i)
		}
		kv.SetIfAbsentAsync(bkey(i), nil, byte(i%127), f)
	}

	require.NoError(t, kv.Close())
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)

	opt := DefaultIteratorOptions
	var count int
	it := kv.NewIterator(opt)
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
	kv, _ := NewKV(getTestOptions(dir))

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
			err = kv.Set([]byte(key), data, 0x00)
			require.NoError(t, err)
			keyCh <- key
		}
	}()

	// reader
	wg.Add(1)
	go func() {
		defer wg.Done()

		for key := range keyCh {
			var item KVItem

			err := kv.Get([]byte(key), &item)
			require.NoError(t, err)

			var val []byte
			err = item.Value(func(v []byte) error {
				val = make([]byte, len(v))
				copy(val, v)
				return nil
			})
			require.NoError(t, err)
		}
	}()

	wg.Wait()
}
