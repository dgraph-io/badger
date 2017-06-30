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
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestOptions(dir string) *Options {
	opt := new(Options)
	*opt = DefaultOptions
	opt.MaxTableSize = 1 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	opt.Dir = dir
	opt.ValueDir = dir
	opt.SyncWrites = true // Some tests seem to need this to pass.
	opt.ValueGCThreshold = 0.0
	return opt
}

func TestWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := NewKV(getTestOptions(dir))
	defer kv.Close()

	var entries []*Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
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
				kv.Set([]byte(fmt.Sprintf("k%05d_%08d", i, j)),
					[]byte(fmt.Sprintf("v%05d_%08d", i, j)))
			}
		}(i)
	}
	wg.Wait()

	t.Log("Starting iteration")

	opt := IteratorOptions{}
	opt.Reverse = false
	opt.PrefetchSize = 10
	opt.FetchValues = true

	it := kv.NewIterator(opt)
	defer it.Close()
	var i, j int
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if k == nil {
			break // end of iteration.
		}

		require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), string(k))
		v := item.Value()
		require.EqualValues(t, fmt.Sprintf("v%05d_%08d", i, j), string(v))
		j++
		if j == m {
			i++
			j = 0
		}
	}
	require.EqualValues(t, n, i)
	require.EqualValues(t, 0, j)
}

func TestCAS(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, _ := NewKV(getTestOptions(dir))
	defer kv.Close()

	var entries []*Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	time.Sleep(time.Second)

	var item KVItem
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		if err := kv.Get(k, &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, v, item.Value())
		require.EqualValues(t, entries[i].casCounter, item.Counter())
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("zzz%d", i))
		cc := entries[i].casCounter
		if cc == 5 {
			cc = 6
		} else {
			cc = 5
		}
		require.Error(t, kv.CompareAndSet(k, v, cc))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		if err := kv.Get(k, &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, v, item.Value())
		require.EqualValues(t, entries[i].casCounter, item.Counter())
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		cc := entries[i].casCounter
		if cc == 5 {
			cc = 6
		} else {
			cc = 5
		}
		require.Error(t, kv.CompareAndDelete(k, cc))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		if err := kv.Get(k, &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, v, item.Value())
		require.EqualValues(t, entries[i].casCounter, item.Counter())
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("zzz%d", i))
		require.NoError(t, kv.CompareAndSet(k, v, entries[i].casCounter))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("zzz%d", i)) // Value should be changed.
		if err := kv.Get(k, &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, v, item.Value())
		require.True(t, item.Counter() != 0)
	}
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

	var item KVItem
	kv.Set([]byte("key1"), []byte("val1"))

	if err := kv.Get([]byte("key1"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, "val1", item.Value())
	require.True(t, item.Counter() != 0)

	kv.Set([]byte("key1"), []byte("val2"))
	if err := kv.Get([]byte("key1"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, "val2", item.Value())
	require.True(t, item.Counter() != 0)

	kv.Delete([]byte("key1"))
	if err := kv.Get([]byte("key1"), &item); err != nil {
		t.Error(err)
	}
	require.Nil(t, item.Value())
	require.True(t, item.Counter() != 0)

	kv.Set([]byte("key1"), []byte("val3"))
	if err := kv.Get([]byte("key1"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, "val3", item.Value())
	require.True(t, item.Counter() != 0)

	longVal := make([]byte, 1000)
	kv.Set([]byte("key1"), longVal)
	if err := kv.Get([]byte("key1"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, longVal, item.Value())
	require.True(t, item.Counter() != 0)
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
	err = kv.Set([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	tt := []struct {
		key    []byte
		exists bool
		name   string
	}{
		{
			key:    []byte("key1"),
			exists: true,
			name:   " valid key",
		},
		{
			key:    []byte("non-exits"),
			exists: false,
			name:   "non exist key",
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			exists, err := kv.Exists(test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.exists, exists)
		})
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

	//	n := 500000
	n := 10000
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
			require.NoError(t, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()

	var item KVItem
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		if err := kv.Get([]byte(k), &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, k, string(item.Value()))
	}

	// Overwrite
	for i := n - 1; i >= 0; i -= m {
		if (i % 10000) == 0 {
			fmt.Printf("Overwriting i=%d\n", i)
		}
		var entries []*Entry
		for j := i; j > i-m && j >= 0; j-- {
			entries = append(entries, &Entry{
				Key: []byte(fmt.Sprintf("%09d", j)),
				// Use a long value that will certainly exceed value threshold.
				Value: []byte(fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%09d", j)),
			})
		}
		kv.BatchSet(entries)
		for _, e := range entries {
			require.NoError(t, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%09d", i)
		if err := kv.Get([]byte(k), &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, expectedValue, string(item.Value()))
	}

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 10000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		var entries []*Entry
		for j := i; j < i+m && j < n; j++ {
			entries = append(entries, &Entry{
				Key:  []byte(fmt.Sprintf("%09d", j)),
				Meta: BitDelete,
			})
		}
		kv.BatchSet(entries)
		for _, e := range entries {
			require.NoError(t, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		if err := kv.Get([]byte(k), &item); err != nil {
			t.Error(err)
		}
		require.Nil(t, item.Value())
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
		var entries []*Entry
		for j := i; j < i+m && j < n; j++ {
			entries = append(entries, &Entry{
				Key:   []byte(fmt.Sprintf("%09d", j)),
				Value: []byte(fmt.Sprintf("%09d", j)),
			})
		}
		kv.BatchSet(entries)
		for _, e := range entries {
			require.NoError(t, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()

	var found bool
	for i := 0; i < n; i++ {
		if (i % 1000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		found, err = kv.Exists([]byte(k))
		if err != nil {
			t.Error(err)
		}
		require.EqualValues(t, true, found)
	}
	found, err = kv.Exists([]byte("non-exists"))
	if err != nil {
		t.Error(err)
	}
	require.EqualValues(t, false, found)

	// "Delete" key.
	for i := 0; i < n; i += m {
		if (i % 1000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		var entries []*Entry
		for j := i; j < i+m && j < n; j++ {
			entries = append(entries, &Entry{
				Key:  []byte(fmt.Sprintf("%09d", j)),
				Meta: BitDelete,
			})
		}
		kv.BatchSet(entries)
		for _, e := range entries {
			require.NoError(t, e.Error, "entry with error: %+v", e)
		}
	}
	kv.validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		found, err := kv.Exists([]byte(k))
		if err != nil {
			t.Error(err)
		}
		require.False(t, found, fmt.Sprintf("key=%s", k))
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
		kv.Set(bkey(i), bval(i))
	}

	opt := IteratorOptions{}
	opt.FetchValues = true
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
			val := item.Value()
			require.EqualValues(t, bval(count), string(val))
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
			require.EqualValues(t, bval(idx), string(item.Value()))
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
			kv.Set(k, k)
		}
		kv.Close()
	}

	kv, _ := NewKV(getTestOptions(dir))
	var item KVItem
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		if err := kv.Get([]byte(k), &item); err != nil {
			t.Error(err)
		}
		require.EqualValues(t, k, string(item.Value()))
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
	ps.Set([]byte("Key1"), []byte("Value1"))
	ps.Set([]byte("Key2"), []byte("Value2"))

	iterOpt := DefaultIteratorOptions
	iterOpt.FetchValues = false
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
			iterOpt.FetchValues = prefetch
			idxIt = ps.NewIterator(iterOpt)

			var idxKeys []string
			for idxIt.Seek(prefix); idxIt.Valid(); idxIt.Next() {
				key := idxIt.Item().Key()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				idxKeys = append(idxKeys, string(key))
				t.Logf("%+v\n", idxIt.Item())
			}
			require.Equal(t, 0, len(idxKeys))
		})
	}
}

func TestDirNotExists(t *testing.T) {
	_, err := NewKV(getTestOptions("not-exists"))
	require.Error(t, err)
}

func TestDeleteWithoutSyncWrite(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
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
	require.NoError(t, kv.Set(key, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789FOOBARZOGZOG")))
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
	require.Equal(t, 0, len(item.Value()))
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
			found := item.Value() == nil
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
