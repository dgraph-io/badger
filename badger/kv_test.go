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
	"fmt"
	"io/ioutil"
	"os"
	//	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getTestOptions(dir string) *Options {
	opt := new(Options)
	*opt = DefaultOptions
	opt.MaxTableSize = 1 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	opt.Verbose = true
	opt.Dir = dir
	opt.SyncWrites = true // Some tests seem to need this to pass.
	opt.ValueGCThreshold = 0.0
	return opt
}

func TestWrite(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
	defer kv.Close()

	var entries []*Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	ctx := context.Background()
	require.NoError(t, kv.Write(ctx, entries))
}

func TestConcurrentWrite(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
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
				kv.Put(ctx, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
					[]byte(fmt.Sprintf("v%05d_%08d", i, j)))
			}
		}(i)
	}
	wg.Wait()

	t.Log("Starting iteration")

	opt := IteratorOptions{}
	opt.Reversed = false
	opt.PrefetchSize = 10
	opt.FetchValues = true

	it := kv.NewIterator(opt)
	var i, j int
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if k == nil {
			break // end of iteration.
		}

		if bytes.Equal(k, Head) {
			continue
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
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
	defer kv.Close()

	var entries []*Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	ctx := context.Background()
	require.NoError(t, kv.Write(ctx, entries))
	time.Sleep(time.Second)

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		value, casCounter := kv.Get(ctx, k)
		require.EqualValues(t, v, value)
		require.EqualValues(t, entries[i].casCounter, casCounter)
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
		require.NoError(t, kv.CASPut(ctx, k, v, cc))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		value, casCounter := kv.Get(ctx, k)
		require.EqualValues(t, v, value)
		require.EqualValues(t, entries[i].casCounter, casCounter)
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		cc := entries[i].casCounter
		if cc == 5 {
			cc = 6
		} else {
			cc = 5
		}
		require.NoError(t, kv.CASDelete(ctx, k, cc))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		value, casCounter := kv.Get(ctx, k)
		require.EqualValues(t, v, value)
		require.EqualValues(t, entries[i].casCounter, casCounter)
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("zzz%d", i))
		require.NoError(t, kv.CASPut(ctx, k, v, entries[i].casCounter))
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("zzz%d", i)) // Value should be changed.
		value, casCounter := kv.Get(ctx, k)
		require.EqualValues(t, v, value)
		require.True(t, casCounter != 0)
	}
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
	defer kv.Close()

	require.NoError(t, kv.Put(ctx, []byte("key1"), []byte("val1")))
	value, casCounter := kv.Get(ctx, []byte("key1"))
	require.EqualValues(t, "val1", value)
	require.True(t, casCounter != 0)

	require.NoError(t, kv.Put(ctx, []byte("key1"), []byte("val2")))
	value, casCounter = kv.Get(ctx, []byte("key1"))
	require.EqualValues(t, "val2", value)
	require.True(t, casCounter != 0)

	require.NoError(t, kv.Delete(ctx, []byte("key1")))
	value, casCounter = kv.Get(ctx, []byte("key1"))
	require.Nil(t, value)
	require.True(t, casCounter != 0)

	require.NoError(t, kv.Put(ctx, []byte("key1"), []byte("val3")))
	value, casCounter = kv.Get(ctx, []byte("key1"))
	require.EqualValues(t, "val3", value)
	require.True(t, casCounter != 0)

	longVal := make([]byte, 1000)
	require.NoError(t, kv.Put(ctx, []byte("key1"), longVal))
	value, casCounter = kv.Get(ctx, []byte("key1"))
	require.EqualValues(t, longVal, value)
	require.True(t, casCounter != 0)
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestGetMore(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
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
		require.NoError(t, kv.Write(ctx, entries))
	}
	kv.Validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		value, _ := kv.Get(ctx, []byte(k))
		require.EqualValues(t, k, string(value))
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
		require.NoError(t, kv.Write(ctx, entries))
	}
	kv.Validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		expectedValue := fmt.Sprintf("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz%09d", i)
		value, _ := kv.Get(ctx, []byte(k))
		require.EqualValues(t, expectedValue, string(value))
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
		require.NoError(t, kv.Write(ctx, entries))
	}
	kv.Validate()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		value, _ := kv.Get(ctx, []byte(k))
		require.Nil(t, value)
	}
	fmt.Println("Done and closing")
}

func TestIterate2Basic(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv := NewKV(getTestOptions(dir))
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
		require.NoError(t, kv.Put(ctx, bkey(i), bval(i)))
	}

	opt := IteratorOptions{}
	opt.FetchValues = true
	opt.PrefetchSize = 10

	{
		var count int
		rewind := true
		t.Log("Starting first basic iteration")
		it := kv.NewIterator(opt)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Equal(key, Head) {
				continue
			}
			if rewind && count == 5000 {
				count = 0
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
		it := kv.NewIterator(opt)
		idx := 5030
		start := bkey(idx)
		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			require.EqualValues(t, bkey(idx), string(item.Key()))
			require.EqualValues(t, bval(idx), string(item.Value()))
			idx++
		}
	}
}

func TestLoad(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	fmt.Printf("Writing to dir %s\n", dir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	n := 10000
	{
		kv := NewKV(getTestOptions(dir))
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%09d", i))
			require.NoError(t, kv.Put(ctx, k, k))
		}
		kv.Close()
	}

	kv := NewKV(getTestOptions(dir))
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		value, _ := kv.Get(ctx, []byte(k))
		require.EqualValues(t, k, string(value))
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

func TestCrash(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.MaxTableSize = 1 << 20
	opt.Dir = dir
	opt.DoNotCompact = true
	opt.Verbose = true

	kv := NewKV(&opt)
	var keys [][]byte
	for i := 0; i < 150000; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		keys = append(keys, k)
	}

	entries := make([]*Entry, 0, 10)
	for _, k := range keys {
		e := &Entry{
			Key:   k,
			Value: k,
		}
		entries = append(entries, e)

		if len(entries) == 100 {
			err := kv.Write(ctx, entries)
			require.Nil(t, err)
			entries = entries[:0]
		}
	}

	for _, k := range keys {
		value, _ := kv.Get(ctx, k)
		require.Equal(t, k, value)
	}
	// Do not close kv store (!!) for this test to make sense.

	kv2 := NewKV(&opt)
	for _, k := range keys {
		value, casCounter := kv2.Get(ctx, k)
		require.Equal(t, k, value, "Key: %s", k)
		require.True(t, casCounter != 0)
	}

	{
		val, _ := kv.Get(ctx, Head)
		voffset := binary.BigEndian.Uint64(val)
		fmt.Printf("level 1 val: %v\n", voffset)
	}

	kv.lc.tryCompact(1)
	kv.lc.tryCompact(1)
	val, _ := kv.Get(ctx, Head)
	require.True(t, len(val) > 0)
	voffset := binary.BigEndian.Uint64(val)
	fmt.Printf("level 1 val: %v\n", voffset)

	kv3 := NewKV(&opt)
	for _, k := range keys {
		value, casCounter := kv3.Get(ctx, k)
		require.True(t, casCounter != 0)
		require.Equal(t, k, value, "Key: %s", k)
	}
}

// Test replay of log when there are CAS entries.
func TestCrashCAS(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.MaxTableSize = 1 << 20
	opt.Dir = dir
	opt.DoNotCompact = true
	opt.Verbose = true

	kv := NewKV(&opt)
	var keys [][]byte
	for i := 0; i < 150000; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		keys = append(keys, k)
	}

	entries := make([]*Entry, 0, 10)
	for _, k := range keys {
		e := &Entry{
			Key:   k,
			Value: k,
		}
		entries = append(entries, e)
	}
	err = kv.Write(ctx, entries)
	require.Nil(t, err)

	oldEntries := entries
	entries = make([]*Entry, 0, 10)

	for i := 0; i < 150000; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		vs := kv.get(ctx, k)
		require.EqualValues(t, oldEntries[i].casCounter, vs.CASCounter)

		e := &Entry{
			Key:             k,
			Value:           []byte(fmt.Sprintf("changed%d", i)),
			CASCounterCheck: vs.CASCounter,
		}
		if (i % 2) == 1 {
			// Be careful with writing "vs.CASCounter+1" as it may overflow and the test can fail.
			if e.CASCounterCheck != 5 {
				e.CASCounterCheck = 5
			} else {
				e.CASCounterCheck = 6
			}
		}
		entries = append(entries, e)
	}
	err = kv.Write(ctx, entries)
	require.Nil(t, err)

	for i, k := range keys {
		value, casCounter := kv.Get(ctx, k)
		if (i % 2) == 0 {
			require.EqualValues(t, fmt.Sprintf("changed%d", i), string(value), "%d", casCounter)
		} else {
			require.EqualValues(t, string(k), string(value), "%d %d", casCounter, entries[i].CASCounterCheck)
		}
	}
	entries = entries[:0]

	//	// Do not close kv store (!!) for this test to make sense.

	kv2 := NewKV(&opt)
	for i, k := range keys {
		value, _ := kv2.Get(ctx, k)
		if (i % 2) == 0 {
			require.EqualValues(t, fmt.Sprintf("changed%d", i), string(value))
		} else {
			require.EqualValues(t, string(k), string(value))
		}
	}

	{
		val, _ := kv.Get(ctx, Head)
		voffset := binary.BigEndian.Uint64(val)
		fmt.Printf("level 1 val: %v\n", voffset)
	}

	kv.lc.tryCompact(1)
	kv.lc.tryCompact(1)
	val, _ := kv.Get(ctx, Head)
	require.True(t, len(val) > 0)
	voffset := binary.BigEndian.Uint64(val)
	fmt.Printf("level 1 val: %v\n", voffset)

	kv3 := NewKV(&opt)
	for i, k := range keys {
		value, _ := kv3.Get(ctx, k)
		if (i % 2) == 0 {
			require.EqualValues(t, fmt.Sprintf("changed%d", i), string(value))
		} else {
			require.EqualValues(t, string(k), string(value))
		}
	}
}
