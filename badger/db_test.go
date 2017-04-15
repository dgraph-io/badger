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
	"sync"
	"testing"
	//	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/value"
)

func getTestOptions(dir string) *Options {
	opt := new(Options)
	*opt = DefaultOptions
	opt.MaxTableSize = 1 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	//	opt.Verbose = true
	opt.Dir = dir
	return opt
}

func TestDBWrite(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewDB(getTestOptions(dir))
	defer db.Close()

	var entries []value.Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, value.Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	ctx := context.Background()
	require.NoError(t, db.Write(ctx, entries))
}

func TestConcurrentWrite(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewDB(getTestOptions(dir))
	defer db.Close()

	// Not a benchmark. Just a simple test for concurrent writes.
	n := 20
	m := 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < m; j++ {
				db.Put(ctx, []byte(fmt.Sprintf("k%05d_%08d", i, j)),
					[]byte(fmt.Sprintf("v%05d_%08d", i, j)))
			}
		}(i)
	}
	wg.Wait()
	it := db.NewIterator(ctx)
	defer it.Close()

	var i, j int
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		if bytes.Equal(k, Head) {
			continue
		}
		require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), string(k))
		v := it.Value()
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

func TestDBGet(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewDB(getTestOptions(dir))
	defer db.Close()

	require.NoError(t, db.Put(ctx, []byte("key1"), []byte("val1")))
	require.EqualValues(t, "val1", db.Get(ctx, []byte("key1")))

	require.NoError(t, db.Put(ctx, []byte("key1"), []byte("val2")))
	require.EqualValues(t, "val2", db.Get(ctx, []byte("key1")))

	require.NoError(t, db.Delete(ctx, []byte("key1")))
	require.Nil(t, db.Get(ctx, []byte("key1")))

	require.NoError(t, db.Put(ctx, []byte("key1"), []byte("val3")))
	require.EqualValues(t, "val3", db.Get(ctx, []byte("key1")))

	longVal := make([]byte, 1000)
	require.NoError(t, db.Put(ctx, []byte("key1"), longVal))
	require.EqualValues(t, longVal, db.Get(ctx, []byte("key1")))
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestDBGetMore(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewDB(getTestOptions(dir))
	defer db.Close()

	//	n := 500000
	n := 10000
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Putting i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Put(ctx, k, k))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.EqualValues(t, k, string(db.Get(ctx, []byte(k))))
	}

	// Overwrite value.
	for i := n - 1; i >= 0; i-- {
		if (i % 10000) == 0 {
			fmt.Printf("Overwriting i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		v := []byte(fmt.Sprintf("val%09d", i))
		require.NoError(t, db.Put(ctx, k, v))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Testing i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		expectedValue := fmt.Sprintf("val%09d", i)
		require.EqualValues(t, expectedValue, string(db.Get(ctx, k)))
	}

	// "Delete" key.
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Deleting i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Delete(ctx, k))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.Nil(t, db.Get(ctx, []byte(k)))
	}
	fmt.Println("Done and closing")
}

// Put a lot of data to move some data to disk. Then iterate.
func TestDBIterateBasic(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewDB(getTestOptions(dir))
	defer db.Close()

	// n := 500000
	n := 10000
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Put i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Put(ctx, k, k))
	}

	it := db.NewIterator(ctx)
	defer it.Close()

	var count int
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		if bytes.Equal(key, Head) {
			continue
		}
		require.EqualValues(t, fmt.Sprintf("%09d", count), string(key))
		val := it.Value()
		require.EqualValues(t, fmt.Sprintf("%09d", count), string(val))
		count++
	}
	require.EqualValues(t, n, count)
}

func TestDBLoad(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	n := 1000
	{
		db := NewDB(getTestOptions(dir))
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%09d", i))
			require.NoError(t, db.Put(ctx, k, k))
		}
		db.Close()
	}

	{
		db := NewDB(getTestOptions(dir))
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Testing i=%d\n", i)
			}
			k := fmt.Sprintf("%09d", i)
			require.EqualValues(t, k, string(db.Get(ctx, []byte(k))))
		}
		db.Close()
	}
}

func TestCrash(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	opt := DefaultOptions
	opt.MaxTableSize = 1 << 20
	opt.Dir = dir
	opt.DoNotCompact = true
	opt.Verbose = true

	db := NewDB(&opt)
	var keys [][]byte
	for i := 0; i < 150000; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		keys = append(keys, k)
	}

	entries := make([]value.Entry, 0, 10)
	for _, k := range keys {
		e := value.Entry{
			Key:   k,
			Value: k,
		}
		entries = append(entries, e)

		if len(entries) == 100 {
			err := db.Write(ctx, entries)
			require.Nil(t, err)
			entries = entries[:0]
		}
	}

	for _, k := range keys {
		require.Equal(t, k, db.Get(ctx, k))
	}
	// Do not close db.

	db2 := NewDB(&opt)
	for _, k := range keys {
		require.Equal(t, k, db2.Get(ctx, k), "Key: %s", k)
	}

	{
		val := db.Get(ctx, Head)
		voffset := binary.BigEndian.Uint64(val)
		fmt.Printf("level 1 val: %v\n", voffset)
	}

	db.lc.tryCompact(1)
	db.lc.tryCompact(1)
	val := db.Get(ctx, Head)
	// val := db.lc.levels[1].get(Head)
	require.True(t, len(val) > 0)
	voffset := binary.BigEndian.Uint64(val)
	fmt.Printf("level 1 val: %v\n", voffset)

	db3 := NewDB(&opt)
	for _, k := range keys {
		require.Equal(t, k, db3.Get(ctx, k), "Key: %s", k)
	}
}
