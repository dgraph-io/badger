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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/value"
)

func TestDBWrite(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	var entries []value.Entry
	for i := 0; i < 100; i++ {
		entries = append(entries, value.Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("val%d", i)),
		})
	}
	require.NoError(t, db.Write(entries))
}

func TestDBGet(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	require.NoError(t, db.Put([]byte("key1"), []byte("val1")))
	require.EqualValues(t, "val1", db.Get([]byte("key1")))

	require.NoError(t, db.Put([]byte("key1"), []byte("val2")))
	require.EqualValues(t, "val2", db.Get([]byte("key1")))

	require.NoError(t, db.Delete([]byte("key1")))
	require.Nil(t, db.Get([]byte("key1")))

	require.NoError(t, db.Put([]byte("key1"), []byte("val3")))
	require.EqualValues(t, "val3", db.Get([]byte("key1")))

	longVal := make([]byte, 1000)
	require.NoError(t, db.Put([]byte("key1"), longVal))
	require.EqualValues(t, longVal, db.Get([]byte("key1")))
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
func TestDBGetMore(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	//	n := 500000
	n := 100000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Putting i=%d\n", i)
		}
		require.NoError(t, db.Put(k, k))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.EqualValues(t, k, string(db.Get([]byte(k))))
	}

	// Overwrite value.
	for i := n - 1; i >= 0; i-- {
		k := []byte(fmt.Sprintf("%09d", i))
		v := []byte(fmt.Sprintf("val%09d", i))
		require.NoError(t, db.Put(k, v))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 100000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		expectedValue := fmt.Sprintf("val%09d", i)
		require.EqualValues(t, expectedValue, string(db.Get(k)))
	}

	// "Delete" key.
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Delete(k))
	}
	db.Check()
	for i := 0; i < n; i++ {
		if (i % 100000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.Nil(t, db.Get([]byte(k)))
	}
}

// Put a lot of data to move some data to disk. Then iterate.
func TestDBIterateBasic(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	defer db.Close()

	n := 500000
	// n := 100
	for i := 0; i < n; i++ {
		if (i % 10000) == 0 {
			fmt.Printf("Put i=%d\n", i)
		}
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Put(k, k))
	}

	it := db.NewIterator()
	defer it.Close()

	var count int
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key, val := it.KeyValue()
		require.EqualValues(t, fmt.Sprintf("%09d", count), string(key))
		require.EqualValues(t, fmt.Sprintf("%09d", count), string(val))
		count++
	}
	require.EqualValues(t, n, count)
}
