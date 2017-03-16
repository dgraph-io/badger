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

package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBWrite(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	wb := NewWriteBatch(10)
	for i := 0; i < 100; i++ {
		wb.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}
	require.NoError(t, db.Write(wb))

	wb2 := NewWriteBatch(10)
	for i := 0; i < 100; i++ {
		wb2.Put([]byte(fmt.Sprintf("KEY%d", i)), []byte(fmt.Sprintf("VAL%d", i)))
	}
	require.NoError(t, db.Write(wb2))
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
}

// Put a lot of data to move some data to disk.
func TestDBGetMore(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	n := 500000
	//	n := 10000000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		require.NoError(t, db.Put(k, k))
	}
	for i := 0; i < n; i++ {
		if (i % 100000) == 0 {
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
	for i := 0; i < n; i++ {
		if (i % 100000) == 0 {
			// Display some progress. Right now, it's not very fast with no caching.
			fmt.Printf("Testing i=%d\n", i)
		}
		k := fmt.Sprintf("%09d", i)
		require.Nil(t, db.Get([]byte(k)))
	}
}
