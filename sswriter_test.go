/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

func getSortedKVList(valueSize, listSize int) *pb.KVList {
	value := make([]byte, valueSize)
	y.Check2(rand.Read(value))
	list := &pb.KVList{}
	for i := 0; i < listSize; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		list.Kv = append(list.Kv, &pb.KV{
			Key:     key,
			Value:   value,
			Version: 20,
		})
	}

	return list
}

// check if we can read values after writing using stream writer
func TestStreamWriter1(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// write entries using stream writer
	noOfKeys := 1000
	valueSize := 128
	list := getSortedKVList(valueSize, noOfKeys)
	sw := db.NewStreamWriter(2)
	require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
	require.NoError(t, sw.Write(list), "sw.Write() failed")
	require.NoError(t, sw.Done(), "sw.Done() failed")

	err = db.View(func(txn *Txn) error {
		// read any random key from inserted keys
		keybyte := make([]byte, 8)
		keyNo := uint64(rand.Int63n(int64(noOfKeys)))
		binary.BigEndian.PutUint64(keybyte, keyNo)
		_, err := txn.Get(keybyte)
		require.Nil(t, err, "key should be found")

		// count all keys written using stream writer
		keysCount := 0
		itrOps := DefaultIteratorOptions
		it := txn.NewIterator(itrOps)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keysCount++
		}
		require.True(t, keysCount == noOfKeys, "count of keys should be matched")
		return nil
	})
	require.NoError(t, err, "error while retrieving key")
}

// write more keys to db after writing keys using stream writer
func TestStreamWriter2(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// write entries using stream writer
	noOfKeys := 1000
	valueSize := 128
	list := getSortedKVList(valueSize, noOfKeys)
	sw := db.NewStreamWriter(2)
	require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
	require.NoError(t, sw.Write(list), "sw.Write() failed")
	require.NoError(t, sw.Done(), "sw.Done() failed")

	// delete all the inserted keys
	val := make([]byte, valueSize)
	y.Check2(rand.Read(val))
	for i := 0; i < noOfKeys; i++ {
		err = db.Update(func(txn *Txn) error {
			keybyte := make([]byte, 8)
			keyNo := uint64(i)
			binary.BigEndian.PutUint64(keybyte, keyNo)
			return txn.Delete(keybyte)
		})
		require.Nil(t, err, "error while deleting keys")
	}

	// verify while iteration count of keys should be 0
	err = db.View(func(txn *Txn) error {
		keysCount := 0
		itrOps := DefaultIteratorOptions
		it := txn.NewIterator(itrOps)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keysCount++
		}
		require.Zero(t, keysCount, "count of keys should be 0")
		return nil
	})

	require.Nil(t, err, "error should be nil while iterating")
}

func TestStreamWriter3(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// write entries using stream writer
	noOfKeys := 1000
	valueSize := 128

	// insert keys which are even
	value := make([]byte, valueSize)
	y.Check2(rand.Read(value))
	list := &pb.KVList{}
	counter := 0
	for i := 0; i < noOfKeys; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(counter))
		list.Kv = append(list.Kv, &pb.KV{
			Key:     key,
			Value:   value,
			Version: 20,
		})
		counter = counter + 2
	}

	sw := db.NewStreamWriter(8)
	require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
	require.NoError(t, sw.Write(list), "sw.Write() failed")
	require.NoError(t, sw.Done(), "sw.Done() failed")

	// insert keys which are odd
	val := make([]byte, valueSize)
	y.Check2(rand.Read(val))
	counter = 1
	for i := 0; i < noOfKeys; i++ {
		err = db.Update(func(txn *Txn) error {
			keybyte := make([]byte, 8)
			keyNo := uint64(counter)
			binary.BigEndian.PutUint64(keybyte, keyNo)
			return txn.Set(keybyte, val)
		})
		require.Nil(t, err, "error while deleting keys")
		counter = counter + 2
	}

	// verify while iteration keys are in sorted order
	err = db.View(func(txn *Txn) error {
		keysCount := 0
		itrOps := DefaultIteratorOptions
		it := txn.NewIterator(itrOps)
		defer it.Close()
		prev := uint64(0)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			current := binary.BigEndian.Uint64(key)
			if prev != 0 && current != (prev+uint64(1)) {
				t.Fatalf(" keys should be in increasing order")
			}
			keysCount++
			prev = current
		}
		require.True(t, keysCount == 2*noOfKeys, "count of keys is not matching")
		return nil
	})

	require.Nil(t, err, "error should be nil while iterating")
}
