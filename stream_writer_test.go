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
	"fmt"
	"math"
	"math/rand"
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
	normalModeOpts := DefaultOptions
	managedModeOpts := DefaultOptions
	managedModeOpts.managedTxns = true

	for _, opts := range []*Options{&normalModeOpts, &managedModeOpts} {
		runBadgerTest(t, opts, func(t *testing.T, db *DB) {
			// write entries using stream writer
			noOfKeys := 1000
			valueSize := 128
			list := getSortedKVList(valueSize, noOfKeys)
			sw := db.NewStreamWriter()
			require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
			require.NoError(t, sw.Write(list), "sw.Write() failed")
			require.NoError(t, sw.Flush(), "sw.Flush() failed")

			err := db.View(func(txn *Txn) error {
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
		})
	}
}

// write more keys to db after writing keys using stream writer
func TestStreamWriter2(t *testing.T) {
	normalModeOpts := DefaultOptions
	managedModeOpts := DefaultOptions
	managedModeOpts.managedTxns = true

	for _, opts := range []*Options{&normalModeOpts, &managedModeOpts} {
		runBadgerTest(t, opts, func(t *testing.T, db *DB) {
			// write entries using stream writer
			noOfKeys := 1000
			valueSize := 128
			list := getSortedKVList(valueSize, noOfKeys)
			sw := db.NewStreamWriter()
			require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
			require.NoError(t, sw.Write(list), "sw.Write() failed")
			// get max version of sw, will be used in transactions for managed mode
			maxVs := sw.maxVersion
			require.NoError(t, sw.Flush(), "sw.Flush() failed")

			// delete all the inserted keys
			val := make([]byte, valueSize)
			y.Check2(rand.Read(val))
			for i := 0; i < noOfKeys; i++ {
				txn := db.newTransaction(true, opts.managedTxns)
				if opts.managedTxns {
					txn.readTs = math.MaxUint64
					txn.commitTs = maxVs
				}
				keybyte := make([]byte, 8)
				keyNo := uint64(i)
				binary.BigEndian.PutUint64(keybyte, keyNo)
				require.NoError(t, txn.Delete(keybyte), "error while deleting keys")
				require.NoError(t, txn.Commit(), "error while commit")
			}

			// verify while iteration count of keys should be 0
			err := db.View(func(txn *Txn) error {
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
		})
	}
}

func TestStreamWriter3(t *testing.T) {
	normalModeOpts := DefaultOptions
	managedModeOpts := DefaultOptions
	managedModeOpts.managedTxns = true

	for _, opts := range []*Options{&normalModeOpts, &managedModeOpts} {
		runBadgerTest(t, opts, func(t *testing.T, db *DB) {
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

			sw := db.NewStreamWriter()
			require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
			require.NoError(t, sw.Write(list), "sw.Write() failed")
			// get max version of sw, will be used in transactions for managed mode
			maxVs := sw.maxVersion
			require.NoError(t, sw.Flush(), "sw.Flush() failed")

			// insert keys which are odd
			val := make([]byte, valueSize)
			y.Check2(rand.Read(val))
			counter = 1
			for i := 0; i < noOfKeys; i++ {
				txn := db.newTransaction(true, opts.managedTxns)
				if opts.managedTxns {
					txn.readTs = math.MaxUint64
					txn.commitTs = maxVs
				}
				keybyte := make([]byte, 8)
				keyNo := uint64(counter)
				binary.BigEndian.PutUint64(keybyte, keyNo)
				require.NoError(t, txn.Set(keybyte, val), "error while inserting entries")
				require.NoError(t, txn.Commit(), "error while commit")
				counter = counter + 2
			}

			// verify while iteration keys are in sorted order
			err := db.View(func(txn *Txn) error {
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
						t.Fatal("keys should be in increasing order")
					}
					keysCount++
					prev = current
				}
				require.True(t, keysCount == 2*noOfKeys, "count of keys is not matching")
				return nil
			})

			require.Nil(t, err, "error should be nil while iterating")
		})
	}
}

// After inserting all data from streams, StreamWriter reinitializes Oracle and updates its nextTs
// to maxVersion found in all entries inserted(if db is running in non managed mode). It also
// updates Oracle's txnMark and readMark. If Oracle is not reinitialized, it might cause issue
// while updating readMark and txnMark when its nextTs is ahead of maxVersion. This tests verifies
// Oracle reinitialization is happening. Try commenting line 171 in stream_writer.go with code
// (sw.db.orc = newOracle(sw.db.opt), this test should fail.
func TestStreamWriter4(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// first insert some entries in db
		for i := 0; i < 10; i++ {
			err := db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key-%d", i))
				value := []byte(fmt.Sprintf("val-%d", i))
				return txn.Set(key, value)
			})
			require.NoError(t, err, "error while updating db")
		}

		list := &pb.KVList{}
		list.Kv = append(list.Kv, &pb.KV{
			Key:     []byte("key-1"),
			Value:   []byte("value-1"),
			Version: 1,
		})

		sw := db.NewStreamWriter()
		require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
		require.NoError(t, sw.Write(list), "sw.Write() failed")
		require.NoError(t, sw.Flush(), "sw.Flush() failed")
	})
}

func TestStreamWriter5(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		list := &pb.KVList{}

		left := make([]byte, 6)
		left[0] = 0x00
		copy(left[1:], []byte("break"))

		right := make([]byte, 6)
		right[0] = 0xff
		copy(right[1:], []byte("break"))

		list.Kv = append(list.Kv, &pb.KV{
			Key:     left,
			Value:   []byte("val"),
			Version: 1,
		})
		list.Kv = append(list.Kv, &pb.KV{
			Key:     right,
			Value:   []byte("val"),
			Version: 1,
		})

		sw := db.NewStreamWriter()
		require.NoError(t, sw.Prepare(), "sw.Prepare() failed")
		require.NoError(t, sw.Write(list), "sw.Write() failed")
		require.NoError(t, sw.Flush(), "sw.Flush() failed")
		require.NoError(t, db.Close())

		var err error
		_, err = Open(db.opt)
		require.NoError(t, err)
	})
}
