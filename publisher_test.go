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
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/pb"
)

func TestSubscribe(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var numUpdates int32
		numUpdates = 0
		unsubscribe := db.Subscribe("ke", func(kv *pb.KV) {
			atomic.AddInt32(&numUpdates, 1)
		})
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key1"), []byte("value1"))
		})
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key2"), []byte("value2"))
		})
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key3"), []byte("value3"))
		})
		unsubscribe()
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key4"), []byte("value4"))
		})
		require.Equal(t, int32(3), numUpdates)
	})
}

func TestPublisherOrdering(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		order := []string{}
		unsub := db.Subscribe("ke", func(kv *pb.KV) {
			order = append(order, string(kv.Value))
		})
		for i := 0; i < 5; i++ {
			db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			})
		}
		unsub()
		for i := 0; i < 5; i++ {
			require.Equal(t, fmt.Sprintf("value%d", i), order[i])
		}
	})
}

func TestBlockingPublish(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var once sync.Once
		var numUpdates int32
		numUpdates = 0
		unsub := db.Subscribe("ke", func(kv *pb.KV) {
			once.Do(func() {
				time.Sleep(time.Second * 10)
			})
			atomic.AddInt32(&numUpdates, 1)
		})

		for i := 0; i < 100; i++ {
			start := time.Now()
			db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			})
			finish := time.Now()
			require.Less(t, finish.Sub(start).Seconds(), float64(1))
		}
		unsub()
		require.Equal(t, int32(100), numUpdates)
	})
}

func TestMaxBatch(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	opts.MaxPendingSubscriberUpdates = 100
	runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
		var once sync.Once
		var numUpdates int32
		numUpdates = 0
		unsub := db.Subscribe("ke", func(kv *pb.KV) {
			once.Do(func() {
				time.Sleep(time.Second * 10)
			})
			atomic.AddInt32(&numUpdates, 1)
		})

		for i := 0; i < 200; i++ {
			db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			})
		}
		unsub()
		// we're block after 1'st msg. toatal update count is 101
		require.Equal(t, int32(101), numUpdates)
	})
}
