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
		unsubscribe, err := db.Subscribe([]byte("ke"), func(kvs *pb.KVList) {
			atomic.AddInt32(&numUpdates, int32(len(kvs.GetKv())))
		})
		if err != nil {
			require.NoError(t, err)
		}
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key1"), []byte("value1"))
		})
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key2"), []byte("value2"))
		})
		db.Update(func(txn *Txn) error {
			return txn.Set([]byte("key3"), []byte("value3"))
		})
		time.Sleep(1 * time.Millisecond)
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
		unsub, err := db.Subscribe([]byte("ke"), func(kvs *pb.KVList) {
			for _, kv := range kvs.GetKv() {
				order = append(order, string(kv.Value))
			}
		})
		if err != nil {
			require.NoError(t, err)
		}
		for i := 0; i < 5; i++ {
			db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			})
		}
		time.Sleep(1 * time.Millisecond)
		unsub()
		for i := 0; i < 5; i++ {
			require.Equal(t, fmt.Sprintf("value%d", i), order[i])
		}
	})
}

func TestPublisherBatching(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var once sync.Once
		first := false
		unsub, err := db.Subscribe([]byte("ke"), func(kvs *pb.KVList) {
			once.Do(func() {
				time.Sleep(time.Second * 10)
				first = true
			})
			if first {
				first = false
				return
			}
			require.Equal(t, 99, len(kvs.GetKv()))
		})
		if err != nil {
			require.NoError(t, err)
		}

		for i := 0; i < 100; i++ {
			start := time.Now()
			db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			})
			finish := time.Now()
			require.Less(t, finish.Sub(start).Seconds(), float64(1))
		}
		unsub()
	})
}
