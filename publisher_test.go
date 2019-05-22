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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/pb"
)

func TestPublisherOrdering(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		order := []string{}
		var wg sync.WaitGroup
		wg.Add(1)
		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			subWg.Done()
			updates := 0
			err := db.Subscribe(context.Background(), func(kvs *pb.KVList) {
				updates += len(kvs.GetKv())
				for _, kv := range kvs.GetKv() {
					order = append(order, string(kv.Value))
				}
				if updates == 5 {
					wg.Done()
				}
			}, []byte("ke"))
			if err != nil {
				require.Equal(t, err.Error(), context.Canceled.Error())
			}
		}()
		subWg.Wait()
		for i := 0; i < 5; i++ {
			db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%d", i)),
					[]byte(fmt.Sprintf("value%d", i))))
			})
		}
		wg.Wait()
		for i := 0; i < 5; i++ {
			require.Equal(t, fmt.Sprintf("value%d", i), order[i])
		}
	})
}

func TestMultiplePrefix(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		wg.Add(1)
		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			subWg.Done()
			updates := 0
			err := db.Subscribe(context.Background(), func(kvs *pb.KVList) {
				updates += len(kvs.GetKv())
				for _, kv := range kvs.GetKv() {
					if string(kv.Key) == "key" {
						require.Equal(t, string(kv.Value), "value")
					} else {
						require.Equal(t, string(kv.Value), "badger")
					}
				}
				if updates == 2 {
					wg.Done()
				}
			}, []byte("ke"), []byte("hel"))
			if err != nil {
				require.Equal(t, err.Error(), context.Canceled.Error())
			}
		}()
		subWg.Wait()
		db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte("key"), []byte("value")))
		})
		db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte("hello"), []byte("badger")))
		})
		wg.Wait()
	})
}
