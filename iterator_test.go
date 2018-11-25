/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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
	"testing"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func TestPickTables(t *testing.T) {
	opt := DefaultIteratorOptions

	within := func(prefix, left, right string) {
		opt.Prefix = []byte(prefix)
		require.True(t, opt.PickTable([]byte(left), []byte(right)))
	}
	outside := func(prefix, left, right string) {
		opt.Prefix = []byte(prefix)
		require.False(t, opt.PickTable([]byte(left), []byte(right)))
	}
	within("abc", "ab", "ad")
	within("abc", "abc", "ad")
	within("abc", "abb123", "ad")
	within("abc", "abc123", "abd234")
	within("abc", "abc123", "abc456")

	outside("abd", "abe", "ad")
	outside("abd", "ac", "ad")
	outside("abd", "b", "e")
	outside("abd", "a", "ab")
	outside("abd", "ab", "abc")
	outside("abd", "ab", "abc123")
}

func TestIteratePrefix(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		bkey := func(i int) []byte {
			return []byte(fmt.Sprintf("%04d", i))
		}
		val := []byte("OK")
		n := 10000

		batch := db.NewWriteBatch()
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				t.Logf("Put i=%d\n", i)
			}
			require.NoError(t, batch.Set(bkey(i), val, 0))
		}
		require.NoError(t, batch.Flush())

		countKeys := func(prefix string) int {
			t.Logf("Testing with prefix: %s", prefix)
			var count int
			opt := DefaultIteratorOptions
			opt.Prefix = []byte(prefix)
			err := db.View(func(txn *Txn) error {
				itr := txn.NewIterator(opt)
				defer itr.Close()
				for itr.Rewind(); itr.Valid(); itr.Next() {
					item := itr.Item()
					err := item.Value(func(v []byte) error {
						require.Equal(t, val, v)
						return nil
					})
					require.NoError(t, err)
					require.True(t, bytes.HasPrefix(item.Key(), opt.Prefix))
					count++
				}
				return nil
			})
			require.NoError(t, err)
			return count
		}

		for i := 0; i <= 9; i++ {
			require.Equal(t, 1, countKeys(fmt.Sprintf("%d%d%d%d", i, i, i, i)))
			require.Equal(t, 10, countKeys(fmt.Sprintf("%d%d%d", i, i, i)))
			require.Equal(t, 100, countKeys(fmt.Sprintf("%d%d", i, i)))
			require.Equal(t, 1000, countKeys(fmt.Sprintf("%d", i)))
			require.Equal(t, 10000, countKeys(""))
		}
	})
}

func BenchmarkIteratePrefixSingleKey(b *testing.B) {
	dir, err := ioutil.TempDir(".", "badger-test")
	y.Check(err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	db, err := Open(opts)
	y.Check(err)
	defer db.Close()

	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%04d", i))
	}
	val := []byte("OK")
	n := 10000

	batch := db.NewWriteBatch()
	for i := 0; i < n; i++ {
		y.Check(batch.Set(bkey(i), val, 0))
	}
	y.Check(batch.Flush())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := bkey(i % n)
		err := db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.Prefix = key
			opt.AllVersions = true

			itr := txn.NewIterator(opt)
			defer itr.Close()

			var count int
			for itr.Rewind(); itr.Valid(); itr.Next() {
				count++
			}
			if count != 1 {
				b.Fatalf("Count must be one for key: %s", key)
			}
			return nil
		})
		if err != nil {
			b.Fatalf("Error while View: %v", err)
		}
	}
}
