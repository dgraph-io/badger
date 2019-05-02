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
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteBatch(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}

	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		N, M := 50000, 1000
		start := time.Now()

		for i := 0; i < N; i++ {
			require.NoError(t, wb.Set(key(i), val(i), 0))
		}
		for i := 0; i < M; i++ {
			require.NoError(t, wb.Delete(key(i)))
		}
		require.NoError(t, wb.Flush())
		t.Logf("Time taken for %d writes (w/ test options): %s\n", N+M, time.Since(start))

		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			defer itr.Close()

			i := M
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, string(key(i)), string(item.Key()))
				valcopy, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, val(i), valcopy)
				i++
			}
			require.Equal(t, N, i)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestWriteBatchCompaction(t *testing.T) {
	dir, err := ioutil.TempDir(".", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.ValueDir = dir
	opts.Dir = dir

	db, err := Open(opts)
	require.NoError(t, err)

	wb := db.NewWriteBatch()
	entries := 10000
	for i := 0; i < entries; i++ {
		require.Nil(t, wb.Set([]byte(fmt.Sprintf("foo%d", i)), []byte("bar"), 0))
	}
	require.Nil(t, wb.Flush())

	wb = db.NewWriteBatch()
	// Delete 50% of the entries
	for i := 0; i < entries/2; i++ {
		require.Nil(t, wb.Delete([]byte(fmt.Sprintf("foo%d", i))))
	}
	require.Nil(t, wb.Flush())

	// It is necessary that we call db.Update(..) before compaction so that the db.orc.readMark
	// value is incremented. The transactions in WriteBatch call do not increment the
	// db.orc.readMark value and hence compaction wouldn't discard any entries added by write batch
	// if we do not increment the db.orc.readMark value
	require.Nil(t, db.Update(func(txn *Txn) error {
		txn.Set([]byte("key1"), []byte("val1"))
		return nil
	}))

	// Close DB to force compaction
	require.Nil(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()

	iopt := DefaultIteratorOptions
	iopt.AllVersions = true
	txn := db.NewTransaction(false)
	defer txn.Discard()
	it := txn.NewIterator(iopt)
	defer it.Close()
	countAfterCompaction := 0
	for it.Rewind(); it.Valid(); it.Next() {
		countAfterCompaction++
	}
	// We have deleted 50% of the keys
	require.Less(t, countAfterCompaction, entries+entries/2)
}
