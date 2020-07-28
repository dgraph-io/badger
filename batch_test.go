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

	test := func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		// Sanity check for SetEntryAt.
		require.Error(t, wb.SetEntryAt(&Entry{}, 12))

		N, M := 50000, 1000
		start := time.Now()

		for i := 0; i < N; i++ {
			require.NoError(t, wb.Set(key(i), val(i)))
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
	}
	t.Run("disk mode", func(t *testing.T) {
		opt := getTestOptions("")
		opt.VlogOnlyWAL = false

		// Set value threshold to 32 bytes otherwise write batch will generate
		// too many files and we will crash with too many files open error.
		opt.ValueThreshold = 32
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
	t.Run("InMemory mode", func(t *testing.T) {
		opt := getTestOptions("")
		opt.InMemory = true
		db, err := Open(opt)
		require.NoError(t, err)
		test(t, db)
		require.NoError(t, db.Close())
	})
}

// This test ensures we don't end up in deadlock in case of empty writebatch.
func TestEmptyWriteBatch(t *testing.T) {
	t.Run("normal mode", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			wb := db.NewWriteBatch()
			require.NoError(t, wb.Flush())
			wb = db.NewWriteBatch()
			require.NoError(t, wb.Flush())
			wb = db.NewWriteBatch()
			require.NoError(t, wb.Flush())
		})
	})
	t.Run("managed mode", func(t *testing.T) {
		opt := getTestOptions("")
		opt.managedTxns = true
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			t.Run("WriteBatchAt", func(t *testing.T) {
				wb := db.NewWriteBatchAt(2)
				require.NoError(t, wb.Flush())
				wb = db.NewWriteBatchAt(208)
				require.NoError(t, wb.Flush())
				wb = db.NewWriteBatchAt(31)
				require.NoError(t, wb.Flush())
			})
			t.Run("ManagedWriteBatch", func(t *testing.T) {
				wb := db.NewManagedWriteBatch()
				require.NoError(t, wb.Flush())
				wb = db.NewManagedWriteBatch()
				require.NoError(t, wb.Flush())
				wb = db.NewManagedWriteBatch()
				require.NoError(t, wb.Flush())
			})
		})
	})
}
