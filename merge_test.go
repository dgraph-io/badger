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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetMergeOperator(t *testing.T) {
	t.Run("Key exists in the DB", func(t *testing.T) {
		t.Run("get without add", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				txnSet(t, db, []byte("fooKey"), []byte("11"), 0x00)
				txnSet(t, db, []byte("fooKey1"), []byte("22"), 0x00)
				// Merge function to merge two byte slices
				add := func(originalValue, newValue []byte) []byte {
					return append(originalValue, newValue...)
				}
				// Create a merge operator using "foo" prefix
				m := db.GetMergeOperator([]byte("foo"), add, 200*time.Millisecond)
				defer m.Stop()

				value, err := m.Get()
				require.Nil(t, err)
				// fooKey and fooKey1 should be merged
				require.Equal(t, "1122", string(value))
			})
		})
		t.Run("add and get", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				txnSet(t, db, []byte("barKey"), []byte("11"), 0x00)
				txnSet(t, db, []byte("barKey1"), []byte("22"), 0x00)
				// Merge function to merge two byte slices
				add := func(originalValue, newValue []byte) []byte {
					return append(originalValue, newValue...)
				}
				// Create a merge operator using "foo" prefix
				m := db.GetMergeOperator([]byte("bar"), add, 200*time.Millisecond)
				defer m.Stop()

				require.Nil(t, m.Add([]byte("A")))
				require.Nil(t, m.Add([]byte("B")))
				require.Nil(t, m.Add([]byte("C")))
				value, err := m.Get()
				require.Nil(t, err)
				// fooKey and fooKey1 should be merged
				require.Equal(t, "1122ABC", string(value))
			})
		})
	})
	t.Run("Key doesn't exist in the DB", func(t *testing.T) {
		t.Run("get should return error", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				// Merge function to merge two byte slices
				add := func(originalValue, newValue []byte) []byte {
					return append(originalValue, newValue...)
				}
				// Create a merge operator using "foo" prefix
				m := db.GetMergeOperator([]byte("foo"), add, 200*time.Millisecond)
				defer m.Stop()

				value, err := m.Get()
				// MergeOperator should return key not found error
				require.Error(t, err)
				require.Nil(t, value)
			})
		})
		t.Run("add and get", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				// Merge function to merge two byte slices
				add := func(originalValue, newValue []byte) []byte {
					return append(originalValue, newValue...)
				}
				// Create a merge operator using "fooprefix" prefix. fooprefix doesn't exists in DB
				m := db.GetMergeOperator([]byte("fooprefix"), add, 200*time.Millisecond)
				defer m.Stop()
				require.Nil(t, m.Add([]byte("1")))
				require.Nil(t, m.Add([]byte("2")))
				require.Nil(t, m.Add([]byte("3")))

				require.NoError(t, db.View(func(txn *Txn) error {
					val, err := txn.Get([]byte("fooprefix"))
					require.NoError(t, err)
					// Version should be 1 since all the other additions aren't
					// stored in the database
					require.Equal(t, uint64(1), val.Version())
					val.Value(func(v []byte) error {
						require.Equal(t, "3", string(v))
						return nil
					})
					return nil
				}))

				value, err := m.Get()
				// MergeOperator should return key not found error
				require.Nil(t, err)
				require.Equal(t, "123", string(value))
			})
		})
	})
}
