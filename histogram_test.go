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

	"github.com/stretchr/testify/require"
)

func TestBuildKeyValueSizeHistogram(t *testing.T) {
	t.Run("All same size key-values", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			entries := int64(40)
			err := db.Update(func(txn *Txn) error {
				for i := int64(0); i < entries; i++ {
					err := txn.Set([]byte(string(i)), []byte("B"))
					if err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)

			histogram := db.buildHistogram(nil)
			keyHistogram := histogram.keySizeHistogram
			valueHistogram := histogram.valueSizeHistogram

			require.Equal(t, entries, keyHistogram.totalCount)
			require.Equal(t, entries, valueHistogram.totalCount)

			// Each entry is of size one. So the sum of sizes should be the same
			// as number of entries
			require.Equal(t, entries, valueHistogram.sum)
			require.Equal(t, entries, keyHistogram.sum)

			// All value sizes are same. The first bin should have all the values.
			require.Equal(t, entries, valueHistogram.countPerBin[0])
			require.Equal(t, entries, keyHistogram.countPerBin[0])

			require.Equal(t, int64(1), keyHistogram.max)
			require.Equal(t, int64(1), keyHistogram.min)
			require.Equal(t, int64(1), valueHistogram.max)
			require.Equal(t, int64(1), valueHistogram.min)
		})
	})

	t.Run("different size key-values", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			entries := int64(3)
			err := db.Update(func(txn *Txn) error {
				if err := txn.Set([]byte("A"), []byte("B")); err != nil {
					return err
				}

				if err := txn.Set([]byte("AA"), []byte("BB")); err != nil {
					return err
				}

				if err := txn.Set([]byte("AAA"), []byte("BBB")); err != nil {
					return err
				}
				return nil
			})
			require.NoError(t, err)

			histogram := db.buildHistogram(nil)
			keyHistogram := histogram.keySizeHistogram
			valueHistogram := histogram.valueSizeHistogram

			require.Equal(t, entries, keyHistogram.totalCount)
			require.Equal(t, entries, valueHistogram.totalCount)

			// Each entry is of size one. So the sum of sizes should be the same
			// as number of entries
			require.Equal(t, int64(6), valueHistogram.sum)
			require.Equal(t, int64(6), keyHistogram.sum)

			// Lenght 1 key is in first bucket, length 2 and 3 are in the second
			// bucket
			require.Equal(t, int64(1), valueHistogram.countPerBin[0])
			require.Equal(t, int64(2), valueHistogram.countPerBin[1])
			require.Equal(t, int64(1), keyHistogram.countPerBin[0])
			require.Equal(t, int64(2), keyHistogram.countPerBin[1])

			require.Equal(t, int64(3), keyHistogram.max)
			require.Equal(t, int64(1), keyHistogram.min)
			require.Equal(t, int64(3), valueHistogram.max)
			require.Equal(t, int64(1), valueHistogram.min)
		})
	})
}
