/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
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
				for i := rune(0); i < rune(entries); i++ {
					err := txn.SetEntry(NewEntry([]byte(string(i)), []byte("B")))
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
				if err := txn.SetEntry(NewEntry([]byte("A"), []byte("B"))); err != nil {
					return err
				}

				if err := txn.SetEntry(NewEntry([]byte("AA"), []byte("BB"))); err != nil {
					return err
				}

				return txn.SetEntry(NewEntry([]byte("AAA"), []byte("BBB")))
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

			// Length 1 key is in first bucket, length 2 and 3 are in the second
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
