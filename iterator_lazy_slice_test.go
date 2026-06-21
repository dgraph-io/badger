/*
 * SPDX-FileCopyrightText: © 2017-2025 Dgraph Labs, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRegressionLazyItemSliceValueRead exercises Item.Value/ValueCopy on
// items produced by an iterator that uses the lazy-slice allocation path
// (newItem no longer eagerly allocates y.Slice). yieldItemValue and
// prefetchValue must lazy-init item.slice on first use; if they don't,
// item.slice.Resize() will nil-deref.
//
// Covers three configurations that previously relied on the eager
// allocation: PrefetchValues=true (prefetch goroutine), PrefetchValues=false
// + Item.Value() (synchronous read), and ValueCopy on a lazy item.
func TestRegressionLazyItemSliceValueRead(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Two keys with distinct, non-empty values so the value path is
		// actually traversed (deletes/empty values exit yieldItemValue
		// before touching slice).
		txnSet(t, db, []byte("k1"), []byte("value-one"), 0)
		txnSet(t, db, []byte("k2"), []byte("value-two-larger"), 0)

		// Variant A: PrefetchValues=true triggers the prefetch goroutine
		// path, which calls yieldItemValue → item.slice.Resize.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = true
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NoError(t, item.Value(func(val []byte) error {
					require.NotEmpty(t, val, "prefetched value must be non-empty")
					return nil
				}))
				count++
			}
			require.Equal(t, 2, count)
			return nil
		}))

		// Variant B: PrefetchValues=false + synchronous Item.Value(). Item
		// is produced with slice=nil; yieldItemValue must initialize it.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NoError(t, item.Value(func(val []byte) error {
					require.NotEmpty(t, val)
					return nil
				}))
				count++
			}
			require.Equal(t, 2, count)
			return nil
		}))

		// Variant C: ValueCopy on a lazy item — separate code path through
		// yieldItemValue → SafeCopy.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			buf, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.NotEmpty(t, buf)
			return nil
		}))
	})
}
