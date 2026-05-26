/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRegressionHasPrefixShortPrefixFallback covers the hasPrefix fallback
// branch where the user-supplied prefix is longer than userKey (len(p) >
// len(key)-8). The short-circuit "len(key) >= len(p)+8" must be false so we
// take the ParseKey path and correctly return false (no spurious match
// against ts bytes).
func TestRegressionHasPrefixShortPrefixFallback(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// userKey "z" sorts AFTER prefix "yy", so Seek(prefix) lands on it
		// and hasPrefix is actually called. The internal key is "z"+8 ts
		// bytes (9 bytes), strictly shorter than prefix(2)+8=10, forcing
		// the ParseKey fallback branch.
		txnSet(t, db, []byte("z"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.Prefix = []byte("yy") // 2 bytes, longer than userKey "z"
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			require.Equal(t, 0, count, "no key should match an over-long prefix")
			return nil
		}))
	})
}

// TestRegressionHasPrefixFastPath covers the optimized hasPrefix path where
// len(prefix) fits within userKey: it must still correctly identify matching
// and non-matching keys, equivalent to the old ParseKey-based check.
func TestRegressionHasPrefixFastPath(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("alpha/1"), []byte("v"), 0)
		txnSet(t, db, []byte("alpha/2"), []byte("v"), 0)
		txnSet(t, db, []byte("beta/1"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.Prefix = []byte("alpha/")
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				require.True(t, bytes.HasPrefix(it.Item().Key(), opt.Prefix))
				count++
			}
			require.Equal(t, 2, count, "alpha/ should match both alpha/1 and alpha/2")
			return nil
		}))
	})
}
