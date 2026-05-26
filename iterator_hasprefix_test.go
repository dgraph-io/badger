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
		// Single short key — userKey is 1 byte.
		txnSet(t, db, []byte("a"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			// Prefix is longer than the single existing userKey "a". The
			// optimized hasPrefix must take the ParseKey fallback (because
			// len(key) < len(prefix)+8) and return false — no key matches.
			opt.Prefix = []byte("abcdefghij") // 10 bytes, longer than "a"
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
