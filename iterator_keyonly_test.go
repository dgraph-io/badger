/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/stretchr/testify/require"
)

// TestKeyOnlyIterator_ValueReturnsErrKeyOnlyMode covers iterator.go:Item.Value
// short-circuit when item.keyOnly is set.
func TestKeyOnlyIterator_ValueReturnsErrKeyOnlyMode(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error { return nil })
				require.True(t, errors.Is(err, ErrKeyOnlyMode),
					"Value() should return ErrKeyOnlyMode, got %v", err)
			}
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ValueCopyReturnsErrKeyOnlyMode covers iterator.go:Item.ValueCopy.
func TestKeyOnlyIterator_ValueCopyReturnsErrKeyOnlyMode(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				buf, err := item.ValueCopy(nil)
				require.True(t, errors.Is(err, ErrKeyOnlyMode))
				require.Nil(t, buf)
			}
			return nil
		}))
	})
}

// TestKeyOnlyIterator_EstimatedSizeReturnsKeyLen covers iterator.go:Item.EstimatedSize
// short-circuit (returns int64(len(item.key)) when keyOnly).
func TestKeyOnlyIterator_EstimatedSizeReturnsKeyLen(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("abc"), []byte("lots-of-value-bytes-here"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			item := it.Item()
			require.Equal(t, int64(len("abc")), item.EstimatedSize())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ValueSizeIsZero covers iterator.go:Item.ValueSize short-circuit.
func TestKeyOnlyIterator_ValueSizeIsZero(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("hello-world"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			require.Equal(t, int64(0), it.Item().ValueSize())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ForcesPrefetchOff covers iterator.go:NewIterator forcing
// PrefetchValues=false when KeyOnly=true.
func TestKeyOnlyIterator_ForcesPrefetchOff(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			opt.PrefetchValues = true
			opt.PrefetchSize = 100
			it := txn.NewIterator(opt)
			defer it.Close()
			require.False(t, it.opt.PrefetchValues,
				"NewIterator must force PrefetchValues=false when KeyOnly=true")
			it.Rewind()
			require.True(t, it.Valid())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_KeyMetaVersionWork verifies that the metadata methods on Item
// still work correctly in KeyOnly mode.
func TestKeyOnlyIterator_KeyMetaVersionWork(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("hello")
		val := []byte("world")
		txn := db.NewTransaction(true)
		require.NoError(t, txn.SetEntry(NewEntry(key, val).WithMeta(0x42)))
		require.NoError(t, txn.Commit())

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			item := it.Item()
			require.Equal(t, key, item.Key())
			require.Equal(t, byte(0x42), item.UserMeta())
			require.NotZero(t, item.Version())
			require.False(t, item.IsDeletedOrExpired())
			require.Equal(t, int64(len(key)), item.KeySize())
			require.False(t, item.DiscardEarlierVersions())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_PrefetchValuesFalseStillWorks covers fill() KeyOnly branch
// without prefetch (the actual hot path; PrefetchValues=false bypasses the
// prefetch goroutine entirely).
func TestKeyOnlyIterator_PrefetchValuesFalseStillWorks(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 20; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("k%02d", i)), []byte("vvv"), 0)
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NotEmpty(t, item.Key())
				err := item.Value(func(v []byte) error { return nil })
				require.True(t, errors.Is(err, ErrKeyOnlyMode))
				count++
			}
			require.Equal(t, 20, count)
			return nil
		}))
	})
}

// TestPrecludesInternalKeys is the table-driven unit test for the moved helper
// (was a free function in the PR, now a method on IteratorOptions).
func TestPrecludesInternalKeys(t *testing.T) {
	cases := []struct {
		name   string
		prefix []byte
		want   bool
	}{
		{"empty prefix sees everything", nil, false},
		{"empty slice sees everything", []byte{}, false},
		{"prefix starting with '!' (badgerPrefix[0]) overlaps", []byte("!badger"), false},
		{"prefix starting with 0x21 (== '!') overlaps", []byte{0x21, 0xff}, false},
		{"prefix starting with 0x00 cannot overlap", []byte{0x00, 0x01, 0x02}, true},
		{"prefix starting with 'a' cannot overlap", []byte("abc"), true},
		{"prefix starting with high byte cannot overlap", []byte{0xff}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := DefaultIteratorOptions
			opt.Prefix = tc.prefix
			got := opt.precludesInternalKeys()
			require.Equal(t, tc.want, got,
				"prefix=%x precludesInternalKeys=%v want=%v", tc.prefix, got, tc.want)
		})
	}
}

// TestRegressionSameKeyDedup locks in the user-key-only lastKey behavior: when
// multiple versions of the same user-key exist and AllVersions=false, the
// iterator must surface exactly one item per user-key (the freshest
// non-expired one).
func TestRegressionSameKeyDedup(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for _, k := range []string{"k1", "k2"} {
			for v := 1; v <= 3; v++ {
				txnSet(t, db, []byte(k), []byte(fmt.Sprintf("v%d", v)), 0)
			}
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			var seen []string
			for it.Rewind(); it.Valid(); it.Next() {
				seen = append(seen, string(it.Item().Key()))
			}
			require.Equal(t, []string{"k1", "k2"}, seen,
				"non-AllVersions iterator must dedup to one item per user-key")
			return nil
		}))

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.AllVersions = true
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			counts := map[string]int{}
			for it.Rewind(); it.Valid(); it.Next() {
				counts[string(it.Item().Key())]++
			}
			require.Equal(t, 3, counts["k1"])
			require.Equal(t, 3, counts["k2"])
			return nil
		}))
	})
}

// TestRegressionInternalKeysHidden locks in: a default user iterator (no
// InternalAccess) must not surface badger-internal keys even when the
// optimized precludesInternalKeys path is taken.
func TestRegressionInternalKeysHidden(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("user-key"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			it := txn.NewIterator(DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				k := it.Item().Key()
				require.False(t, isBadgerInternalKey(k),
					"default iterator surfaced internal key %q", k)
			}
			return nil
		}))
	})
}

func isBadgerInternalKey(k []byte) bool {
	if len(k) < len(badgerPrefix) {
		return false
	}
	for i := range badgerPrefix {
		if k[i] != badgerPrefix[i] {
			return false
		}
	}
	return true
}

// TestRegressionIsBannedNoBans locks in the fast-path correctness: when no
// namespaces have been banned, isBanned must return nil for any key.
func TestRegressionIsBannedNoBans(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		require.False(t, db.bannedNamespaces.hasAny.Load())

		key := y.KeyWithTs([]byte("hello-world-12345"), 1)
		require.NoError(t, db.isBanned(key))

		require.NoError(t, db.isBanned(nil))
		require.NoError(t, db.isBanned([]byte("x")))
	})
}

// TestRegressionIsBannedWithBan covers the slow-path: hasAny=true, key matches
// a banned namespace.
func TestRegressionIsBannedWithBan(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const ns = uint64(0x4242)
		require.NoError(t, db.BanNamespace(ns))
		require.True(t, db.bannedNamespaces.hasAny.Load(),
			"hasAny must flip to true after add()")

		banned := y.KeyWithTs(append(y.U64ToBytes(ns), []byte("suffix")...), 1)
		require.ErrorIs(t, db.isBanned(banned), ErrBannedKey)

		other := y.KeyWithTs(append(y.U64ToBytes(0x1111), []byte("suffix")...), 1)
		require.NoError(t, db.isBanned(other))
	})
}

// TestRegressionIsBannedShortKeyWithBans covers the slow path early-return when
// hasAny=true but the key is too short to extract a namespace.
func TestRegressionIsBannedShortKeyWithBans(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const ns = uint64(0x4242)
		require.NoError(t, db.BanNamespace(ns))

		require.NoError(t, db.isBanned([]byte("short")))
		require.NoError(t, db.isBanned(nil))
	})
}

// TestRegressionIsBannedNegativeOffset covers the early-return: NamespaceOffset<0
// short-circuits before any atomic load.
func TestRegressionIsBannedNegativeOffset(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = -1
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		require.NoError(t, db.isBanned([]byte("anything")))
		require.NoError(t, db.isBanned(nil))
	})
}

// TestRegressionFillNonKeyOnly exercises the non-KeyOnly fill path, verifying
// that values are still copied correctly when KeyOnly=false.
func TestRegressionFillNonKeyOnly(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("expected-value-bytes"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			got, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("expected-value-bytes"), got)
			return nil
		}))
	})
}

// TestRegressionPrefetchValuesTrue exercises the prefetch goroutine path,
// confirming KeyOnly=false + PrefetchValues=true still works.
func TestRegressionPrefetchValuesTrue(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 5; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)), 0)
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = true
			opt.PrefetchSize = 10
			it := txn.NewIterator(opt)
			defer it.Close()
			seen := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				v, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.NotEmpty(t, v)
				seen++
			}
			require.Equal(t, 5, seen)
			return nil
		}))
	})
}

// TestSameUserKey exercises the new y.SameUserKey helper. It is a thin wrapper
// around bytes.Equal that callers use when they have already stripped the 8-byte
// ts suffix; the test pins the contract so a future change cannot regress.
func TestSameUserKey(t *testing.T) {
	require.True(t, y.SameUserKey(nil, nil))
	require.True(t, y.SameUserKey([]byte{}, []byte{}))
	require.True(t, y.SameUserKey([]byte("abc"), []byte("abc")))
	require.False(t, y.SameUserKey([]byte("abc"), []byte("abd")))
	require.False(t, y.SameUserKey([]byte("abc"), []byte("ab")))
	require.False(t, y.SameUserKey([]byte("ab"), []byte("abc")))
	require.False(t, y.SameUserKey([]byte("abc"), nil))
}
