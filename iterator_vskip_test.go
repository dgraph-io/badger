/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

// observed is one row produced by an iterator pass: the user key, the version
// we yielded, and the value. Two iteration passes (seek-skip vs forced
// step-skip) must produce byte-identical sequences of these.
type observed struct {
	key     string
	version uint64
	val     string
}

// collectVSkip runs a full iteration with the given options and returns the
// ordered sequence of items. If forceStep is true, the seek-ahead optimization
// is disabled so the iterator falls back to stepping mi.Next() one version at a
// time (the reference behavior we are differentially testing against).
func collectVSkip(t *testing.T, txn *Txn, opt IteratorOptions, forceStep bool) []observed {
	t.Helper()
	it := txn.NewIterator(opt)
	it.forceStepSkip = forceStep
	defer it.Close()

	var out []observed
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		out = append(out, observed{
			key:     string(item.KeyCopy(nil)),
			version: item.Version(),
			val:     string(val),
		})
	}
	return out
}

func openManagedVSkip(t *testing.T) (*DB, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-vskip")
	require.NoError(t, err)
	opts := getTestOptions(dir)
	opts.managedTxns = true
	db, err := Open(opts)
	require.NoError(t, err)
	return db, func() {
		require.NoError(t, db.Close())
		removeDir(dir)
	}
}

// TestVersionSkipDifferential is the core correctness test for the seek-ahead
// MVCC version skipping. It builds randomized multi-version data (with deleted
// and expired versions) in managed mode with explicit commit timestamps, then
// asserts that the seek-skip iterator produces byte-identical output to the
// forced step-skip reference across a matrix of readTs / SinceTs / prefix /
// reverse / allVersions options.
func TestVersionSkipDifferential(t *testing.T) {
	for seed := 0; seed < 12; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			db, cleanup := openManagedVSkip(t)
			defer cleanup()

			// User keys with overlapping prefixes (e.g. "k1", "k10") to stress
			// nextLexKey ordering, plus single bytes and a 0xFF-tail key.
			userKeys := []string{
				"a", "k1", "k10", "k100", "k2", "k20", "m", "mm", "z",
			}

			var ts uint64 = 1
			for _, uk := range userKeys {
				nver := 1 + rng.Intn(12) // 1..12 versions per key
				for v := 0; v < nver; v++ {
					txn := db.NewTransactionAt(ts, true)
					r := rng.Intn(10)
					switch {
					case r == 0:
						require.NoError(t, txn.Delete([]byte(uk)))
					case r == 1:
						// Expired version (TTL already in the past).
						e := NewEntry([]byte(uk),
							[]byte(fmt.Sprintf("%s-exp", uk))).WithTTL(-1)
						require.NoError(t, txn.SetEntry(e))
					default:
						require.NoError(t, txn.Set([]byte(uk),
							[]byte(fmt.Sprintf("%s-v%d-ts%d", uk, v, ts))))
					}
					require.NoError(t, txn.CommitAt(ts, nil))
					ts++
				}
			}
			maxTs := ts

			// On even seeds, flatten so data spans memtable + multiple LSM levels,
			// exercising the merge/concat iterator seek paths across files.
			if seed%2 == 0 {
				require.NoError(t, db.Flatten(2))
			}

			prefixes := [][]byte{nil, []byte("k"), []byte("k1"), []byte("m"), []byte("zzz")}
			readTses := []uint64{0, 1, maxTs / 3, maxTs / 2, maxTs - 1, maxTs, maxTs + 100}
			sinceTses := []uint64{0, maxTs / 4, maxTs / 2}

			for _, rev := range []bool{false, true} {
				for _, allv := range []bool{false, true} {
					for _, prefix := range prefixes {
						for _, sinceTs := range sinceTses {
							for _, readTs := range readTses {
								opt := DefaultIteratorOptions
								opt.Reverse = rev
								opt.AllVersions = allv
								opt.Prefix = prefix
								opt.SinceTs = sinceTs
								opt.PrefetchValues = true

								txn := db.NewTransactionAt(readTs, false)
								ref := collectVSkip(t, txn, opt, true)
								got := collectVSkip(t, txn, opt, false)
								txn.Discard()

								require.Equalf(t, ref, got,
									"mismatch rev=%v allv=%v prefix=%q sinceTs=%d readTs=%d",
									rev, allv, prefix, sinceTs, readTs)
							}
						}
					}
				}
			}
		})
	}
}

// TestVersionSafePointGet documents and verifies that the existing Txn.Get is
// already a version-safe point read: for a key with a long version chain spread
// across memtable and multiple LSM levels, reading at readTs returns the newest
// version whose commit ts is <= readTs — never a newer version, and never a
// stale one even when an older version happens to sit in a higher table.
//
// This is why no new GetVersioned method is needed: Txn.Get seeks to
// KeyWithTs(key, readTs) and db.get takes the max version found across ALL
// tables (see (*levelHandler).get / (*DB).get), which is exactly the
// newest-visible-version-<=-readTs semantics.
func TestVersionSafePointGet(t *testing.T) {
	db, cleanup := openManagedVSkip(t)
	defer cleanup()

	key := []byte("pointkey")
	// Write versions at ts = 10, 20, 30, ... 100, flattening partway so the
	// versions are deliberately spread across memtable + LSM levels.
	for i := 1; i <= 10; i++ {
		ts := uint64(i * 10)
		txn := db.NewTransactionAt(ts, true)
		require.NoError(t, txn.Set(key, []byte(fmt.Sprintf("val-ts%d", ts))))
		require.NoError(t, txn.CommitAt(ts, nil))
		if i == 5 {
			require.NoError(t, db.Flatten(2))
		}
	}

	// For each readTs, Get must return the newest version <= readTs, and it must
	// agree with what an AllVersions iterator (the reference) would pick.
	for readTs := uint64(0); readTs <= 105; readTs += 7 {
		txn := db.NewTransactionAt(readTs, false)

		item, err := txn.Get(key)
		if readTs < 10 {
			require.ErrorIsf(t, err, ErrKeyNotFound, "readTs=%d", readTs)
			txn.Discard()
			continue
		}
		require.NoErrorf(t, err, "readTs=%d", readTs)
		want := (readTs / 10) * 10 // newest committed ts <= readTs
		require.Equalf(t, want, item.Version(), "readTs=%d Get version", readTs)
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equalf(t, fmt.Sprintf("val-ts%d", want), string(val), "readTs=%d", readTs)

		txn.Discard()
	}
}

// TestSeekToNextUserKey verifies the seek-target helper used to jump past all
// versions of a user key to the next one.
func TestSeekToNextUserKey(t *testing.T) {
	// Property 1: the target must sort strictly after every version of uk.
	// Property 2: the target must NOT overshoot a prefix-extension key. For "k1"
	// the next stored user key could be "k10"; the target must sort <= every
	// version of "k10" so iteration still visits it.
	cases := []struct {
		uk   []byte
		next []byte // a prefix-extension / immediately-following user key
	}{
		{[]byte("a"), []byte("a\x00")},
		{[]byte("k1"), []byte("k10")},
		{[]byte("k1"), []byte("k1\x00")},
		{[]byte("z"), []byte("z0")},
		{[]byte{0xff, 0xff}, []byte{0xff, 0xff, 0x00}},
	}
	for _, c := range cases {
		target := seekToNextUserKey(c.uk)
		for _, ts := range []uint64{0, 1, 100, math.MaxUint64} {
			require.Truef(t,
				y.CompareKeys(target, y.KeyWithTs(c.uk, ts)) > 0,
				"target for %q must sort after version ts=%d of itself", c.uk, ts)
			require.Truef(t,
				y.CompareKeys(target, y.KeyWithTs(c.next, ts)) <= 0,
				"target for %q must not overshoot next user key %q (ts=%d)",
				c.uk, c.next, ts)
		}
	}
}
