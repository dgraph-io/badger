/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// keyWithTsRef is a reference implementation matching y.KeyWithTs, used to
// validate the arena-backed keyWithTs against the canonical encoding.
func keyWithTsRef(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// ---------------------------------------------------------------------------
// White-box tests for the pendingEntries store. These exercise the collision
// handling, duplicate-version handling and read-your-own-writes lookup logic
// directly, independent of the surrounding DB machinery.
// ---------------------------------------------------------------------------

// TestPendingEntriesNilSafe verifies a nil *pendingEntries behaves as an empty
// store, matching the old nil-map semantics for read-only transactions.
func TestPendingEntriesNilSafe(t *testing.T) {
	var pw *pendingEntries
	require.Equal(t, 0, pw.len())
	_, ok := pw.get([]byte("x"))
	require.False(t, ok)
	count := 0
	pw.rangeEntries(func(*Entry) { count++ })
	require.Equal(t, 0, count)
}

func TestPendingEntriesBasic(t *testing.T) {
	pw := newPendingEntries()
	require.Equal(t, 0, pw.len())

	e1 := &Entry{Key: []byte("a"), Value: []byte("1")}
	old, dup := pw.set(e1)
	require.Nil(t, old)
	require.False(t, dup)
	require.Equal(t, 1, pw.len())

	got, ok := pw.get([]byte("a"))
	require.True(t, ok)
	require.Equal(t, e1, got)

	_, ok = pw.get([]byte("b"))
	require.False(t, ok)
}

func TestPendingEntriesOverwriteSameVersion(t *testing.T) {
	pw := newPendingEntries()
	e1 := &Entry{Key: []byte("a"), Value: []byte("1"), version: 5}
	e2 := &Entry{Key: []byte("a"), Value: []byte("2"), version: 5}

	pw.set(e1)
	old, dup := pw.set(e2)
	// Same version => overwrite, old returned but NOT flagged as duplicate.
	require.Equal(t, e1, old)
	require.False(t, dup)
	require.Equal(t, 1, pw.len())

	got, ok := pw.get([]byte("a"))
	require.True(t, ok)
	require.Equal(t, e2, got)
}

func TestPendingEntriesDuplicateDifferentVersion(t *testing.T) {
	pw := newPendingEntries()
	e1 := &Entry{Key: []byte("a"), Value: []byte("1"), version: 5}
	e2 := &Entry{Key: []byte("a"), Value: []byte("2"), version: 7}

	pw.set(e1)
	old, dup := pw.set(e2)
	// Different version => the OLD entry must be reported as a duplicate.
	require.Equal(t, e1, old)
	require.True(t, dup)
	// The live entry for the key is now e2.
	require.Equal(t, 1, pw.len())
	got, ok := pw.get([]byte("a"))
	require.True(t, ok)
	require.Equal(t, e2, got)
}

// forceCollision constructs a pendingEntries whose hashing always collides so
// the bucket slice path (multiple distinct keys in one bucket) is exercised.
func TestPendingEntriesCollisions(t *testing.T) {
	pw := newPendingEntries()
	pw.hash = func(b []byte) uint64 { return 42 } // everything collides

	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("a")}
	for i, k := range keys {
		e := &Entry{Key: k, Value: []byte(fmt.Sprintf("v%d", i))}
		old, dup := pw.set(e)
		require.Nil(t, old)
		require.False(t, dup)
	}
	require.Equal(t, len(keys), pw.len())

	for i, k := range keys {
		got, ok := pw.get(k)
		require.True(t, ok, "key %s", k)
		require.Equal(t, []byte(fmt.Sprintf("v%d", i)), got.Value)
	}

	// Lookup of a non-present key that hashes into the same bucket must miss.
	_, ok := pw.get([]byte("delta"))
	require.False(t, ok)

	// Overwrite within a colliding bucket.
	e := &Entry{Key: []byte("beta"), Value: []byte("new")}
	old, dup := pw.set(e)
	require.NotNil(t, old)
	require.False(t, dup)
	require.Equal(t, len(keys), pw.len())
	got, ok := pw.get([]byte("beta"))
	require.True(t, ok)
	require.Equal(t, []byte("new"), got.Value)
}

// TestPendingEntriesRangeMatchesMap cross-checks range() against an oracle map.
func TestPendingEntriesRangeMatchesMap(t *testing.T) {
	pw := newPendingEntries()
	oracle := map[string]*Entry{}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("k-%d", r.Intn(500)))
		e := &Entry{Key: k, Value: []byte(fmt.Sprintf("v-%d", i))}
		pw.set(e)
		oracle[string(k)] = e
	}
	require.Equal(t, len(oracle), pw.len())

	collected := map[string]*Entry{}
	pw.rangeEntries(func(e *Entry) {
		collected[string(e.Key)] = e
	})
	require.Equal(t, len(oracle), len(collected))
	for k, e := range oracle {
		require.Equal(t, e, collected[k])
		got, ok := pw.get([]byte(k))
		require.True(t, ok)
		require.Equal(t, e, got)
	}
}

// ---------------------------------------------------------------------------
// End-to-end semantic regression tests through the public API.
// ---------------------------------------------------------------------------

// TestTxnReadYourWrites verifies that Get sees pending writes within the txn,
// including overwrites and deletes, before commit.
func TestTxnReadYourWrites(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		defer txn.Discard()

		require.NoError(t, txn.Set([]byte("foo"), []byte("bar")))
		item, err := txn.Get([]byte("foo"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("bar"), v)

		// Overwrite (last write wins).
		require.NoError(t, txn.Set([]byte("foo"), []byte("baz")))
		item, err = txn.Get([]byte("foo"))
		require.NoError(t, err)
		v, err = item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("baz"), v)

		// Delete then read-your-own-write -> not found.
		require.NoError(t, txn.Delete([]byte("foo")))
		_, err = txn.Get([]byte("foo"))
		require.Equal(t, ErrKeyNotFound, err)
	})
}

// TestTxnManagedDuplicateVersions checks the managed multi-version write path:
// writing the same key at different versions in one batch keeps all versions.
func TestTxnManagedDuplicateVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.managedTxns = true
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	wb := db.NewManagedWriteBatch()
	key := []byte("dup-key")
	// Same key, three different versions in a single batch.
	require.NoError(t, wb.SetEntryAt(NewEntry(key, []byte("v100")), 100))
	require.NoError(t, wb.SetEntryAt(NewEntry(key, []byte("v200")), 200))
	require.NoError(t, wb.SetEntryAt(NewEntry(key, []byte("v300")), 300))
	require.NoError(t, wb.Flush())

	// All three versions must be readable at their respective read timestamps.
	for _, tc := range []struct {
		readTs uint64
		want   string
	}{{150, "v100"}, {250, "v200"}, {350, "v300"}} {
		txn := db.NewTransactionAt(tc.readTs, false)
		item, err := txn.Get(key)
		require.NoError(t, err, "readTs=%d", tc.readTs)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, string(v), "readTs=%d", tc.readTs)
		txn.Discard()
	}
}

// TestTxnConflictDetection ensures conflict detection still works after the
// pendingWrites refactor (default non-managed mode, DetectConflicts=true).
func TestTxnConflictDetectionStillWorks(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("k"), []byte("v0"))
		}))

		txn1 := db.NewTransaction(true)
		txn2 := db.NewTransaction(true)

		// Both read k.
		_, err := txn1.Get([]byte("k"))
		require.NoError(t, err)
		_, err = txn2.Get([]byte("k"))
		require.NoError(t, err)

		require.NoError(t, txn1.Set([]byte("k"), []byte("v1")))
		require.NoError(t, txn2.Set([]byte("k"), []byte("v2")))

		require.NoError(t, txn1.Commit())
		// txn2 read k which txn1 modified -> conflict.
		require.Equal(t, ErrConflict, txn2.Commit())
	})
}

// TestTxnIteratorOverPendingWrites verifies the pending-writes iterator returns
// keys in sorted order and merges with committed data correctly.
func TestTxnIteratorOverPendingWrites(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Commit some baseline keys.
		require.NoError(t, db.Update(func(txn *Txn) error {
			for _, k := range []string{"b", "d", "f"} {
				if err := txn.Set([]byte(k), []byte("committed-"+k)); err != nil {
					return err
				}
			}
			return nil
		}))

		require.NoError(t, db.Update(func(txn *Txn) error {
			// Pending writes interleaved with committed keys.
			for _, k := range []string{"a", "c", "e", "g"} {
				if err := txn.Set([]byte(k), []byte("pending-"+k)); err != nil {
					return err
				}
			}
			// Overwrite a committed key in the txn.
			if err := txn.Set([]byte("d"), []byte("pending-d")); err != nil {
				return err
			}

			var gotKeys []string
			it := txn.NewIterator(DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				gotKeys = append(gotKeys, string(item.Key()))
				v, err := item.ValueCopy(nil)
				require.NoError(t, err)
				if string(item.Key()) == "d" {
					require.Equal(t, "pending-d", string(v))
				}
			}
			require.True(t, sort.StringsAreSorted(gotKeys), "keys not sorted: %v", gotKeys)
			require.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g"}, gotKeys)
			return nil
		}))
	})
}

// TestTxnManyWritesRoundTrip is a fuzz-ish test that commits many keys and
// verifies every one reads back correctly, exercising fingerprint buckets at
// scale.
func TestTxnManyWritesRoundTrip(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		const n = 5000
		want := map[string]string{}
		require.NoError(t, db.Update(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("key-%d", i)
				v := fmt.Sprintf("val-%d", i)
				want[k] = v
				if err := txn.Set([]byte(k), []byte(v)); err != nil {
					return err
				}
			}
			return nil
		}))
		require.NoError(t, db.View(func(txn *Txn) error {
			for k, v := range want {
				item, err := txn.Get([]byte(k))
				require.NoError(t, err)
				got, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, v, string(got))
			}
			return nil
		}))
	})
}

// BenchmarkManagedCommit measures allocations on the managed-mode commit hot
// path that dgraph exercises (managedTxns=true, DetectConflicts=false). It
// reports allocs/op so the per-write string-key allocation and per-key
// KeyWithTs allocation reductions are visible with `-benchmem`.
//
// Run: go test -run=^$ -bench=BenchmarkManagedCommit -benchmem .
func BenchmarkManagedCommit(b *testing.B) {
	for _, n := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "badger-bench")
			require.NoError(b, err)
			defer removeDir(dir)

			opt := getTestOptions(dir).WithInMemory(true).WithDetectConflicts(false)
			opt.Dir = ""
			opt.ValueDir = ""
			opt.managedTxns = true
			db, err := Open(opt)
			require.NoError(b, err)
			defer func() { require.NoError(b, db.Close()) }()

			// Pre-build the key/value byte slices once so the benchmark measures
			// the commit-path allocations, not test fixture allocations.
			keys := make([][]byte, n)
			vals := make([][]byte, n)
			for i := 0; i < n; i++ {
				keys[i] = []byte(fmt.Sprintf("benchmark-key-%08d", i))
				vals[i] = []byte(fmt.Sprintf("benchmark-value-%08d", i))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ts := uint64(i + 1)
				txn := db.NewTransactionAt(ts, true)
				for j := 0; j < n; j++ {
					if err := txn.SetEntry(NewEntry(keys[j], vals[j])); err != nil {
						b.Fatal(err)
					}
				}
				if err := txn.CommitAt(ts, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestKeyWithTsBufMatchesKeyWithTs verifies the arena buffer helper produces
// byte-identical output to y.KeyWithTs.
func TestKeyWithTsBufMatchesKeyWithTs(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		klen := r.Intn(64)
		key := make([]byte, klen)
		r.Read(key)
		ts := r.Uint64()

		a := newKeyArena(len(key) + 8)
		got := a.keyWithTs(key, ts)
		want := keyWithTsRef(key, ts)
		require.True(t, bytes.Equal(got, want), "i=%d key=%x ts=%d got=%x want=%x", i, key, ts, got, want)
		// The arena-backed slice must NOT share storage with the input key.
		if len(key) > 0 {
			require.NotSame(t, &key[0], &got[0])
		}
	}
}
