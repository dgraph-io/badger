/*
 * SPDX-FileCopyrightText: © 2017-2026 Dgraph Labs, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

// refVersions reads all versions of a single key the way dgraph does today:
// NewKeyIterator(AllVersions, PrefetchValues=false) + Seek + walk.
func refVersions(t *testing.T, txn *Txn, key []byte) []ItemVersion {
	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.PrefetchValues = false
	it := txn.NewKeyIterator(key, opt)
	defer it.Close()
	var out []ItemVersion
	for it.Seek(key); it.Valid(); it.Next() {
		item := it.Item()
		if string(item.Key()) != string(key) {
			break
		}
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		out = append(out, ItemVersion{
			Value:     val,
			Version:   item.Version(),
			ExpiresAt: item.expiresAt,
			UserMeta:  item.UserMeta(),
			meta:      item.meta,
		})
	}
	return out
}

func sameVersions(t *testing.T, a, b []ItemVersion) {
	require.Equal(t, len(a), len(b), "version count")
	for i := range a {
		require.Equal(t, a[i].Version, b[i].Version, "version[%d].Version", i)
		require.Equal(t, a[i].UserMeta, b[i].UserMeta, "version[%d].UserMeta", i)
		require.Equal(t, a[i].ExpiresAt, b[i].ExpiresAt, "version[%d].ExpiresAt", i)
		require.Equal(t, a[i].meta, b[i].meta, "version[%d].meta", i)
		require.Equal(t, a[i].IsDeletedOrExpired(), b[i].IsDeletedOrExpired(), "version[%d].del", i)
		require.Equal(t, a[i].DiscardEarlierVersions(), b[i].DiscardEarlierVersions(), "version[%d].dev", i)
		require.Equal(t, string(a[i].Value), string(b[i].Value), "version[%d].Value", i)
	}
}

// TestMultiGetMatchesKeyIterator is a differential test: MultiGet must return,
// for every key, exactly the version chain that a per-key NewKeyIterator yields.
func TestMultiGetMatchesKeyIterator(t *testing.T) {
	run := func(t *testing.T, onDisk bool) {
		dir := t.TempDir()
		db, err := OpenManaged(DefaultOptions(dir).
			WithNumVersionsToKeep(math.MaxInt32).
			WithLoggingLevel(WARNING))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()

		const nKeys = 400
		mk := func(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) }
		rng := rand.New(rand.NewSource(99))
		ts := uint64(1)

		for i := 0; i < nKeys; i++ {
			nv := rng.Intn(4) // 0..3 versions (0 => absent key)
			for v := 0; v < nv; v++ {
				txn := db.NewTransactionAt(math.MaxUint64, true)
				e := &Entry{Key: mk(i)}
				switch rng.Intn(5) {
				case 0:
					e.Value = []byte(fmt.Sprintf("v%d-small", v))
				case 1:
					big := make([]byte, 4096)
					for b := range big {
						big[b] = byte(v + b)
					}
					e.Value = big
				case 2:
					e = e.WithMeta(byte(rng.Intn(8)))
					e.Value = []byte("withmeta")
				case 3:
					e = e.WithDiscard()
					e.Value = []byte("discard")
				case 4:
					txn2 := db.NewTransactionAt(math.MaxUint64, true)
					require.NoError(t, txn2.Delete(mk(i)))
					require.NoError(t, txn2.CommitAt(ts, nil))
					ts++
					txn.Discard()
					continue
				}
				require.NoError(t, txn.SetEntry(e))
				require.NoError(t, txn.CommitAt(ts, nil))
				ts++
			}
		}
		if onDisk {
			require.NoError(t, db.Flatten(2))
		}

		readTs := ts
		txn := db.NewTransactionAt(readTs, false)
		defer txn.Discard()

		var keys [][]byte
		for i := 0; i < nKeys; i++ {
			keys = append(keys, mk(i))
		}
		keys = append(keys, []byte("absent-1"), []byte("zzz-absent"), []byte("key-0000-nope"))
		rng.Shuffle(len(keys), func(a, b int) { keys[a], keys[b] = keys[b], keys[a] })

		got, err := txn.MultiGet(keys)
		require.NoError(t, err)
		require.Equal(t, len(keys), len(got))
		for i := range keys {
			want := refVersions(t, txn, keys[i])
			sameVersions(t, want, got[i].Versions)
		}
	}
	t.Run("memtable", func(t *testing.T) { run(t, false) })
	t.Run("ondisk", func(t *testing.T) { run(t, true) })
}

// TestMultiGetReadTs verifies versions newer than the read ts are excluded.
func TestMultiGetReadTs(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).WithNumVersionsToKeep(math.MaxInt32).WithLoggingLevel(WARNING))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	key := []byte("k")
	for v := uint64(1); v <= 5; v++ {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.SetEntry(&Entry{Key: key, Value: []byte(fmt.Sprintf("v%d", v))}))
		require.NoError(t, txn.CommitAt(v, nil))
	}
	for readTs := uint64(1); readTs <= 5; readTs++ {
		txn := db.NewTransactionAt(readTs, false)
		got, err := txn.MultiGet([][]byte{key})
		require.NoError(t, err)
		require.Len(t, got[0].Versions, int(readTs), "readTs=%d", readTs)
		for _, v := range got[0].Versions {
			require.LessOrEqual(t, v.Version, readTs)
		}
		require.True(t, sort.SliceIsSorted(got[0].Versions, func(a, b int) bool {
			return got[0].Versions[a].Version > got[0].Versions[b].Version
		}), "newest-first")
		txn.Discard()
	}
}

// TestMultiGetEmpty checks the empty and nil inputs.
func TestMultiGetEmpty(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).WithLoggingLevel(WARNING))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	txn := db.NewTransactionAt(1, false)
	defer txn.Discard()
	got, err := txn.MultiGet(nil)
	require.NoError(t, err)
	require.Len(t, got, 0)
	got, err = txn.MultiGet([][]byte{})
	require.NoError(t, err)
	require.Len(t, got, 0)
}

// TestMultiGetRejectsBadKeys exercises Txn.Get parity: an empty key in
// the request returns ErrEmptyKey and a key in a banned namespace
// returns ErrBannedKey. Otherwise the caller could not distinguish
// banned from absent — exactly the bug dgraph would hit if its posting
// reads went through MultiGet.
func TestMultiGetRejectsBadKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).
		WithNamespaceOffset(1).
		WithLoggingLevel(WARNING))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.BanNamespace(42))
	txn := db.NewTransactionAt(1, false)
	defer txn.Discard()

	_, err = txn.MultiGet([][]byte{nil})
	require.ErrorIs(t, err, ErrEmptyKey)

	// 8-byte namespace at offset 1; big-endian 42 == bytes 0,0,0,0,0,0,0,42.
	// isBanned requires len(key) > NamespaceOffset+8, so add a trailing byte.
	banned := []byte{0xff, 0, 0, 0, 0, 0, 0, 0, 42, 0x01}
	_, err = txn.MultiGet([][]byte{banned})
	require.ErrorIs(t, err, ErrBannedKey)

	// Mixed batch with one bad key still fails the whole call, matching
	// Txn.Get's first-error-wins behavior.
	good := []byte("ok")
	_, err = txn.MultiGet([][]byte{good, nil})
	require.ErrorIs(t, err, ErrEmptyKey)
}

// TestMultiGetMaxVersionsPerKey verifies the MaxVersionsPerKey cap is
// applied per key: the walk stops once the requested number of versions
// is materialized, so callers that fold deltas on top of a complete
// record don't pay for the rest of the chain. Versions are returned
// newest-first, so the cap trims the oldest tail.
func TestMultiGetMaxVersionsPerKey(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).
		WithNumVersionsToKeep(math.MaxInt32).
		WithLoggingLevel(WARNING))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	key := []byte("k")
	const total = 10
	for v := uint64(1); v <= total; v++ {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.SetEntry(&Entry{Key: key, Value: []byte(fmt.Sprintf("v%d", v))}))
		require.NoError(t, txn.CommitAt(v, nil))
	}

	readTxn := db.NewTransactionAt(total+1, false)
	defer readTxn.Discard()

	// Without the cap we get every version.
	full, err := readTxn.MultiGet([][]byte{key})
	require.NoError(t, err)
	require.Len(t, full[0].Versions, total)

	// Cap returns the newest N only.
	cap3 := [][]byte{key}
	withCap, err := readTxn.MultiGetWithOptions(cap3, DefaultMultiGetOptions().WithMaxVersionsPerKey(3))
	require.NoError(t, err)
	require.Len(t, withCap[0].Versions, 3)
	require.Equal(t, uint64(total), withCap[0].Versions[0].Version)
	require.Equal(t, uint64(total-1), withCap[0].Versions[1].Version)
	require.Equal(t, uint64(total-2), withCap[0].Versions[2].Version)
}

// TestMultiGetReadSet is the regression test for the read-write conflict
// tracking contract. Without noReadTracking on the iterator, MultiGet would
// record every walked key between successive Seeks as a read, causing
// spurious conflicts in dgraph's HNSW-style fan-out.
//
// What we assert:
//   - Only the requested keys end up in the txn's read set (len(txn.reads)
//     equals the unique-key count, not the walked-key count).
//   - The read set contains exactly the requested keys, with duplicates
//     in the request collapsed to a single fingerprint.
//
// We populate the DB with 50 dense keys and ask MultiGet for a sparse
// frontier [k0, k49, k0] (mk(0) repeated to exercise collapsing). The
// naive implementation would walk k0, k1, ..., k49 (50 keys added to
// reads); the correct implementation adds only {k0, k49} — the
// fingerprint of mk(0) appears once even though it was requested twice.
func TestMultiGetReadSet(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).
		WithNumVersionsToKeep(math.MaxInt32).
		WithDetectConflicts(true).
		WithLoggingLevel(WARNING))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	mk := func(i int) []byte {
		return []byte(fmt.Sprintf("key-%04d", i))
	}
	const nKeys = 50
	// Write each key at a unique ts so the version we expect to read back
	// for mk(i) is exactly (i+1).
	versions := make(map[string]uint64, nKeys)
	for i := 0; i < nKeys; i++ {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.SetEntry(&Entry{Key: mk(i), Value: []byte("v")}))
		require.NoError(t, txn.CommitAt(uint64(i+1), nil))
		versions[string(mk(i))] = uint64(i + 1)
	}
	readTs := uint64(nKeys + 1)

	// Read-write txn: ask for a sparse frontier across the full key range,
	// with mk(0) repeated so the result exercises duplicate collapsing.
	requested := [][]byte{mk(0), mk(49), mk(0)}
	txn := db.NewTransactionAt(readTs, true) // update = true
	defer txn.Discard()

	got, err := txn.MultiGet(requested)
	require.NoError(t, err)
	require.Len(t, got, len(requested))
	require.Len(t, got[0].Versions, 1)
	require.Len(t, got[1].Versions, 1)
	require.Len(t, got[2].Versions, 1)
	require.Equal(t, versions[string(mk(0))], got[0].Versions[0].Version)
	require.Equal(t, versions[string(mk(49))], got[1].Versions[0].Version)
	require.Equal(t, versions[string(mk(0))], got[2].Versions[0].Version)

	// Assert: txn.reads contains exactly the fingerprints of {mk(0), mk(49)}
	// — the two requested keys — and NOT the 48 intermediate keys the
	// iterator walked between Seek(mk(0)) and Seek(mk(49)). txn.reads holds
	// z.MemHash fingerprints (matching addReadKey), so we hash the expected
	// keys the same way and check the resulting set.
	readSet := make(map[uint64]bool, len(txn.reads))
	for _, fp := range txn.reads {
		readSet[fp] = true
	}
	require.Equal(t, 2, len(readSet),
		"read set should contain exactly the requested keys; got %d entries", len(readSet))
	require.True(t, readSet[z.MemHash(mk(0))], "mk(0) should be in read set")
	require.True(t, readSet[z.MemHash(mk(49))], "mk(49) should be in read set")
}

// loadFrontierDB builds a managed DB shaped like a dgraph predicate: dense
// keys, a few MVCC versions each, flushed to disk.
func loadFrontierDB(b *testing.B, nKeys, versions int) (*DB, [][]byte) {
	b.Helper()
	dir := b.TempDir()
	db, err := OpenManaged(DefaultOptions(dir).
		WithSyncWrites(false).WithLoggingLevel(WARNING).
		WithNumVersionsToKeep(math.MaxInt32).WithDetectConflicts(false))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	val := make([]byte, 64)
	mk := func(uid uint64) []byte {
		k := make([]byte, 16)
		for i := 0; i < 8; i++ {
			k[i] = byte(uid >> (8 * (7 - i)))
		}
		return k
	}
	ts := uint64(1)
	for v := 0; v < versions; v++ {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 0; i < nKeys; i++ {
			if err := txn.SetEntry(&Entry{Key: mk(uint64(i + 1)), Value: val}); err != nil {
				b.Fatal(err)
			}
		}
		if err := txn.CommitAt(ts, nil); err != nil {
			b.Fatal(err)
		}
		ts++
	}
	if err := db.Flatten(2); err != nil {
		b.Fatal(err)
	}
	keys := make([][]byte, nKeys)
	for i := range keys {
		keys[i] = mk(uint64(i + 1))
	}
	return db, keys
}

// loadDeepChainDB builds a DB with deep MVCC version chains and
// value-log-sized values, so benchmarks exercise the case that matters
// for dgraph: a candidate's sibling reads may sit on a delta chain atop
// a complete posting, and MultiGet must materialize every visible
// version (unless the caller caps MaxVersionsPerKey).
func loadDeepChainDB(b *testing.B, nKeys, versions int, valueSize int) (*DB, [][]byte) {
	b.Helper()
	dir := b.TempDir()
	opt := DefaultOptions(dir).
		WithSyncWrites(false).
		WithLoggingLevel(WARNING).
		WithNumVersionsToKeep(math.MaxInt32).
		WithDetectConflicts(false).
		WithValueThreshold(64) // force most values into the vlog.
	// Shrink the vlog so the file actually rotates during setup.
	opt.ValueLogFileSize = 1 << 20
	db, err := OpenManaged(opt)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	val := make([]byte, valueSize)
	mk := func(uid uint64) []byte {
		k := make([]byte, 16)
		for i := 0; i < 8; i++ {
			k[i] = byte(uid >> (8 * (7 - i)))
		}
		return k
	}
	ts := uint64(1)
	for v := 0; v < versions; v++ {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 0; i < nKeys; i++ {
			if err := txn.SetEntry(&Entry{Key: mk(uint64(i + 1)), Value: val}); err != nil {
				b.Fatal(err)
			}
		}
		if err := txn.CommitAt(ts, nil); err != nil {
			b.Fatal(err)
		}
		ts++
	}
	if err := db.Flatten(2); err != nil {
		b.Fatal(err)
	}
	keys := make([][]byte, nKeys)
	for i := range keys {
		keys[i] = mk(uint64(i + 1))
	}
	return db, keys
}

// BenchmarkMultiGetVsKeyIterator compares reading a frontier of K keys via K
// independent NewKeyIterator reads (today's dgraph HNSW path) vs one MultiGet.
func BenchmarkMultiGetVsKeyIterator(b *testing.B) {
	const nKeys, versions = 100000, 3
	db, keys := loadFrontierDB(b, nKeys, versions)
	rng := rand.New(rand.NewSource(7))

	frontier := func(k int) [][]byte {
		out := make([][]byte, k)
		base := rng.Intn(nKeys - k)
		for i := 0; i < k; i++ {
			out[i] = keys[base+i]
		}
		return out
	}

	for _, K := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("KeyIterator/K=%d", K), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fr := frontier(K)
				txn := db.NewTransactionAt(math.MaxUint64, false)
				for _, key := range fr {
					opt := DefaultIteratorOptions
					opt.AllVersions = true
					opt.PrefetchValues = false
					it := txn.NewKeyIterator(key, opt)
					for it.Seek(key); it.Valid(); it.Next() {
						item := it.Item()
						if string(item.Key()) != string(key) {
							break
						}
						_, _ = item.ValueCopy(nil)
					}
					it.Close()
				}
				txn.Discard()
			}
		})
		b.Run(fmt.Sprintf("MultiGet/K=%d", K), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fr := frontier(K)
				txn := db.NewTransactionAt(math.MaxUint64, false)
				if _, err := txn.MultiGet(fr); err != nil {
					b.Fatal(err)
				}
				txn.Discard()
			}
		})
	}
}

// BenchmarkMultiGetDeepChainVsKeyIterator exercises the workload that
// MultiGet's per-key version walk was questioned on in review: many
// MVCC versions per key, with value-log-sized values that force a vlog
// resolve on every version. Without MaxVersionsPerKey the per-version
// cost dominates; with the cap, MultiGet is bounded.
func BenchmarkMultiGetDeepChainVsKeyIterator(b *testing.B) {
	const (
		nKeys            = 1000
		versionsPerKey   = 200
		valueSize        = 4 << 10 // 4 KiB, well above ValueThreshold so it lands in vlog.
	)
	db, keys := loadDeepChainDB(b, nKeys, versionsPerKey, valueSize)
	rng := rand.New(rand.NewSource(11))
	const K = 64
	frontier := func() [][]byte {
		out := make([][]byte, K)
		base := rng.Intn(nKeys - K)
		for i := 0; i < K; i++ {
			out[i] = keys[base+i]
		}
		return out
	}
	for _, name := range []string{"KeyIterator", "MultiGet", "MultiGetCap1"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fr := frontier()
				txn := db.NewTransactionAt(math.MaxUint64, false)
				switch name {
				case "KeyIterator":
					for _, key := range fr {
						opt := DefaultIteratorOptions
						opt.AllVersions = true
						opt.PrefetchValues = false
						it := txn.NewKeyIterator(key, opt)
						for it.Seek(key); it.Valid(); it.Next() {
							item := it.Item()
							if string(item.Key()) != string(key) {
								break
							}
							_, _ = item.ValueCopy(nil)
						}
						it.Close()
					}
				case "MultiGet":
					if _, err := txn.MultiGet(fr); err != nil {
						b.Fatal(err)
					}
				case "MultiGetCap1":
					if _, err := txn.MultiGetWithOptions(fr,
						DefaultMultiGetOptions().WithMaxVersionsPerKey(1)); err != nil {
						b.Fatal(err)
					}
				}
				txn.Discard()
			}
		})
	}
}
