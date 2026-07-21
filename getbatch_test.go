/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The tests in this file pin the behavior of Txn.GetBatch as a batched equivalent of
// Txn.Get: for every key, GetBatch(keys)[i] must observe exactly what Get(keys[i]) would
// observe at the same read timestamp — committed versions, the transaction's own pending
// writes and deletes, and a nil Item for keys that are absent (or deleted) at that ts.

// runManagedBadgerTest opens a managed-mode DB in a temp dir and runs the test closure.
// Mirrors runBadgerTest (db_test.go) but with managedTxns, following TestTxnSimpleTsRead.
func runManagedBadgerTest(t *testing.T, mutateOpts func(*Options), test func(t *testing.T, db *DB)) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opts := getTestOptions(dir)
	opts.managedTxns = true
	if mutateOpts != nil {
		mutateOpts(&opts)
	}
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	test(t, db)
}

// commitAt writes key=val in a managed txn started at startTs and commits it at commitTs.
func commitAt(t *testing.T, db *DB, key, val []byte, startTs, commitTs uint64) {
	t.Helper()
	txn := db.NewTransactionAt(startTs, true)
	defer txn.Discard()
	require.NoError(t, txn.SetEntry(NewEntry(key, val)))
	require.NoError(t, txn.CommitAt(commitTs, nil))
}

// batchVal extracts the value of a non-nil batch item.
func batchVal(t *testing.T, item *Item) []byte {
	t.Helper()
	require.NotNil(t, item)
	val, err := item.ValueCopy(nil)
	require.NoError(t, err)
	return val
}

// TestGetBatchSingleInsertedKey: insert exactly one record; a batch read of exactly that
// key must return exactly one item holding exactly that value.
func TestGetBatchSingleInsertedKey(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("name"), []byte("Alice"), 1, 2)

		txn := db.NewTransactionAt(3, false)
		defer txn.Discard()
		items, err := txn.GetBatch([][]byte{[]byte("name")})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, []byte("Alice"), batchVal(t, items[0]))
	})
}

// TestGetBatchAbsentKeyIsNil: asking for a key that was never inserted must yield a nil
// item — not a non-nil Item with an empty value — so callers can distinguish "absent"
// from "present with empty value", exactly as Get distinguishes via ErrKeyNotFound.
func TestGetBatchAbsentKeyIsNil(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("k1"), []byte("v1"), 1, 2)
		commitAt(t, db, []byte("k2"), []byte("v2"), 3, 4)

		txn := db.NewTransactionAt(5, false)
		defer txn.Discard()
		items, err := txn.GetBatch([][]byte{[]byte("k1"), []byte("k2"), []byte("never-inserted")})
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, []byte("v1"), batchVal(t, items[0]))
		require.Equal(t, []byte("v2"), batchVal(t, items[1]))
		require.Nil(t, items[2], "absent key must yield a nil item")
	})
}

// TestGetBatchSeesOwnPendingWrite: an update transaction that batch-reads a key it has a
// pending (uncommitted) write for must see its own write — same read-your-own-writes
// guarantee Txn.Get provides via pendingWrites.
func TestGetBatchSeesOwnPendingWrite(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("k"), []byte("old"), 1, 2)

		txn := db.NewTransactionAt(5, true)
		defer txn.Discard()
		require.NoError(t, txn.SetEntry(NewEntry([]byte("k"), []byte("new"))))

		items, err := txn.GetBatch([][]byte{[]byte("k")})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, []byte("new"), batchVal(t, items[0]))
	})
}

// TestGetBatchSeesOwnPendingDelete: an update transaction that deleted a key must NOT get
// the older committed value back from a batch read of that key.
func TestGetBatchSeesOwnPendingDelete(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("k"), []byte("old"), 1, 2)

		txn := db.NewTransactionAt(5, true)
		defer txn.Discard()
		require.NoError(t, txn.Delete([]byte("k")))

		items, err := txn.GetBatch([][]byte{[]byte("k")})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Nil(t, items[0], "own pending delete must hide the committed value")
	})
}

// TestGetBatchMixedPendingCommittedAbsent: one batch containing a pending write, an
// untouched committed key, and an absent key — each position must be answered
// independently and correctly.
func TestGetBatchMixedPendingCommittedAbsent(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("pending"), []byte("old"), 1, 2)
		commitAt(t, db, []byte("committed"), []byte("stable"), 1, 2)

		txn := db.NewTransactionAt(5, true)
		defer txn.Discard()
		require.NoError(t, txn.SetEntry(NewEntry([]byte("pending"), []byte("new"))))

		items, err := txn.GetBatch([][]byte{[]byte("pending"), []byte("committed"), []byte("absent")})
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, []byte("new"), batchVal(t, items[0]), "pending write wins")
		require.Equal(t, []byte("stable"), batchVal(t, items[1]), "committed value untouched")
		require.Nil(t, items[2], "absent key is nil")
	})
}

// TestGetBatchVersionedReads: with two committed versions, a batch read must return the
// version visible at the transaction's read timestamp — v1 below the second commit, v2 at
// or above it.
func TestGetBatchVersionedReads(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("k"), []byte("v1"), 1, 2)
		commitAt(t, db, []byte("k"), []byte("v2"), 4, 5)

		read := func(ts uint64) []byte {
			txn := db.NewTransactionAt(ts, false)
			defer txn.Discard()
			items, err := txn.GetBatch([][]byte{[]byte("k")})
			require.NoError(t, err)
			require.Len(t, items, 1)
			return batchVal(t, items[0])
		}
		require.Equal(t, []byte("v1"), read(3), "ts 3 sees the first commit")
		require.Equal(t, []byte("v2"), read(6), "ts 6 sees the second commit")
	})
}

// TestGetBatchValueLogValues: values above ValueThreshold live in the value log (vptr
// path); a batch read must round-trip them exactly.
func TestGetBatchValueLogValues(t *testing.T) {
	runManagedBadgerTest(t, func(o *Options) { o.ValueThreshold = 32 }, func(t *testing.T, db *DB) {
		big1 := make([]byte, 512)
		big2 := make([]byte, 1024)
		for i := range big1 {
			big1[i] = byte(i % 251)
		}
		for i := range big2 {
			big2[i] = byte((i * 7) % 251)
		}
		commitAt(t, db, []byte("big1"), big1, 1, 2)
		commitAt(t, db, []byte("big2"), big2, 3, 4)

		txn := db.NewTransactionAt(5, false)
		defer txn.Discard()
		items, err := txn.GetBatch([][]byte{[]byte("big1"), []byte("big2")})
		require.NoError(t, err)
		require.Equal(t, big1, batchVal(t, items[0]))
		require.Equal(t, big2, batchVal(t, items[1]))
	})
}

// TestGetBatchAcrossMemtableAndLSMUnsorted: with keys spread across flushed tables and
// the live memtable, a batch asked in non-sorted order must still return each key's own
// value. Pins the per-table iterator-reuse heuristic in levelHandler.getBatch.
func TestGetBatchAcrossMemtableAndLSMUnsorted(t *testing.T) {
	small := func(o *Options) {
		o.MemTableSize = 1 << 15   // 32KB → early flushes
		o.ValueThreshold = 1 << 10 // must be ≤ maxBatchSize (15% of MemTableSize)
	}
	runManagedBadgerTest(t, small, func(t *testing.T, db *DB) {
		// Phase 1: enough ~900B inline values to force memtable flushes into the LSM.
		filler := make([]byte, 900)
		ts := uint64(1)
		for i := 0; i < 100; i++ {
			key := fmt.Appendf(nil, "flushed-%03d", i)
			commitAt(t, db, key, append(filler, byte(i)), ts, ts+1)
			ts += 2
		}
		// Phase 2: a few small keys that stay in the live memtable.
		commitAt(t, db, []byte("mem-a"), []byte("va"), ts, ts+1)
		commitAt(t, db, []byte("mem-b"), []byte("vb"), ts+2, ts+3)

		txn := db.NewTransactionAt(ts+10, false)
		defer txn.Discard()
		// Deliberately unsorted, mixing LSM-resident and memtable-resident keys.
		keys := [][]byte{
			[]byte("mem-b"),
			[]byte("flushed-099"),
			[]byte("flushed-000"),
			[]byte("mem-a"),
			[]byte("flushed-050"),
		}
		items, err := txn.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, items, 5)
		require.Equal(t, []byte("vb"), batchVal(t, items[0]))
		require.Equal(t, byte(99), batchVal(t, items[1])[900])
		require.Equal(t, byte(0), batchVal(t, items[2])[900])
		require.Equal(t, []byte("va"), batchVal(t, items[3]))
		require.Equal(t, byte(50), batchVal(t, items[4])[900])
	})
}

// TestGetBatchEdgeCases: an empty key slice returns an empty result; an empty key is
// rejected with ErrEmptyKey, matching Get's contract.
func TestGetBatchEdgeCases(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransactionAt(5, false)
		defer txn.Discard()

		items, err := txn.GetBatch(nil)
		require.NoError(t, err)
		require.Empty(t, items)

		_, err = txn.GetBatch([][]byte{{}})
		require.ErrorIs(t, err, ErrEmptyKey)
	})
}

// TestGetBatchL0VersionsAcrossTables pins an MVCC hazard of iterator reuse at level 0:
// L0 tables overlap, so a later key in the batch can have its NEWEST version in an L0
// table that was bloom-skipped for the FIRST key. If the reused iterator set (built for
// the first key) finds an older version of the later key, it must not be accepted as the
// result - the read has to consult every L0 table that may hold that key.
//
// Construction: T1 (first flush) holds a-key and b-key@old; T2 (second flush) holds
// b-key@new but NOT a-key. A batch of [a-key, b-key] must return b-key@new.
func TestGetBatchL0VersionsAcrossTables(t *testing.T) {
	small := func(o *Options) {
		o.MemTableSize = 1 << 15
		o.ValueThreshold = 1 << 10
		o.NumLevelZeroTables = 10 // keep both tables in L0, no compaction
	}
	runManagedBadgerTest(t, small, func(t *testing.T, db *DB) {
		filler := make([]byte, 900)
		ts := uint64(1)
		// rollAndWait writes enough filler (keyed AFTER "b-key") to roll the memtable,
		// then waits until an SST whose smallest key has the given prefix lands on disk.
		rollAndWait := func(fillPrefix, wantLeftPrefix string) {
			for i := 0; i < 45; i++ {
				commitAt(t, db, fmt.Appendf(nil, "%s-%03d", fillPrefix, i), filler, ts, ts+1)
				ts += 2
			}
			ok := false
			for range 300 { // up to 3s for the async flush
				for _, ti := range db.Tables() {
					if bytes.HasPrefix(ti.Left, []byte(wantLeftPrefix)) {
						ok = true
					}
				}
				if ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.True(t, ok, "no SST starting with %q landed", wantLeftPrefix)
		}

		// T1: a-key and b-key@old (+ filler), flushed to disk.
		commitAt(t, db, []byte("a-key"), []byte("A1"), ts, ts+1)
		ts += 2
		commitAt(t, db, []byte("b-key"), []byte("B-OLD"), ts, ts+1)
		ts += 2
		rollAndWait("c-fill-1", "a-key")

		// T2: b-key@new (+ filler only - a-key is absent from T2), flushed to disk.
		commitAt(t, db, []byte("b-key"), []byte("B-NEW"), ts, ts+1)
		newVersion := ts + 1
		ts += 2
		rollAndWait("c-fill-2", "b-key")

		txn := db.NewTransactionAt(ts+100, false)
		defer txn.Discard()
		items, err := txn.GetBatch([][]byte{[]byte("a-key"), []byte("b-key")})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, []byte("A1"), batchVal(t, items[0]))
		require.Equal(t, []byte("B-NEW"), batchVal(t, items[1]),
			"batch must return b-key's newest L0 version, not a stale one from a reused table set")
		require.Equal(t, newVersion, items[1].Version())
	})
}

// TestGetBatchConflictDetection pins SSI conflict tracking for batched reads in normal
// (non-managed) mode: keys read via GetBatch must be registered as reads of the
// transaction, so a concurrent commit to any of them conflicts the reader — exactly as
// if each key had been read with Get. It also pins the parity carve-out: a key served
// from the transaction's own pendingWrites is NOT a tracked read (Get behaves the same),
// so a concurrent commit to such a key does not conflict a blind writer.
func TestGetBatchConflictDetection(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		seed := db.NewTransaction(true)
		for i := 1; i <= 5; i++ {
			require.NoError(t, seed.Set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))))
		}
		require.NoError(t, seed.Commit())

		keys := func() [][]byte {
			var ks [][]byte
			for i := 1; i <= 5; i++ {
				ks = append(ks, []byte(fmt.Sprintf("k%d", i)))
			}
			return ks
		}()

		// A batch-read key committed by another txn must conflict the reader.
		txnA := db.NewTransaction(true)
		defer txnA.Discard()
		items, err := txnA.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, items, 5)
		require.NoError(t, txnA.Set([]byte("out-a"), []byte("a")))

		txnB := db.NewTransaction(true)
		require.NoError(t, txnB.Set([]byte("k3"), []byte("v3-newer")))
		require.NoError(t, txnB.Commit())

		require.ErrorIs(t, txnA.Commit(), ErrConflict,
			"a concurrent commit to a GetBatch-read key must conflict the reading txn")

		// Control: a concurrent commit to an unrelated key must not conflict.
		txnC := db.NewTransaction(true)
		defer txnC.Discard()
		_, err = txnC.GetBatch(keys)
		require.NoError(t, err)
		require.NoError(t, txnC.Set([]byte("out-c"), []byte("c")))

		txnD := db.NewTransaction(true)
		require.NoError(t, txnD.Set([]byte("unrelated"), []byte("u")))
		require.NoError(t, txnD.Commit())

		require.NoError(t, txnC.Commit(),
			"a commit to a key outside the batch must not conflict the reader")

		// Parity carve-out: a key answered from the reader's own pendingWrites is not a
		// tracked read (same as Get), so a concurrent blind overwrite does not conflict.
		txnE := db.NewTransaction(true)
		defer txnE.Discard()
		require.NoError(t, txnE.Set([]byte("k5"), []byte("mine")))
		items, err = txnE.GetBatch([][]byte{[]byte("k5")})
		require.NoError(t, err)
		require.Equal(t, []byte("mine"), batchVal(t, items[0]))

		txnF := db.NewTransaction(true)
		require.NoError(t, txnF.Set([]byte("k5"), []byte("theirs")))
		require.NoError(t, txnF.Commit())

		require.NoError(t, txnE.Commit(),
			"a key served from own pendingWrites is not a tracked read; no conflict, matching Get")
	})
}

// TestGetBatchTTLExpiryMidBatch pins TTL semantics: an entry whose TTL has lapsed by
// read time must yield a nil item at its position — the batched analogue of Get's
// ErrKeyNotFound for expired keys — while unexpired neighbors in the same batch are
// returned normally.
func TestGetBatchTTLExpiryMidBatch(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("t1"), []byte("stays1")))
		require.NoError(t, txn.SetEntry(NewEntry([]byte("t2"), []byte("fleeting")).WithTTL(20*time.Millisecond)))
		require.NoError(t, txn.Set([]byte("t3"), []byte("stays3")))
		require.NoError(t, txn.Commit())

		time.Sleep(100 * time.Millisecond)

		read := db.NewTransaction(false)
		defer read.Discard()
		items, err := read.GetBatch([][]byte{[]byte("t1"), []byte("t2"), []byte("t3")})
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, []byte("stays1"), batchVal(t, items[0]))
		require.Nil(t, items[1], "an expired key must read as a nil item mid-batch")
		require.Equal(t, []byte("stays3"), batchVal(t, items[2]))

		// Parity: Get reports the same expiry as ErrKeyNotFound.
		_, err = read.Get([]byte("t2"))
		require.ErrorIs(t, err, ErrKeyNotFound)
	})
}

// TestGetBatchReadAtExactCommitTs pins the MVCC boundary: a snapshot read at ts T must
// see versions committed AT T, not just strictly before it — Txn.Get does, and callers
// (dgraph reads at a readTs that routinely equals the latest commitTs) depend on it.
// Found via the non-managed tests: GetBatch aliased its "answered from pendingWrites"
// slice with db.getBatch's "found exact version" slice, so a memtable hit at exactly the
// requested version was skipped when building items and read back as absent.
func TestGetBatchReadAtExactCommitTs(t *testing.T) {
	runManagedBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitAt(t, db, []byte("exact"), []byte("at-boundary"), 1, 4)
		commitAt(t, db, []byte("older"), []byte("before-boundary"), 2, 3)

		txn := db.NewTransactionAt(4, false)
		defer txn.Discard()
		items, err := txn.GetBatch([][]byte{[]byte("exact"), []byte("older"), []byte("absent")})
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, []byte("at-boundary"), batchVal(t, items[0]),
			"a version committed exactly at readTs must be visible, as it is to Get")
		require.Equal(t, uint64(4), items[0].Version())
		require.Equal(t, []byte("before-boundary"), batchVal(t, items[1]))
		require.Nil(t, items[2])

		// Parity: Get sees the boundary version too.
		item, err := txn.Get([]byte("exact"))
		require.NoError(t, err)
		require.Equal(t, []byte("at-boundary"), batchVal(t, item))
	})
}

// TestGetBatchBannedNamespace pins banned-key semantics: reading a banned key with Get
// fails with ErrBannedKey, and a batch containing one banned key mid-batch fails the
// whole call with the same error — the batch cannot partially succeed, and the error is
// the same one Get reports. Batches that avoid the banned namespace keep working.
func TestGetBatchBannedNamespace(t *testing.T) {
	nsKey := func(ns uint64, suffix string) []byte {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, ns)
		return append(key, suffix...)
	}
	runManagedBadgerTest(t, func(opts *Options) {
		*opts = opts.WithNamespaceOffset(0)
	}, func(t *testing.T, db *DB) {
		commitAt(t, db, nsKey(1, "a"), []byte("ns1-a"), 1, 2)
		commitAt(t, db, nsKey(1, "b"), []byte("ns1-b"), 3, 4)
		commitAt(t, db, nsKey(2, "a"), []byte("ns2-a"), 5, 6)

		// Before the ban, a cross-namespace batch works.
		txn := db.NewTransactionAt(10, false)
		items, err := txn.GetBatch([][]byte{nsKey(1, "a"), nsKey(2, "a"), nsKey(1, "b")})
		require.NoError(t, err)
		require.Equal(t, []byte("ns2-a"), batchVal(t, items[1]))
		txn.Discard()

		require.NoError(t, db.BanNamespace(2))

		// Get parity: the banned key errors with ErrBannedKey.
		txn = db.NewTransactionAt(10, false)
		defer txn.Discard()
		_, err = txn.Get(nsKey(2, "a"))
		require.ErrorIs(t, err, ErrBannedKey)

		// A batch containing the banned key mid-batch fails whole with the same error:
		// no partial results that could be mistaken for "banned key is absent".
		items, err = txn.GetBatch([][]byte{nsKey(1, "a"), nsKey(2, "a"), nsKey(1, "b")})
		require.ErrorIs(t, err, ErrBannedKey)
		require.Nil(t, items)

		// A batch that stays outside the banned namespace is unaffected.
		items, err = txn.GetBatch([][]byte{nsKey(1, "a"), nsKey(1, "b")})
		require.NoError(t, err)
		require.Equal(t, []byte("ns1-a"), batchVal(t, items[0]))
		require.Equal(t, []byte("ns1-b"), batchVal(t, items[1]))
	})
}

// TestGetBatchUnderCompaction stresses the batched read path while the LSM shape is
// changing underneath it — memtable flushes, L0 accumulation and forced compactions —
// the environment where the reusable-iterator stale-read bug (B8) lived. Phase A runs
// writers and readers concurrently (run with -race to shake out data races): readers
// only assert invariants that need no cross-goroutine coordination — every returned
// version is within the snapshot, every value matches the version that wrote it, and
// two batch reads at the same readTs return byte-identical versions. Phase B quiesces,
// force-compacts with Flatten, and then checks exact expected versions per key against
// the recorded write history, at the final ts and at historical snapshots — versions
// must survive compaction and stay visible at their exact boundaries.
func TestGetBatchUnderCompaction(t *testing.T) {
	const nKeys = 100
	const commitsPerWriter = 250
	const nWriters = 4

	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%03d", i)) }
	// The value encodes the commitTs that wrote it, padded to force frequent flushes.
	val := func(ts uint64) []byte {
		return []byte(fmt.Sprintf("val-%09d-%s", ts, strings.Repeat("x", 150)))
	}

	runManagedBadgerTest(t, func(opts *Options) {
		opts.MemTableSize = 32 << 10  // ~32KiB: many flushes -> many L0 tables -> compactions
		opts.ValueThreshold = 1 << 10 // must stay under the max batch size implied by MemTableSize
	}, func(t *testing.T, db *DB) {
		var tsCounter atomic.Uint64
		var mu sync.Mutex
		history := make([][]uint64, nKeys) // per-key commit timestamps, ascending

		commitKey := func(i int) uint64 {
			start := tsCounter.Add(2)
			commit := start + 1
			txn := db.NewTransactionAt(start, true)
			require.NoError(t, txn.SetEntry(NewEntry(key(i), val(commit))))
			require.NoError(t, txn.CommitAt(commit, nil))
			mu.Lock()
			history[i] = append(history[i], commit)
			mu.Unlock()
			return commit
		}

		// Seed every key so reads never legitimately miss.
		var seedFinal uint64
		for i := 0; i < nKeys; i++ {
			seedFinal = commitKey(i)
		}

		// Managed-mode reads are only safe at a ts all smaller commits have reached
		// durability for (dgraph's oracle enforces this with WaitForTs). Each writer
		// commits its own timestamps in increasing order and publishes "done up to";
		// the minimum across writers is a readTs with no in-flight commits below it.
		doneUpTo := make([]atomic.Uint64, nWriters)
		for w := range doneUpTo {
			doneUpTo[w].Store(seedFinal)
		}
		safeTs := func() uint64 {
			safe := doneUpTo[0].Load()
			for w := 1; w < nWriters; w++ {
				if d := doneUpTo[w].Load(); d < safe {
					safe = d
				}
			}
			return safe
		}

		keys := make([][]byte, nKeys)
		for i := range keys {
			keys[i] = key(i)
		}

		// checkBatch reads all keys twice at readTs and asserts the coordination-free
		// invariants; it returns the versions seen for the stability comparison.
		checkBatch := func(readTs uint64) []uint64 {
			txn := db.NewTransactionAt(readTs, false)
			defer txn.Discard()
			items, err := txn.GetBatch(keys)
			require.NoError(t, err)
			require.Len(t, items, nKeys)
			versions := make([]uint64, nKeys)
			for i, item := range items {
				require.NotNilf(t, item, "seeded key %d absent at readTs %d", i, readTs)
				require.LessOrEqualf(t, item.Version(), readTs,
					"key %d: version beyond the snapshot", i)
				require.Equalf(t, val(item.Version()), batchVal(t, item),
					"key %d: value does not match the version that wrote it", i)
				versions[i] = item.Version()
			}
			again, err := txn.GetBatch(keys)
			require.NoError(t, err)
			for i, item := range again {
				require.Equalf(t, versions[i], item.Version(),
					"key %d: two batch reads at readTs %d disagree", i, readTs)
			}
			return versions
		}

		// Phase A: concurrent writers, readers and forced compactions.
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for w := 0; w < nWriters; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				for c := 0; c < commitsPerWriter; c++ {
					// Each writer owns a disjoint key set: per-key history stays ordered.
					doneUpTo[w].Store(commitKey((c*nWriters + w) % nKeys))
				}
			}()
		}
		wg.Add(1)
		go func() { // compaction churn while reads and writes are in flight
			defer wg.Done()
			for i := 0; i < 5; i++ {
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
				}
				require.NoError(t, db.Flatten(2))
			}
		}()
		var readerWg sync.WaitGroup
		for r := 0; r < 3; r++ {
			readerWg.Add(1)
			go func() {
				defer readerWg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					checkBatch(safeTs())
				}
			}()
		}
		wg.Wait()
		close(stop)
		readerWg.Wait()

		// Phase B: quiesced and force-compacted; expected versions are now exact.
		// Memtable flushes are asynchronous, so poll: flatten, then check that tables
		// actually landed below L0 — otherwise "survives compaction" tested nothing.
		require.Eventually(t, func() bool {
			require.NoError(t, db.Flatten(2))
			for _, lvl := range db.Levels() {
				if lvl.Level > 0 && lvl.NumTables > 0 {
					return true
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "no tables below L0: compaction never ran")

		expectedAt := func(i int, readTs uint64) uint64 {
			var e uint64
			for _, ts := range history[i] {
				if ts <= readTs && ts > e {
					e = ts
				}
			}
			return e
		}
		final := safeTs()
		// Check the final snapshot, a mid-history snapshot, and the exact boundary of
		// each key's newest commit (readTs == commitTs must see that commit: B11).
		for _, readTs := range []uint64{final, final / 2} {
			versions := checkBatch(readTs)
			for i := 0; i < nKeys; i++ {
				require.Equalf(t, expectedAt(i, readTs), versions[i],
					"key %d at readTs %d: wrong version after compaction", i, readTs)
			}
		}
		for i := 0; i < nKeys; i++ {
			newest := expectedAt(i, final)
			txn := db.NewTransactionAt(newest, false)
			items, err := txn.GetBatch([][]byte{key(i)})
			require.NoError(t, err)
			require.NotNilf(t, items[0], "key %d invisible at its own commitTs %d", i, newest)
			require.Equal(t, newest, items[0].Version())
			txn.Discard()
		}
	})
}
