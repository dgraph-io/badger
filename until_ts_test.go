/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func untilTsManagedDB(t *testing.T) *DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-until-ts")
	require.NoError(t, err)
	opt := getTestOptions(dir)
	db, err := OpenManaged(opt)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		removeDir(dir)
	})
	return db
}

func commitAt(t *testing.T, db *DB, ts uint64, key, val string) {
	t.Helper()
	txn := db.NewTransactionAt(math.MaxUint64, true)
	require.NoError(t, txn.Set([]byte(key), []byte(val)))
	require.NoError(t, txn.CommitAt(ts, nil))
}

func deleteAt(t *testing.T, db *DB, ts uint64, key string) {
	t.Helper()
	txn := db.NewTransactionAt(math.MaxUint64, true)
	require.NoError(t, txn.Delete([]byte(key)))
	require.NoError(t, txn.CommitAt(ts, nil))
}

func collectVersions(t *testing.T, db *DB, readTs uint64, opt IteratorOptions) map[string][]uint64 {
	t.Helper()
	out := make(map[string][]uint64)
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())
		out[k] = append(out[k], item.Version())
	}
	return out
}

func TestUntilTsIteratorUntilOnly(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 10, "a", "v10")
	commitAt(t, db, 20, "a", "v20")
	commitAt(t, db, 30, "a", "v30")
	commitAt(t, db, 15, "b", "vb")

	opt := DefaultIteratorOptions
	opt.UntilTs = 20
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{20}, got["a"])
	require.Equal(t, []uint64{15}, got["b"])
}

func TestUntilTsIteratorSinceAndUntilWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 5, "k", "v5")
	commitAt(t, db, 10, "k", "v10")
	commitAt(t, db, 15, "k", "v15")
	commitAt(t, db, 20, "k", "v20")

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.SinceTs = 10
	opt.UntilTs = 20
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{20, 15}, got["k"])
}

func TestUntilTsIteratorUntilBoundaryInclusive(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 7, "x", "old")
	commitAt(t, db, 8, "x", "exact")
	commitAt(t, db, 9, "x", "new")

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.UntilTs = 8
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{8, 7}, got["x"])
}

func TestUntilTsIteratorReverseWithWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	for i, ts := range []uint64{2, 4, 6, 8} {
		commitAt(t, db, ts, fmt.Sprintf("r%d", i), "v")
	}
	opt := DefaultIteratorOptions
	opt.Reverse = true
	opt.SinceTs = 2
	opt.UntilTs = 6
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Item().Key()))
		require.Greater(t, it.Item().Version(), uint64(2))
		require.LessOrEqual(t, it.Item().Version(), uint64(6))
	}
	require.Equal(t, []string{"r2", "r1"}, keys)
}

func TestUntilTsIteratorInvalidWindowPanics(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "k", "v")
	opt := DefaultIteratorOptions
	opt.SinceTs = 10
	opt.UntilTs = 10
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	require.Panics(t, func() { _ = txn.NewIterator(opt) })
}

func TestUntilTsInvertedWindowPanics(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "k", "v")
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	inverted := DefaultIteratorOptions
	inverted.SinceTs = 5
	inverted.UntilTs = 4
	require.Panics(t, func() { _ = txn.NewIterator(inverted) })

	valid := DefaultIteratorOptions
	valid.SinceTs = 5
	valid.UntilTs = 6
	require.NotPanics(t, func() { txn.NewIterator(valid).Close() })

	lowerOnly := DefaultIteratorOptions
	lowerOnly.SinceTs = 9
	require.NotPanics(t, func() { txn.NewIterator(lowerOnly).Close() })

	upperOnly := DefaultIteratorOptions
	upperOnly.UntilTs = 9
	require.NotPanics(t, func() { txn.NewIterator(upperOnly).Close() })
}

func TestUntilTsKeyIteratorRespectsUntil(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "solo", "a")
	commitAt(t, db, 2, "solo", "b")
	commitAt(t, db, 3, "solo", "c")

	opt := DefaultIteratorOptions
	opt.UntilTs = 2
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewKeyIterator([]byte("solo"), opt)
	defer it.Close()
	var vers []uint64
	for it.Rewind(); it.Valid(); it.Next() {
		vers = append(vers, it.Item().Version())
	}
	require.Equal(t, []uint64{2, 1}, vers)
}

func TestUntilTsDeletedInsideWindowStillVisibleWithAllVersions(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "d", "alive")
	deleteAt(t, db, 2, "d")
	commitAt(t, db, 3, "d", "again")

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.SinceTs = 1
	opt.UntilTs = 2
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	var meta []bool
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		require.Equal(t, "d", string(item.Key()))
		meta = append(meta, item.IsDeletedOrExpired())
		require.LessOrEqual(t, item.Version(), uint64(2))
		require.Greater(t, item.Version(), uint64(1))
	}
	require.Equal(t, []bool{true}, meta)
}

func TestUntilTsStreamUntilWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 5, "s", "v5")
	commitAt(t, db, 10, "s", "v10")
	commitAt(t, db, 15, "s", "v15")

	stream := db.NewStreamAt(math.MaxUint64)
	stream.SinceTs = 5
	stream.UntilTs = 10
	var versions []uint64
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		require.NoError(t, err)
		for _, kv := range list.Kv {
			if string(kv.Key) == "s" {
				versions = append(versions, kv.Version)
			}
		}
		return nil
	}
	require.NoError(t, stream.Orchestrate(context.Background()))
	require.Equal(t, []uint64{10}, versions)
}

func TestUntilTsStreamInvalidWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "s", "v")
	stream := db.NewStreamAt(math.MaxUint64)
	stream.SinceTs = 8
	stream.UntilTs = 3
	stream.Send = func(buf *z.Buffer) error { return nil }
	require.Equal(t, ErrInvalidTsWindow, stream.Orchestrate(context.Background()))
}

func untilTsDB(t *testing.T) *DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-until-ts-open")
	require.NoError(t, err)
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		removeDir(dir)
	})
	return db
}

func untilTsSet(t *testing.T, db *DB, key, val string) uint64 {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte(key), []byte(val))
	}))
	return db.MaxVersion()
}

func collectVersionsNonManaged(t *testing.T, db *DB, opt IteratorOptions) map[string][]uint64 {
	t.Helper()
	out := make(map[string][]uint64)
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			out[k] = append(out[k], item.Version())
		}
		return nil
	}))
	return out
}

// Round-trip through a normal (non-managed) DB, the same style as backup_test.go.
func TestUntilTsBackupWindowRoundTrip(t *testing.T) {
	db := untilTsDB(t)
	vEarly := untilTsSet(t, db, "w", "early")
	vMid := untilTsSet(t, db, "w", "mid")
	vKeep := untilTsSet(t, db, "z", "keep")
	_ = untilTsSet(t, db, "w", "late")
	require.Greater(t, vMid, vEarly)
	require.Greater(t, vKeep, vMid)

	var buf bytes.Buffer
	_, err := db.BackupWindow(&buf, vEarly, vKeep)
	require.NoError(t, err)

	dir2, err := os.MkdirTemp("", "badger-until-restore")
	require.NoError(t, err)
	t.Cleanup(func() { removeDir(dir2) })
	db2, err := Open(getTestOptions(dir2))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db2.Close()) })
	require.NoError(t, db2.Load(&buf, 16))

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	got := collectVersionsNonManaged(t, db2, opt)
	require.Equal(t, []uint64{vMid}, got["w"])
	require.Equal(t, []uint64{vKeep}, got["z"])
	for _, vs := range got {
		for _, v := range vs {
			require.Greater(t, v, vEarly)
			require.LessOrEqual(t, v, vKeep)
		}
	}
}

func TestUntilTsBackupWindowRejectsBadBounds(t *testing.T) {
	db := untilTsDB(t)
	var buf bytes.Buffer
	_, err := db.BackupWindow(&buf, 5, 5)
	require.Equal(t, ErrInvalidTsWindow, err)
	_, err = db.BackupWindow(&buf, 5, 0)
	require.Equal(t, ErrInvalidTsWindow, err)
	_, err = db.BackupWindow(&buf, 5, 3)
	require.Equal(t, ErrInvalidTsWindow, err)
}

func TestUntilTsUntilZeroMeansDisabled(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "k", "a")
	commitAt(t, db, 2, "k", "b")
	opt := DefaultIteratorOptions
	opt.UntilTs = 0
	opt.AllVersions = true
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{2, 1}, got["k"])
}

func TestUntilTsReadTsStillCapsWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 5, "k", "v5")
	commitAt(t, db, 10, "k", "v10")
	commitAt(t, db, 15, "k", "v15")

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.UntilTs = 20
	// readTs=12 should hide version 15 even though UntilTs allows it.
	got := collectVersions(t, db, 12, opt)
	require.Equal(t, []uint64{10, 5}, got["k"])
}

// A deletion newer than UntilTs must not hide an older live value that is still
// inside the window. Versions above UntilTs are ignored as if absent.
func TestUntilTsTombstoneAboveUntilDoesNotHideOlder(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "k", "alive")
	deleteAt(t, db, 5, "k")

	opt := DefaultIteratorOptions
	opt.UntilTs = 1
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{1}, got["k"])

	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	it.Rewind()
	require.True(t, it.Valid())
	val, err := it.Item().ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("alive"), val)
	require.False(t, it.Item().IsDeletedOrExpired())
}

func TestUntilTsPrefixRespectsUntil(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "pre/a", "a1")
	commitAt(t, db, 3, "pre/a", "a3")
	commitAt(t, db, 2, "pre/b", "b2")
	commitAt(t, db, 4, "zzz", "z4")

	opt := DefaultIteratorOptions
	opt.Prefix = []byte("pre/")
	opt.AllVersions = true
	opt.UntilTs = 2
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{1}, got["pre/a"])
	require.Equal(t, []uint64{2}, got["pre/b"])
	_, hasZ := got["zzz"]
	require.False(t, hasZ)
}

func TestUntilTsSeekRespectsUntil(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 1, "a", "a1")
	commitAt(t, db, 5, "a", "a5")
	commitAt(t, db, 3, "b", "b3")
	commitAt(t, db, 7, "b", "b7")

	opt := DefaultIteratorOptions
	opt.AllVersions = true
	opt.SinceTs = 1
	opt.UntilTs = 5
	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	it.Seek([]byte("b"))
	require.True(t, it.Valid())
	require.Equal(t, "b", string(it.Item().Key()))
	require.Equal(t, uint64(3), it.Item().Version())
	it.Next()
	require.False(t, it.Valid())
}

func openManagedKeepAllUT(t *testing.T) *DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-until-ts-keep")
	require.NoError(t, err)
	opt := getTestOptions(dir)
	opt.NumVersionsToKeep = math.MaxInt32
	db, err := OpenManaged(opt)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		removeDir(dir)
	})
	return db
}

// Stream iteration is AllVersions; with multiple retained versions, every version
// inside (SinceTs, UntilTs] must be emitted — not only the newest.
func TestUntilTsStreamEmitsAllVersionsInWindow(t *testing.T) {
	db := openManagedKeepAllUT(t)
	commitAt(t, db, 2, "s", "v2")
	commitAt(t, db, 4, "s", "v4")
	commitAt(t, db, 6, "s", "v6")
	commitAt(t, db, 8, "s", "v8")

	stream := db.NewStreamAt(math.MaxUint64)
	stream.SinceTs = 2
	stream.UntilTs = 6
	var versions []uint64
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		require.NoError(t, err)
		for _, kv := range list.Kv {
			if string(kv.Key) == "s" {
				versions = append(versions, kv.Version)
			}
		}
		return nil
	}
	require.NoError(t, stream.Orchestrate(context.Background()))
	require.Equal(t, []uint64{6, 4}, versions)
}

func TestUntilTsBackupWindowValuesAndMultiVersion(t *testing.T) {
	db := untilTsDB(t)
	_ = untilTsSet(t, db, "w", "early")
	v1 := untilTsSet(t, db, "w", "mid-a")
	v2 := untilTsSet(t, db, "w", "mid-b")
	vZ := untilTsSet(t, db, "z", "keep")
	_ = untilTsSet(t, db, "w", "late")

	var buf bytes.Buffer
	_, err := db.BackupWindow(&buf, v1-1, vZ)
	require.NoError(t, err)

	dir2, err := os.MkdirTemp("", "badger-until-restore-multi")
	require.NoError(t, err)
	t.Cleanup(func() { removeDir(dir2) })
	opt := getTestOptions(dir2)
	opt.NumVersionsToKeep = math.MaxInt32
	db2, err := Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db2.Close()) })
	require.NoError(t, db2.Load(&buf, 16))

	iopt := DefaultIteratorOptions
	iopt.AllVersions = true
	got := collectVersionsNonManaged(t, db2, iopt)
	require.Equal(t, []uint64{v2, v1}, got["w"])
	require.Equal(t, []uint64{vZ}, got["z"])

	require.NoError(t, db2.View(func(txn *Txn) error {
		it := txn.NewIterator(iopt)
		defer it.Close()
		vals := map[string]map[uint64]string{}
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			if vals[k] == nil {
				vals[k] = map[uint64]string{}
			}
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			vals[k][item.Version()] = string(v)
		}
		require.Equal(t, "mid-a", vals["w"][v1])
		require.Equal(t, "mid-b", vals["w"][v2])
		require.Equal(t, "keep", vals["z"][vZ])
		return nil
	}))
}

// Walking backwards reaches a key's oldest version first and then climbs towards
// newer ones; that climb must stop at the top of the window.
func TestUntilTsReverseLatestVersionInsideWindow(t *testing.T) {
	db := openManagedKeepAllUT(t)
	commitAt(t, db, 2, "k", "v2")
	commitAt(t, db, 4, "k", "v4")
	commitAt(t, db, 6, "k", "v6")
	commitAt(t, db, 8, "k", "v8")

	opt := DefaultIteratorOptions
	opt.Reverse = true
	opt.UntilTs = 6

	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()

	var versions []uint64
	var values []string
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		versions = append(versions, item.Version())
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		values = append(values, string(val))
	}
	require.Equal(t, []uint64{6}, versions)
	require.Equal(t, []string{"v6"}, values)
}

// Versions inside the window stay visible after they have been written out to
// disk, even when the same file also holds versions above the upper bound.
func TestUntilTsWindowSurvivesFlushToDisk(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-until-ts-flush")
	require.NoError(t, err)
	t.Cleanup(func() { removeDir(dir) })

	opt := getTestOptions(dir)
	opt.NumVersionsToKeep = math.MaxInt32

	db, err := OpenManaged(opt)
	require.NoError(t, err)
	commitAt(t, db, 2, "k", "v2")
	commitAt(t, db, 4, "k", "v4")
	commitAt(t, db, 50, "k", "v50")
	commitAt(t, db, 60, "later", "v60")
	require.NoError(t, db.Close())

	db, err = OpenManaged(opt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	iopt := DefaultIteratorOptions
	iopt.AllVersions = true
	iopt.UntilTs = 4
	got := collectVersions(t, db, math.MaxUint64, iopt)
	require.Equal(t, []uint64{4, 2}, got["k"])
	_, hasLater := got["later"]
	require.False(t, hasLater)
}

func TestUntilTsDefaultIteratorPicksLatestInsideWindow(t *testing.T) {
	db := untilTsManagedDB(t)
	commitAt(t, db, 2, "k", "old")
	commitAt(t, db, 4, "k", "mid")
	commitAt(t, db, 8, "k", "new")

	opt := DefaultIteratorOptions
	opt.SinceTs = 2
	opt.UntilTs = 6
	got := collectVersions(t, db, math.MaxUint64, opt)
	require.Equal(t, []uint64{4}, got["k"])

	txn := db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(opt)
	defer it.Close()
	it.Rewind()
	require.True(t, it.Valid())
	val, err := it.Item().ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("mid"), val)
}

var _ = pb.KV{}
