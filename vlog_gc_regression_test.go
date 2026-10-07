/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/stretchr/testify/require"
)

func gcTestAdvanceRead(t *testing.T, db *DB) {
	t.Helper()
	require.NoError(t, db.View(func(tx *Txn) error { return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.orc.readMark.WaitForMark(ctx, db.MaxVersion()))
}

func gcTestFlush(t *testing.T, db *DB) {
	t.Helper()
	db.lock.Lock()
	old := db.mt
	next, err := db.newMemTable()
	if err != nil {
		db.lock.Unlock()
		t.Fatal(err)
	}
	db.mt = next
	db.lock.Unlock()
	require.NoError(t, db.handleMemTableFlush(old, nil))
	old.DecrRef()
}

func gcTestCompact(t *testing.T, db *DB) {
	t.Helper()
	gcTestFlush(t, db)
	require.NoError(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71}))
	require.NoError(t, db.Flatten(1))
}

func gcTestFiles(db *DB) []*logFile {
	db.vlog.filesLock.RLock()
	defer db.vlog.filesLock.RUnlock()
	var files []*logFile
	for id, file := range db.vlog.filesMap {
		if id < db.vlog.maxFid {
			files = append(files, file)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].fid < files[j].fid })
	return files
}

// Historical reads must keep working after GC moves the old value. Checking
// only the latest version in the rewrite scanner would incorrectly discard it.
func TestVlogGCHistoricalSnapshot(t *testing.T) {
	for _, deletion := range []bool{true, false} {
		name := "overwrite"
		if deletion {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithNumCompactors(0).
				WithCompactL0OnClose(false).WithValueThreshold(1024).
				WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
			db, err := Open(opt)
			require.NoError(t, err)
			key := []byte("history")
			oldValue := bytes.Repeat([]byte{1}, 2<<20)
			newValue := bytes.Repeat([]byte{2}, 2<<20)
			require.NoError(t, db.Update(func(tx *Txn) error { return tx.Set(key, oldValue) }))
			require.NoError(t, db.Close())
			db, err = Open(opt)
			require.NoError(t, err)
			defer db.Close()
			snapshot := db.NewTransaction(false)
			defer snapshot.Discard()
			require.NoError(t, db.Update(func(tx *Txn) error {
				if deletion {
					return tx.Delete(key)
				}
				return tx.Set(key, newValue)
			}))
			gcTestFlush(t, db)
			hookCalled := false
			db.vlogGCPauseHook = func() { hookCalled = true; gcTestCompact(t, db) }
			files := gcTestFiles(db)
			require.NotEmpty(t, files)
			for _, f := range files {
				require.NoError(t, db.vlog.rewrite(f))
			}
			db.vlogGCPauseHook = nil
			require.True(t, hookCalled)
			item, err := snapshot.Get(key)
			require.NoError(t, err)
			got, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, oldValue, got)
			require.NoError(t, db.View(func(tx *Txn) error {
				item, err := tx.Get(key)
				if deletion {
					require.ErrorIs(t, err, ErrKeyNotFound)
					return nil
				}
				if err != nil {
					return err
				}
				got, err := item.ValueCopy(nil)
				require.Equal(t, newValue, got)
				return err
			}))
			for _, f := range files {
				_, exists := db.vlog.filesMap[f.fid]
				require.False(t, exists, "old vlog file must be reclaimed after relocation")
			}
		})
	}
}

// Once GC exits, compaction must once again be able to drop both the old value
// and its tombstone, and a subsequent GC must reclaim the moved value's file.
func TestVlogGCReclamationAfterRewrite(t *testing.T) {
	opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithNumCompactors(0).
		WithCompactL0OnClose(false).WithValueThreshold(1024).
		WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
	db, err := Open(opt)
	require.NoError(t, err)
	key := []byte("garbage")
	require.NoError(t, db.Update(func(tx *Txn) error { return tx.Set(key, bytes.Repeat([]byte{3}, 2<<20)) }))
	require.NoError(t, db.Close())
	db, err = Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Txn) error { return tx.Delete(key) }))
	gcTestFlush(t, db)
	db.vlogGCPauseHook = func() { gcTestCompact(t, db) }
	for _, f := range gcTestFiles(db) {
		require.NoError(t, db.vlog.rewrite(f))
	}
	db.vlogGCPauseHook = nil
	require.False(t, db.gcActive.Load())
	gcTestAdvanceRead(t, db)
	gcTestCompact(t, db)
	vs, err := db.get(y.KeyWithTs(key, ^uint64(0)))
	require.NoError(t, err)
	require.Empty(t, vs.Value, "all deleted versions must become reclaimable after GC")
	require.Zero(t, vs.Meta, "tombstone must also have been removed")
	require.NoError(t, db.Close())
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()
	files := gcTestFiles(db)
	require.NotEmpty(t, files)
	var reclaimed int64
	for _, f := range files {
		reclaimed += int64(f.size.Load())
		require.NoError(t, db.vlog.rewrite(f))
		_, exists := db.vlog.filesMap[f.fid]
		require.False(t, exists)
	}
	require.Greater(t, reclaimed, int64(2<<20))
	t.Logf("reclaimed sealed vlog bytes after GC protection was released: %d", reclaimed)
	require.ErrorIs(t, db.View(func(tx *Txn) error { _, err := tx.Get(key); return err }), ErrKeyNotFound)
}

// The compaction started before GC and is allowed to purge the deleted key.
// GC must wait for its output to be installed before consulting the LSM.
func TestVlogGCWaitsForExistingCompaction(t *testing.T) {
	opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithNumCompactors(0).
		WithCompactL0OnClose(false).WithValueThreshold(1024).
		WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
	db, err := Open(opt)
	require.NoError(t, err)
	key := []byte("inflight")
	require.NoError(t, db.Update(func(tx *Txn) error { return tx.Set(key, bytes.Repeat([]byte{4}, 2<<20)) }))
	require.NoError(t, db.Close())
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(func(tx *Txn) error { return tx.Delete(key) }))
	gcTestFlush(t, db)
	gcTestAdvanceRead(t, db)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCompaction := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCompaction()
	db.compactionGCPauseHook = func() { close(entered); <-release }
	compactDone := make(chan error, 1)
	go func() { compactDone <- db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71}) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not start")
	}
	files := gcTestFiles(db)
	require.NotEmpty(t, files)
	scanned := make(chan struct{})
	db.vlogGCPauseHook = func() { close(scanned) }
	gcDone := make(chan error, 1)
	go func() { gcDone <- db.vlog.rewrite(files[0]) }()
	select {
	case <-scanned:
		releaseCompaction()
		t.Fatal("GC scanned while earlier compaction was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	require.False(t, db.gcActive.Load(), "GC must drain the existing compaction before publication")
	releaseCompaction()
	select {
	case err := <-compactDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction stalled")
	}
	select {
	case err := <-gcDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("GC stalled")
	}
	db.compactionGCPauseHook = nil
	db.vlogGCPauseHook = nil
	require.ErrorIs(t, db.View(func(tx *Txn) error { _, err := tx.Get(key); return err }), ErrKeyNotFound)
}

func TestVlogGCReleasesProtectionOnWriteError(t *testing.T) {
	opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithNumCompactors(0).
		WithCompactL0OnClose(false).WithValueThreshold(1024).
		WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
	db, err := Open(opt)
	require.NoError(t, err)
	key := []byte("write-error")
	require.NoError(t, db.Update(func(tx *Txn) error { return tx.Set(key, bytes.Repeat([]byte{5}, 2<<20)) }))
	require.NoError(t, db.Close())
	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()
	files := gcTestFiles(db)
	require.NotEmpty(t, files)
	file := files[0]
	db.vlogGCPauseHook = func() { db.blockWrites.Store(1) }
	defer func() { db.blockWrites.Store(0); db.vlogGCPauseHook = nil }()
	require.ErrorIs(t, db.vlog.rewrite(file), ErrBlockedWrites)
	require.False(t, db.gcActive.Load(), "failed GC must release compaction protection")
	require.True(t, db.gcCompactionMu.TryLock(), "failed GC must release publication gate")
	db.gcCompactionMu.Unlock()
	_, exists := db.vlog.filesMap[file.fid]
	require.True(t, exists, "failed rewrite must retain the original vlog")
}
