//go:build linux

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestSyncCoversImmutableMemtables checks that DB.Sync makes durable an entry
// whose memtable was rotated out, and not yet flushed, before Sync was called.
// It cannot cut power, so it asks the kernel: once Sync returns, the page cache
// must hold no dirty or under-writeback page of that memtable's WAL.
func TestSyncCoversImmutableMemtables(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	var fsStat unix.Statfs_t
	require.NoError(t, unix.Statfs(dir, &fsStat))
	if fsStat.Type == unix.TMPFS_MAGIC || fsStat.Type == unix.RAMFS_MAGIC {
		t.Skip("tmpfs never writes back, so msync leaves its pages dirty; set TMPDIR to a disk")
	}

	// No compactors and a level-0 stall of 2: the first two memtable flushes
	// fill level 0 and the third waits, so its memtable stays in db.imm.
	opt := getTestOptions(dir).
		WithMemTableSize(1 << 20).
		WithValueThreshold(1 << 10).
		WithNumCompactors(0).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	numImm := func() int {
		db.lock.RLock()
		defer db.lock.RUnlock()
		return len(db.imm)
	}
	key := 0
	writeBatch := func() {
		require.NoError(t, db.Update(func(txn *Txn) error {
			for range 64 {
				if err := txn.Set([]byte(fmt.Sprintf("fill-%09d", key)), make([]byte, 512)); err != nil {
					return err
				}
				key++
			}
			return nil
		}))
	}
	waitForNoImm := func() {
		deadline := time.Now().Add(30 * time.Second)
		for numImm() > 0 {
			require.True(t, time.Now().Before(deadline), "memtable flush did not finish")
			time.Sleep(10 * time.Millisecond)
		}
	}
	for db.lc.levels[0].numTables() < 2 {
		writeBatch()
		waitForNoImm()
	}

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("unsynced"), []byte("committed before the rotation"))
	}))
	for numImm() == 0 {
		writeBatch()
	}
	db.lock.RLock()
	walPath := db.imm[0].wal.Fd.Name()
	db.lock.RUnlock()

	pageCache := func() unix.Cachestat_t {
		f, err := os.Open(walPath)
		require.NoError(t, err)
		defer f.Close()
		var cs unix.Cachestat_t
		err = unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &cs, 0)
		if errors.Is(err, unix.ENOSYS) {
			t.Skip("cachestat(2) needs Linux 6.5 or later")
		}
		require.NoError(t, err)
		return cs
	}
	if pageCache().Dirty == 0 {
		t.Skip("the kernel wrote the WAL back before Sync; this run cannot tell")
	}

	require.NoError(t, db.Sync())

	require.Equal(t, 1, numImm(), "the immutable memtable was flushed during the test")
	after := pageCache()
	require.Zero(t, after.Dirty, "dirty pages left in immutable memtable WAL %s after Sync", walPath)
	require.Zero(t, after.Writeback, "pages under writeback in immutable memtable WAL %s after Sync", walPath)
}
