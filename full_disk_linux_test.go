/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// mountTmpfs mounts a tmpfs of size bytes, with the other mount options given, on a new directory.
// It needs root with CAP_SYS_ADMIN, and skips the test without them.
func mountTmpfs(t *testing.T, size int, options ...string) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("mounting a small filesystem needs root")
	}
	dir := t.TempDir()
	options = append([]string{fmt.Sprintf("size=%d", size)}, options...)
	err := unix.Mount("tmpfs", dir, "tmpfs", 0, strings.Join(options, ","))
	if errors.Is(err, unix.EPERM) {
		t.Skip("mounting a small filesystem needs CAP_SYS_ADMIN")
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unix.Unmount(dir, unix.MNT_DETACH)) })
	return dir
}

// fillDisk takes the free space left in dir with a file, and returns its path.
func fillDisk(t *testing.T, dir string) string {
	t.Helper()
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(dir, &st))
	path := filepath.Join(dir, "filler")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	require.NoError(t, unix.Fallocate(int(f.Fd()), 0, 0, int64(st.Bavail)*int64(st.Bsize)))
	return path
}

// fillInodes creates empty files in a new directory under dir until the filesystem has no free
// inode, and returns that directory.
func fillInodes(t *testing.T, dir string) string {
	t.Helper()
	fill := filepath.Join(dir, "fill")
	require.NoError(t, os.Mkdir(fill, 0700))
	for i := 0; ; i++ {
		require.Less(t, i, 1<<16, "the filesystem did not run out of inodes")
		f, err := os.Create(filepath.Join(fill, fmt.Sprint(i)))
		if errors.Is(err, syscall.ENOSPC) {
			return fill
		}
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}

// skipWithoutAllocation skips the test unless a new memory-mapped file in dir has its blocks
// allocated, as with dgraph-io/ristretto#508. A write into a sparse mapping on a full disk ends the
// process with SIGBUS instead of returning ENOSPC.
func skipWithoutAllocation(t *testing.T, dir string) {
	t.Helper()
	mf, err := z.OpenMmapFile(filepath.Join(dir, "probe"), os.O_CREATE|os.O_RDWR|os.O_EXCL, 1<<20)
	require.ErrorIs(t, err, z.NewFile)
	var st unix.Stat_t
	require.NoError(t, unix.Fstat(int(mf.Fd.Fd()), &st))
	require.NoError(t, mf.Delete())
	if st.Blocks*512 < 1<<20 {
		t.Skip("needs memory-mapped files whose blocks are allocated (dgraph-io/ristretto#508)")
	}
}

// requireKeys checks that db holds every key in keys with its value.
func requireKeys(t *testing.T, db *DB, keys map[string][]byte) {
	t.Helper()
	require.NoError(t, db.View(func(txn *Txn) error {
		for k, v := range keys {
			item, err := txn.Get([]byte(k))
			if err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			got, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			if !bytes.Equal(got, v) {
				return fmt.Errorf("%s: wrong value", k)
			}
		}
		return nil
	}))
}

// On a full disk, writes and Open return an error that matches ENOSPC, every write acknowledged
// before stays readable, and the database writes again once space is freed.
func TestFullDisk(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int64
		random    bool
	}{
		{"ValueLog", 1 << 10, true},
		{"LSM", 64 << 10, true},
		{"LSMCompressible", 64 << 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := mountTmpfs(t, 48<<20)
			skipWithoutAllocation(t, dir)
			ballast := filepath.Join(dir, "ballast")
			require.NoError(t, os.WriteFile(ballast, make([]byte, 8<<20), 0666))
			opt := getTestOptions(filepath.Join(dir, "db")).
				WithMemTableSize(2 << 20).
				WithValueLogFileSize(4 << 20).
				WithBaseTableSize(1 << 20).
				WithNumMemtables(2).
				WithNumLevelZeroTables(2).
				WithNumLevelZeroTablesStall(4).
				WithValueThreshold(tc.threshold)
			db, err := Open(opt)
			require.NoError(t, err)
			var mu sync.Mutex
			acked := make(map[string][]byte)
			set := func(i int) error {
				key, val := fmt.Sprintf("key%06d", i), make([]byte, 4<<10)
				if tc.random {
					_, _ = rand.Read(val)
				} else {
					copy(val, key)
				}
				err := db.Update(func(txn *Txn) error { return txn.Set([]byte(key), val) })
				if err == nil {
					mu.Lock()
					acked[key] = val
					mu.Unlock()
				}
				return err
			}

			// A write on the full disk returns ENOSPC, or waits while L0 is stalled behind
			// compactions that cannot write, and reads wait behind it; it then goes through
			// once space is freed.
			n := 0
			var pending chan error
			for err == nil {
				require.Less(t, n, 64<<10, "the writes never ran out of space")
				n++
				done := make(chan error, 1)
				go func(i int) { done <- set(i) }(n)
				select {
				case err = <-done:
				case <-time.After(10 * time.Second):
					pending, err = done, errors.New("waiting")
				}
			}
			if pending == nil {
				require.ErrorIs(t, err, syscall.ENOSPC)
				require.ErrorIs(t, set(n), syscall.ENOSPC, "a second write on the full disk")
				requireKeys(t, db, acked)
			}

			require.NoError(t, os.Remove(ballast))
			if pending != nil {
				select {
				case err = <-pending:
					require.NoError(t, err, "the waiting write once space is freed")
				case <-time.After(time.Minute):
					t.Fatal("the waiting write did not go through once space was freed")
				}
			} else {
				require.NoError(t, set(n), "a write once space is freed")
			}
			requireKeys(t, db, acked)
			require.NoError(t, db.Close())

			filler := fillDisk(t, dir)
			_, err = Open(opt)
			require.ErrorIs(t, err, syscall.ENOSPC, "an open on the full disk")
			require.NoError(t, os.Remove(filler))

			db, err = Open(opt)
			require.NoError(t, err)
			requireKeys(t, db, acked)
			require.NoError(t, set(n+1))
			require.NoError(t, db.Close())
		})
	}
}

// A compaction whose table cannot be created, as on a full disk, fails and keeps its input.
func TestCompactionOnFullDisk(t *testing.T) {
	dir := mountTmpfs(t, 16<<20, "nr_inodes=64")
	opt := getTestOptions(dir).
		WithMemTableSize(1 << 20).
		WithValueLogFileSize(1 << 20).
		WithBaseTableSize(8 << 20).
		WithNumLevelZeroTables(100).
		WithNumLevelZeroTablesStall(200).
		WithValueThreshold(64 << 10)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	acked := make(map[string][]byte)
	for i := 0; len(db.Tables()) < 2; i++ {
		require.Less(t, i, 4096, "two memtables were not flushed")
		key, val := fmt.Sprintf("key%06d", i), make([]byte, 4<<10)
		_, _ = rand.Read(val)
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(key), val) }))
		acked[key] = val
	}

	db.stopCompactions()
	// Taking the free inodes makes the creation of the table's file fail. With ristretto v2.2.0,
	// taking the free blocks does not: the file is created sparse, and the write into its mapping
	// ends the process with SIGBUS.
	fill := fillInodes(t, dir)
	defer func() {
		require.NoError(t, os.RemoveAll(fill))
		db.startCompactions()
	}()
	require.ErrorIs(t, db.lc.doCompact(0, compactionPriority{level: 0, score: 1}), syscall.ENOSPC)
	requireKeys(t, db, acked)
}

// Discard stats that cannot grow, as on a full disk, drop the update instead of ending the process.
func TestDiscardStatsOnFullDisk(t *testing.T) {
	dir := mountTmpfs(t, 2<<20)
	skipWithoutAllocation(t, dir)
	ds, err := InitDiscardStats(DefaultOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, ds.Close(-1)) }()
	// One page holds 255 entries and the empty slot after them.
	require.NoError(t, ds.Truncate(4<<10))
	fillDisk(t, dir)

	for fid := uint32(1); fid < 256; fid++ {
		require.Equal(t, int64(1), ds.Update(fid, 1))
	}
	require.Zero(t, ds.Update(256, 1), "an update that needs the file to grow")
	require.Equal(t, 255, ds.nextEmptySlot)
	require.Equal(t, int64(2), ds.Update(1, 1), "an update of an entry kept")
}
