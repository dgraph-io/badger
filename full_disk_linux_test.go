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
	"syscall"
	"testing"

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
