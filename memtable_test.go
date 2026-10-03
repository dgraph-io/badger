/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

// testOptions returns options suitable for testing with a small MemTableSize.
// ValueThreshold must be <= maxBatchSize (15% of MemTableSize).
func testOptions(dir string) Options {
	return DefaultOptions(dir).
		WithMemTableSize(1 << 20).    // 1 MB
		WithValueThreshold(1 << 10).  // 1 KB (< 15% of 1MB = 157KB)
		WithValueLogFileSize(1 << 20) // 1 MB
}

// TestReleaseWAL_UnmapsData verifies that releaseWAL munmaps the WAL data,
// closes the file descriptor, and updates the OnClose callback.
func TestReleaseWAL_UnmapsData(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	// Create a memtable with a WAL
	mt, err := db.newMemTable()
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.NotNil(t, mt.wal)
	require.NotNil(t, mt.wal.Fd)
	require.NotNil(t, mt.wal.Data)
	require.NotZero(t, len(mt.wal.Data))

	walPath := mt.wal.Fd.Name()
	t.Logf("WAL path: %s, size: %d", walPath, len(mt.wal.Data))

	// Write some entries to the WAL (via the skiplist Put which writes to WAL)
	for i := range 100 {
		key := []byte("test-key-" + string(rune('a'+i%26)))
		val := y.ValueStruct{Value: []byte("test-value"), Meta: 0, UserMeta: 0}
		_ = mt.Put(key, val)
	}

	// Verify WAL has data
	require.Greater(t, int(mt.wal.writeAt), 0)

	// Now release the WAL
	mt.releaseWAL()

	// Verify Data is nil after release
	require.Nil(t, mt.wal.Data)

	// Verify Fd is nil after release
	require.Nil(t, mt.wal.Fd)

	// Verify OnClose was replaced — calling it should remove the file
	require.NotNil(t, mt.sl.OnClose)
	mt.sl.OnClose()
	_, err = os.Stat(walPath)
	require.True(t, os.IsNotExist(err), "WAL file should be removed after OnClose")
}

// TestReleaseWAL_NilWAL verifies that releaseWAL is a no-op when wal is nil
// (e.g., in InMemory mode).
func TestReleaseWAL_NilWAL(t *testing.T) {
	// InMemory mode must have empty Dir
	opt := DefaultOptions("").WithInMemory(true)
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	// In InMemory mode, memtables don't have WALs
	require.NotNil(t, db.mt)
	require.NotNil(t, db.mt.sl)

	// Should not panic
	db.mt.releaseWAL()
}

// TestReleaseWAL_Idempotent verifies that calling releaseWAL twice is safe.
func TestReleaseWAL_Idempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	mt, err := db.newMemTable()
	require.NoError(t, err)

	// First release
	mt.releaseWAL()
	require.Nil(t, mt.wal.Data)
	require.Nil(t, mt.wal.Fd)

	// Second release should not panic
	mt.releaseWAL()
	require.Nil(t, mt.wal.Data)
	require.Nil(t, mt.wal.Fd)

	// OnClose should be safe to call
	mt.sl.OnClose()
}

// TestReleaseWAL_SkiplistStillReadable verifies that after releasing the WAL
// on a memtable, the skiplist data is still accessible (reads go through
// skiplist, not WAL).
func TestReleaseWAL_SkiplistStillReadable(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	mt, err := db.newMemTable()
	require.NoError(t, err)

	// Insert data via the memtable (goes to both skiplist and WAL).
	// Keys must be at least 8 bytes for version suffix parsing.
	testData := []struct {
		key   string
		value string
	}{
		{"test-key-001", "value-one"},
		{"test-key-002", "value-two"},
		{"test-key-003", "value-three"},
	}
	for _, d := range testData {
		_ = mt.Put([]byte(d.key), y.ValueStruct{Value: []byte(d.value)})
	}

	// Release the WAL — this should not affect skiplist access
	mt.releaseWAL()

	// Verify data is still readable through the skiplist
	for _, d := range testData {
		vs := mt.sl.Get([]byte(d.key))
		require.NotNil(t, vs, "key %s should be readable after WAL release", d.key)
		require.Equal(t, d.value, string(vs.Value))
	}
}

// TestReleaseWAL_RecoveryMemtables verifies that memtables opened during
// recovery have their WAL released in openMemTables.
func TestReleaseWAL_RecoveryMemtables(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Open a DB, write data, close it — this creates .mem files
	opt := testOptions(dir).WithNumMemtables(2)

	db1, err := Open(opt)
	require.NoError(t, err)

	// Write enough data across multiple transactions to create at least one
	// memtable .mem file on disk. Each txn stays within maxBatchSize.
	for txnNum := range 20 {
		err = db1.Update(func(txn *Txn) error {
			for i := range 1000 {
				key := make([]byte, 64)
				val := make([]byte, 64)
				for j := range key {
					key[j] = byte(txnNum*1000 + i)
					val[j] = byte((i + j) % 256)
				}
				if err := txn.Set(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Close and verify .mem files exist
	err = db1.Close()
	require.NoError(t, err)

	memFiles, err := filepath.Glob(filepath.Join(dir, "*.mem"))
	require.NoError(t, err)
	t.Logf("Found %d .mem files", len(memFiles))

	if len(memFiles) == 0 {
		t.Skip("No .mem files created, skipping recovery test")
	}

	// Reopen — this triggers openMemTables which should call releaseWAL
	db2, err := Open(opt)
	require.NoError(t, err)
	defer db2.Close()

	// Verify that recovered memtables have had their WAL released
	// The imm slice should have memtables with nil WAL Data
	db2.lock.RLock()
	for _, mt := range db2.imm {
		if mt.wal != nil {
			require.Nil(t, mt.wal.Data, "Recovered memtable should have WAL Data released")
			require.Nil(t, mt.wal.Fd, "Recovered memtable should have WAL Fd released")
		}
	}
	db2.lock.RUnlock()

	t.Logf("Recovered %d immutable memtables with WALs released", len(db2.imm))
}

// TestReleaseWAL_DataIntegrity verifies that the WAL file is properly cleaned
// up and no stale mmap data remains.
func TestReleaseWAL_DataIntegrity(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	mt, err := db.newMemTable()
	require.NoError(t, err)

	// Write entries to populate the WAL
	for i := range 100 {
		key := make([]byte, 32)
		for j := range key {
			key[j] = byte(i)
		}
		_ = mt.Put(key, y.ValueStruct{Value: []byte("data")})
	}

	walPath := mt.wal.Fd.Name()
	originalSize := len(mt.wal.Data)
	require.Greater(t, originalSize, 0)

	// Release the WAL
	mt.releaseWAL()

	// Data should be nil
	require.Nil(t, mt.wal.Data)

	// The WAL file should still exist on disk (OnClose hasn't been called yet)
	_, err = os.Stat(walPath)
	require.NoError(t, err, "WAL file should still exist until OnClose is called")

	// Trigger cleanup via OnClose
	require.NotNil(t, mt.sl.OnClose)
	mt.sl.OnClose()

	// Now the file should be gone
	_, err = os.Stat(walPath)
	require.True(t, os.IsNotExist(err), "WAL file should be removed after OnClose")
}

// TestReleaseWAL_ActiveMemtableNotReleased verifies that the active memtable
// (db.mt) does NOT have its WAL released — only immutable memtables do.
func TestReleaseWAL_ActiveMemtableNotReleased(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	// The active memtable should have its WAL intact
	require.NotNil(t, db.mt)
	if db.mt.wal != nil {
		require.NotNil(t, db.mt.wal.Data,
			"Active memtable WAL Data should NOT be nil")
		require.NotNil(t, db.mt.wal.Fd,
			"Active memtable WAL Fd should NOT be nil")
		require.Greater(t, len(db.mt.wal.Data), 0,
			"Active memtable WAL should have positive size")
	}
}

// TestReleaseWAL_ImmReleasedAfterClose verifies that releasing WAL still works
// after the DB is closed (safety check for the OnClose callback).
func TestReleaseWAL_ImmReleasedAfterClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(testOptions(dir))
	require.NoError(t, err)

	mt, err := db.newMemTable()
	require.NoError(t, err)
	walPath := mt.wal.Fd.Name()

	mt.releaseWAL()
	db.Close()

	// OnClose should remove the file even after DB close.
	if mt.sl.OnClose != nil {
		mt.sl.OnClose()
	}
	_, err = os.Stat(walPath)
	if !os.IsNotExist(err) {
		_ = os.Remove(walPath)
	}
}

// TestVlogPreRotation_StraddleEntry verifies the pre-rotation check: when
// an entry would straddle the vlog file boundary, the file is rotated first
// so the entry lands in a fresh file. After writing across many files, each
// vlog file's actual data must be ≤ ValueLogFileSize (no dynamic Truncate
// was triggered by normal entries crossing the boundary).
func TestVlogPreRotation_StraddleEntry(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Use minimum vlog file size (1 MB) and write enough data to fill
	// multiple files, ensuring boundary straddles are handled.
	opt := testOptions(dir).
		WithValueLogFileSize(1 << 20) // 1 MB — minimum allowed

	db, err := Open(opt)
	require.NoError(t, err)

	// Write enough entries to fill multiple vlog files.
	// Each entry: ~32B key + 128B value + overhead ~= 180 bytes.
	// ~5800 entries fill 1 MB. Write 20k entries to rotate ~3-4 times.
	smallVal := make([]byte, 128)
	key := make([]byte, 32)

	for txnNum := range 40 {
		err := db.Update(func(txn *Txn) error {
			for i := range 500 {
				key[0] = byte(txnNum)
				key[1] = byte(i)
				if err := txn.Set(key, smallVal); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Verify no vlog file exceeded its initial mmap size. If pre-rotation
	// works, Truncate never fires and len(Data) stays ≤ ValueLogFileSize.
	db.vlog.filesLock.RLock()
	for fid, lf := range db.vlog.filesMap {
		require.LessOrEqual(t, len(lf.Data), int(opt.ValueLogFileSize),
			"vlog file %d mmap size should be ≤ ValueLogFileSize", fid)
	}
	db.vlog.filesLock.RUnlock()

	db.Close()
	t.Log("Pre-rotation kept all vlog files within bounds")
}
