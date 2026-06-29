//go:build windows
// +build windows

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// mmapFile memory-maps a file on Windows, simulating another process holding
// the file open. Returns a cleanup function that releases the mapping.
func mmapFile(t *testing.T, path string) func() {
	t.Helper()
	fd, err := os.OpenFile(path, os.O_RDWR, 0666)
	require.NoError(t, err)

	fi, err := fd.Stat()
	require.NoError(t, err)

	size := fi.Size()
	if size == 0 {
		fd.Close()
		t.Fatalf("file %s is empty, cannot mmap", path)
	}

	sizeLo := uint32(size)
	sizeHi := uint32(size >> 32)
	h, err := syscall.CreateFileMapping(syscall.Handle(fd.Fd()), nil,
		syscall.PAGE_READWRITE, sizeHi, sizeLo, nil)
	require.NoError(t, err)

	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	require.NoError(t, err)
	syscall.CloseHandle(h)

	return func() {
		syscall.UnmapViewOfFile(addr)
		fd.Close()
	}
}

// TestOpenWithBypassLockGuardWindows verifies that Open succeeds when another
// process holds memory-mapped files in the same directory. This is the common
// scenario when multiple CLI processes share a cache directory with
// WithBypassLockGuard(true).
//
// Without the fix, Open fails with ERROR_USER_MAPPED_FILE (1224) because
// WAL/vlog truncation cannot operate on files mapped by another process.
func TestOpenWithBypassLockGuardWindows(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithBypassLockGuard(true)

	// Open and write data to create .mem and .vlog files, then close cleanly.
	db, err := Open(opt)
	require.NoError(t, err)

	err = db.Update(func(txn *Txn) error {
		return txn.Set([]byte("key1"), []byte("val1"))
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Find .mem and .vlog files and hold them open via mmap, simulating
	// another process that has the DB open.
	var cleanups []func()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) == ".mem" || filepath.Ext(name) == ".vlog" {
			cleanup := mmapFile(t, filepath.Join(dir, name))
			cleanups = append(cleanups, cleanup)
			fmt.Printf("Holding mmap on %s\n", name)
		}
	}
	require.NotEmpty(t, cleanups, "expected .mem or .vlog files to mmap")

	// Re-open the DB while the files are externally memory-mapped.
	// Without the fix, this fails with ERROR_USER_MAPPED_FILE.
	db, err = Open(opt)
	require.NoError(t, err, "Open should succeed even when files are externally mmap'd")

	// Verify we can still read the data.
	err = db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("key1"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		require.Equal(t, []byte("val1"), val)
		return nil
	})
	require.NoError(t, err)

	// Release external mmaps first, then close the DB.
	for _, cleanup := range cleanups {
		cleanup()
	}
	require.NoError(t, db.Close())
}
