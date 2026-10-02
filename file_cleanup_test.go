/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func newCleanupMapping(t *testing.T) (*z.MmapFile, string, []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keep.wal")
	contents := bytes.Repeat([]byte("preserve this data"), 256)
	require.NoError(t, os.WriteFile(path, contents, 0600))
	mf, err := z.OpenMmapFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		if len(mf.Data) > 0 {
			require.NoError(t, z.Munmap(mf.Data))
		}
		if mf.Fd != nil {
			require.NoError(t, mf.Fd.Close())
		}
	})
	return mf, path, contents
}

func TestCloseMmapOnErrorSyncFailure(t *testing.T) {
	mf, path, contents := newCleanupMapping(t)
	fd := mf.Fd
	syncErr := errors.New("injected sync failure")
	var unmapped bool
	err := closeMmapOnErrorWith(mf, func([]byte) error { return syncErr }, func(data []byte) error {
		unmapped = true
		return z.Munmap(data)
	})
	require.ErrorIs(t, err, syncErr)
	require.True(t, unmapped)
	require.Nil(t, mf.Data)
	require.Nil(t, mf.Fd)
	_, err = fd.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, got)
	// Completed operations must not run again on a retry.
	unexpected := func([]byte) error { t.Fatal("repeated mmap operation after release"); return nil }
	require.NoError(t, closeMmapOnErrorWith(mf, unexpected, unexpected))
}

func TestCloseMmapOnErrorUnmapFailure(t *testing.T) {
	mf, path, contents := newCleanupMapping(t)
	fd := mf.Fd
	syncErr := errors.New("injected sync failure")
	unmapErr := errors.New("injected unmap failure")
	err := closeMmapOnErrorWith(mf,
		func([]byte) error { return syncErr }, func([]byte) error { return unmapErr })
	require.ErrorIs(t, err, syncErr)
	require.ErrorIs(t, err, unmapErr)
	require.Equal(t, contents, mf.Data, "failed unmap must keep its mapping reachable")
	require.Nil(t, mf.Fd)
	_, err = fd.Stat()
	require.ErrorIs(t, err, os.ErrClosed, "unmap failure must not prevent descriptor close")
	// The enclosing owner retries only the still-live mapping, not the closed FD.
	lf := &logFile{MmapFile: mf, path: path}
	require.NoError(t, lf.closeOnError())
	require.Nil(t, lf.MmapFile)
	require.Nil(t, mf.Data)
	require.NoError(t, lf.closeOnError())
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, got)
}

func TestCloseMmapOnErrorCloseFailure(t *testing.T) {
	mf, path, contents := newCleanupMapping(t)
	require.NoError(t, mf.Fd.Close())
	err := closeMmapOnError(mf)
	require.ErrorIs(t, err, os.ErrClosed)
	require.Nil(t, mf.Data)
	require.Nil(t, mf.Fd)
	require.NoError(t, closeMmapOnError(mf))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, got)
}

func bootstrapFailureLog(t *testing.T) *logFile {
	t.Helper()
	dir := t.TempDir()
	opt := openFailureOptions(dir).WithEncryptionKey(bytes.Repeat([]byte{0x11}, 32))
	kr, err := OpenKeyRegistry(KeyRegistryOptions{
		Dir: dir, EncryptionKey: opt.EncryptionKey, EncryptionKeyRotationDuration: time.Hour,
	})
	require.NoError(t, err)
	// A closed registry makes the real bootstrap fail while persisting its first key.
	require.NoError(t, kr.Close())
	lf := &logFile{path: filepath.Join(dir, "00001.mem"), fid: 1, opt: opt, registry: kr}
	t.Cleanup(func() { require.NoError(t, lf.closeOnError()) })
	return lf
}

func TestLogFileBootstrapFailureRemovesFile(t *testing.T) {
	lf := bootstrapFailureLog(t)
	err := lf.open(lf.path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 4096)
	require.ErrorContains(t, err, "Error while retrieving datakey")
	require.Nil(t, lf.MmapFile)
	require.NoFileExists(t, lf.path, "Windows requires unmapping and closing before removal")

	// The same file ID must be available for a successful retry.
	kr, err := OpenKeyRegistry(lf.registry.opt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kr.Close()) })
	lf.registry = kr
	require.ErrorIs(t, lf.open(lf.path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 4096), z.NewFile)
	require.NoError(t, lf.closeOnError())
}

func TestLogFileBootstrapFailureReturnsRemoveError(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires Unix directory permissions enforced for a non-root user")
	}
	lf := bootstrapFailureLog(t)
	// Ristretto also reports NewFile for an existing zero-length file. It can be
	// opened for writing without permission to remove entries from its directory.
	require.NoError(t, os.WriteFile(lf.path, nil, 0600))
	dir := filepath.Dir(lf.path)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0700)) })
	err := lf.open(lf.path, os.O_RDWR, 4096)
	require.ErrorContains(t, err, "Error while retrieving datakey")
	require.ErrorIs(t, err, os.ErrPermission)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, "remove", pathErr.Op)
	require.Equal(t, lf.path, pathErr.Path)
	require.Nil(t, lf.MmapFile)
	require.FileExists(t, lf.path)
}
