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
	"testing"

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
