/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeDBWithSST creates a DB, writes enough keys to flush an on-disk SSTable,
// closes it, and returns the dir, the options used, and the first .sst path.
func writeDBWithSST(t *testing.T) (dir string, opts Options, sstPath string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	opts = getTestOptions(dir)

	db, err := Open(opts)
	require.NoError(t, err)
	for i := 0; i < 5000; i++ {
		err := db.Update(func(txn *Txn) error {
			return txn.Set(
				[]byte(fmt.Sprintf("key:%08d", i)),
				[]byte(fmt.Sprintf("value:%08d", i)))
		})
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			return dir, opts, filepath.Join(dir, e.Name())
		}
	}
	t.Fatal("expected at least one SST on disk")
	return dir, opts, ""
}

// TestOpenWithCorruptedFooterReturnsError mirrors the issue #2201 repro: the
// footer/index region is zeroed, which must be rejected by the footer
// validation and surfaced as a graceful Open error (not a fatalpanic).
func TestOpenWithCorruptedFooterReturnsError(t *testing.T) {
	dir, opts, sstPath := writeDBWithSST(t)
	defer removeDir(dir)

	data, err := os.ReadFile(sstPath)
	require.NoError(t, err)
	corruptSize := 300
	if len(data) < corruptSize {
		corruptSize = len(data)
	}
	for i := len(data) - corruptSize; i < len(data); i++ {
		data[i] = 0
	}
	require.NoError(t, os.WriteFile(sstPath, data, 0644))

	_, err = Open(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Data corrupted")
}

// TestOpenWithCorruptedBlockDataSucceeds corrupts only block data (the middle
// of the file), leaving the footer and index intact. Block data is read
// lazily, so Open must succeed rather than crash or error.
func TestOpenWithCorruptedBlockDataSucceeds(t *testing.T) {
	dir, opts, sstPath := writeDBWithSST(t)
	defer removeDir(dir)

	data, err := os.ReadFile(sstPath)
	require.NoError(t, err)
	mid := len(data) / 2
	for i := mid; i < mid+100 && i < len(data); i++ {
		data[i] = 0xFF
	}
	require.NoError(t, os.WriteFile(sstPath, data, 0644))

	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// TestOpenWithTinySSTableReturnsError truncates an SST below the 4-byte footer.
// initIndex then reads at a negative offset and panics, and the recover defer's
// debug collection panics again, so the panic escapes the table package
// and must be caught by the recover() in the newLevelsController goroutine.
// The result is a graceful Open error, not a crash.
func TestOpenWithTinySSTableReturnsError(t *testing.T) {
	dir, opts, sstPath := writeDBWithSST(t)
	defer removeDir(dir)

	require.NoError(t, os.Truncate(sstPath, 2))

	_, err := Open(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "opening table")
}
