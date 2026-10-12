/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
)

// TestZSTDCompressionLevelAcrossDBs is a regression test for
// https://github.com/dgraph-io/badger/issues/2178. Opening multiple DBs with
// different ZSTD compression levels must honor each DB's own level, instead of
// reusing the level of whichever DB first wrote a table.
func TestZSTDCompressionLevelAcrossDBs(t *testing.T) {
	const numEntries = 2000
	// value is fixed and highly repetitive, so the LSM blocks compress well and
	// the chosen level meaningfully changes the resulting SST size. A large value
	// keeps per-entry framing and bloom-filter overhead small relative to the
	// compressed data, so the level difference shows up clearly in the SST size.
	value := bytes.Repeat([]byte("zstd-compression-"), 60) // ~1 KiB

	// sstSize opens a fresh DB at the given level, writes identical compressible
	// data, flushes it to SST files, and returns their total size on disk.
	sstSize := func(t *testing.T, level int) int64 {
		t.Helper()
		dir, err := os.MkdirTemp("", "badger-zstd")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := DefaultOptions(dir).
			WithSyncWrites(false).
			WithLoggingLevel(WARNING).
			WithCompression(options.ZSTD).
			WithZSTDCompressionLevel(level)
		db, err := Open(opt)
		require.NoError(t, err)
		defer db.Close() // Safety net; Close is idempotent, so the explicit call below wins.

		wb := db.NewWriteBatch()
		for i := range numEntries {
			key := fmt.Appendf(nil, "%032d", i)
			require.NoError(t, wb.SetEntry(NewEntry(key, value)))
		}
		require.NoError(t, wb.Flush())
		// Close flushes the memtable to SST files on disk.
		require.NoError(t, db.Close())

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		var total int64
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".sst") {
				continue
			}
			info, err := e.Info()
			require.NoError(t, err)
			total += info.Size()
		}
		require.Greater(t, total, int64(0))
		return total
	}

	fast := sstSize(t, 1)  // SpeedFastest
	best := sstSize(t, 19) // SpeedBestCompression

	// The same data must occupy strictly less space at the higher level. With the
	// pre-fix code, the level-1 encoder created first leaked into the level-19 DB,
	// making these two sizes equal.
	require.Less(t, best, fast,
		"level 19 SSTs (%d bytes) should be smaller than level 1 SSTs (%d bytes)",
		best, fast)
}
