/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxnFinishMarkerInline(t *testing.T) {
	for _, threshold := range []int64{1, 10, 20, 21} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("threshold=%d/cached=%v", threshold, cached), func(t *testing.T) {
				value := []byte(strconv.FormatUint(^uint64(0), 10))
				marker := &Entry{Value: value, meta: bitFinTxn}
				if cached {
					marker.estimateSizeAndSetThreshold(threshold)
				}
				require.True(t, marker.skipVlogAndSetThreshold(threshold), "finish markers must stay inline")
				// Ordinary transaction entries still obey the value threshold, including equality.
				entry := &Entry{Value: value, meta: bitTxn}
				require.Equal(t, int64(len(value)) < threshold, entry.skipVlogAndSetThreshold(threshold))
			})
		}
	}
}

func TestTxnFinishMarkerWALRecovery(t *testing.T) {
	for _, threshold := range []int64{1, 10} {
		t.Run(fmt.Sprintf("threshold=%d", threshold), func(t *testing.T) {
			dir := t.TempDir()
			opt := getTestOptions(dir).WithValueThreshold(threshold).
				WithMemTableSize(1 << 20).WithValueLogFileSize(1 << 20).WithLogger(nil)
			db, err := OpenManaged(opt)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()

			const startTs uint64 = 1_000_000_000
			before := db.vlog.woffset()
			for i := 0; i < 3; i++ {
				txn := db.NewTransactionAt(startTs+uint64(i), true)
				require.NoError(t, txn.Set([]byte(fmt.Sprintf("key-%d", i)), nil))
				require.NoError(t, txn.CommitAt(startTs+uint64(i), nil))
			}
			require.Empty(t, db.imm)
			tables, err := filepath.Glob(filepath.Join(dir, "*.sst"))
			require.NoError(t, err)
			require.Empty(t, tables, "committed writes must still be in the WAL, not SSTs")
			if after := db.vlog.woffset(); after != before {
				t.Errorf("nil-value commits wrote finish markers to vlog: offset %d -> %d", before, after)
			}

			// Also exercise legitimate vlog pointers alongside inline finish markers.
			largeValue := bytes.Repeat([]byte("v"), 32)
			txn := db.NewTransactionAt(startTs+3, true)
			require.NoError(t, txn.Set([]byte("large"), largeValue))
			require.NoError(t, txn.CommitAt(startTs+3, nil))
			require.Greater(t, db.vlog.woffset(), before)
			require.NoError(t, db.Sync())

			// Snapshot without closing: Close would flush the memtable and bypass WAL recovery.
			copyPath := t.TempDir()
			require.NoError(t, copyDir(dir, copyPath))
			recovered, err := OpenManaged(opt.WithDir(copyPath).WithValueDir(copyPath))
			require.NoError(t, err)
			defer func() { require.NoError(t, recovered.Close()) }()
			read := recovered.NewTransactionAt(startTs+3, false)
			defer read.Discard()
			for i := 0; i < 3; i++ {
				_, err := read.Get([]byte(fmt.Sprintf("key-%d", i)))
				require.NoError(t, err, "WAL recovery lost committed key %d", i)
			}
			item, err := read.Get([]byte("large"))
			require.NoError(t, err)
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, largeValue, value)
		})
	}
}
