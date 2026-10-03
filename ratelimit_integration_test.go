/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeAndVerify writes n key/value pairs into db (in batches), flushes via
// close-reopen to force the data through compaction, and asserts every value
// reads back correctly. It returns nothing; failures are reported via t.
func ratelimitWriteAndVerify(t *testing.T, db *DB, n int, valSize int) {
	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte(i)
	}
	// Write in batches so we create many tables (small BaseTableSize forces
	// these into multiple SSTables and triggers compaction).
	wb := db.NewWriteBatch()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		require.NoError(t, wb.Set(key, val))
		if i%1000 == 999 {
			require.NoError(t, wb.Flush())
			wb = db.NewWriteBatch()
		}
	}
	require.NoError(t, wb.Flush())

	// Verify all keys are intact.
	err := db.View(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key-%08d", i))
			item, err := txn.Get(key)
			if err != nil {
				return fmt.Errorf("get %s: %w", key, err)
			}
			got, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Len(t, got, valSize)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompactionRateLimitCorrectness opens a DB with CompactionBytesPerSec set,
// writes enough data to trigger compaction, and asserts the data survives
// intact. Throttling must not affect correctness.
func TestCompactionRateLimitCorrectness(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-ratelimit")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := getTestOptions(dir).
		WithBaseTableSize(1 << 20).
		WithValueThreshold(1 << 10).
		WithNumCompactors(2).
		// Throttle compaction to 4 MiB/s; the workload below moves enough bytes
		// through compaction that the limiter is exercised.
		WithCompactionBytesPerSec(4 << 20)
	require.Equal(t, int64(4<<20), opts.CompactionBytesPerSec)

	db, err := Open(opts)
	require.NoError(t, err)

	// ~10k * 1KiB = ~10 MiB of data -> several tables -> compaction runs.
	ratelimitWriteAndVerify(t, db, 10000, 1024)

	require.NoError(t, db.Close())

	// Reopen and verify the data is still intact after compaction + restart.
	db2, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	err = db2.View(func(txn *Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("key-%08d", 5000)))
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Len(t, v, 1024)
		return nil
	})
	require.NoError(t, err)
}

// TestCompactionRateLimitDisabledByDefault verifies that the default option
// (CompactionBytesPerSec == 0) constructs no limiter and behaves exactly as
// before, with data intact.
func TestCompactionRateLimitDisabledByDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-ratelimit-off")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := getTestOptions(dir).
		WithBaseTableSize(1 << 20).
		WithNumCompactors(2)
	// Default must be unlimited.
	require.Equal(t, int64(0), opts.CompactionBytesPerSec)

	db, err := Open(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// No limiter should have been constructed.
	require.Nil(t, db.lc.compactionLimiter)

	ratelimitWriteAndVerify(t, db, 5000, 1024)
}
