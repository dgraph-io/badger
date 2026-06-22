/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// drainL0 removes the first n tables from L0 and signals the stall cond, mimicking
// what runCompactDef does after an L0 compaction (deleteTables + signalL0Drained).
func drainL0(t *testing.T, db *DB, n int) {
	t.Helper()
	l0 := db.lc.levels[0]
	l0.Lock()
	if n > len(l0.tables) {
		n = len(l0.tables)
	}
	toDrop := l0.tables[:n]
	l0.tables = l0.tables[n:]
	for _, tab := range toDrop {
		l0.subtractSize(tab)
	}
	l0.Unlock()
	require.NoError(t, decrRefs(toDrop))
	l0.signalL0Drained()
}

// TestL0StallUnstallSignal drives L0 to the stall threshold, asserts that
// addLevel0Table blocks, and then asserts it promptly resumes (without a polling
// quantum) once L0 is drained and the stall cond is signalled. Compactions are
// disabled so the test fully controls the L0 table count.
func TestL0StallUnstallSignal(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumCompactors = 0
	opt.NumLevelZeroTables = 3
	opt.NumLevelZeroTablesStall = 4

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Fill L0 up to the stall threshold.
		l0 := db.lc.levels[0]
		l0.Lock()
		for i := 0; i < opt.NumLevelZeroTablesStall; i++ {
			tab := createEmptyTable(db)
			l0.tables = append(l0.tables, tab)
			l0.addSize(tab)
		}
		l0.Unlock()

		var added atomic.Bool
		done := make(chan struct{})
		go func() {
			tab := createEmptyTable(db)
			require.NoError(t, db.lc.addLevel0Table(tab))
			require.NoError(t, tab.DecrRef())
			added.Store(true)
			close(done)
		}()

		// The add must block while L0 is at/above the stall threshold.
		time.Sleep(200 * time.Millisecond)
		require.False(t, added.Load(), "addLevel0Table should stall at the threshold")

		// Drain one table below the stall threshold and signal. The waiter should
		// wake promptly via the cond (well under the old 10ms polling quantum loop,
		// and certainly well under this generous timeout).
		unblockStart := time.Now()
		drainL0(t, db, 1)

		select {
		case <-done:
			require.True(t, added.Load())
			t.Logf("resumed %v after signal", time.Since(unblockStart).Round(time.Microsecond))
		case <-time.After(5 * time.Second):
			t.Fatal("addLevel0Table did not resume after L0 was drained and signalled")
		}
	})
}

// TestL0StallCloseNoHang asserts that closing the DB while the flush goroutine is
// stalled in addLevel0Table does not hang. The flush goroutine must be woken on
// close and force-add its table so flushChan can drain and close. Run under -race.
func TestL0StallCloseNoHang(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 3
	// Small memtables so writes produce many L0 tables quickly.
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10

	db, err := Open(opt)
	require.NoError(t, err)

	// Pin L0 at the stall threshold and keep it there so the flush goroutine
	// stalls in addLevel0Table. We hold extra references so compaction can't
	// reduce the count from under us; we never release them until Close forces
	// the flush goroutine past the stall via IsClosed().
	l0 := db.lc.levels[0]
	l0.Lock()
	for i := 0; i < opt.NumLevelZeroTablesStall; i++ {
		tab := createEmptyTable(db)
		l0.tables = append(l0.tables, tab)
		l0.addSize(tab)
	}
	l0.Unlock()

	// Generate writes to force a memtable flush, which will stall in
	// addLevel0Table since L0 is already at the threshold.
	go func() {
		i := 0
		for {
			err := db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%d", i)),
					make([]byte, 4096))
			})
			if err != nil {
				return // DB closing.
			}
			i++
			if i > 10000 {
				return
			}
		}
	}()

	// Give the flush goroutine time to stall.
	time.Sleep(500 * time.Millisecond)

	closed := make(chan error, 1)
	go func() {
		closed <- db.Close()
	}()

	select {
	case err := <-closed:
		// Close may surface errNoRoom-derived errors via writes; we only assert it
		// returns (does not hang). InMemory close should be clean.
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("db.Close() hung while flush goroutine was stalled in addLevel0Table")
	}
}

// TestL0BackpressureNoRegression exercises the normal write path under real
// compaction with small memtables (so L0 backpressure is actually engaged) and
// verifies all writes complete correctly. This guards against behavioral or
// correctness regressions from the cond-based signalling.
func TestL0BackpressureNoRegression(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.MemTableSize = 1 << 16
	opt.ValueThreshold = 1 << 10
	opt.NumLevelZeroTables = 2
	opt.NumLevelZeroTablesStall = 4

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const n = 5000
		val := make([]byte, 512)
		for i := 0; i < n; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%08d", i)), val)
			}))
		}

		// Verify all keys read back correctly.
		require.NoError(t, db.View(func(txn *Txn) error {
			for i := 0; i < n; i++ {
				item, err := txn.Get([]byte(fmt.Sprintf("key-%08d", i)))
				if err != nil {
					return fmt.Errorf("get key-%08d: %w", i, err)
				}
				if int(item.ValueSize()) != len(val) {
					return fmt.Errorf("key-%08d: unexpected value size %d", i, item.ValueSize())
				}
			}
			return nil
		}))
	})
}

// TestL0StallSpuriousWakeupSafe ensures the wait loop re-checks the predicate: a
// signal that does NOT drop L0 below the stall threshold must not let the add
// proceed. We signal repeatedly without draining, then drain and confirm progress.
func TestL0StallSpuriousWakeupSafe(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumCompactors = 0
	opt.NumLevelZeroTables = 3
	opt.NumLevelZeroTablesStall = 4

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l0 := db.lc.levels[0]
		l0.Lock()
		for i := 0; i < opt.NumLevelZeroTablesStall; i++ {
			tab := createEmptyTable(db)
			l0.tables = append(l0.tables, tab)
			l0.addSize(tab)
		}
		l0.Unlock()

		done := make(chan struct{})
		var added atomic.Bool
		go func() {
			tab := createEmptyTable(db)
			require.NoError(t, db.lc.addLevel0Table(tab))
			require.NoError(t, tab.DecrRef())
			added.Store(true)
			close(done)
		}()

		// Spuriously signal without draining; the waiter must re-check and keep
		// waiting because L0 is still at the threshold.
		for i := 0; i < 20; i++ {
			l0.signalL0Drained()
			time.Sleep(5 * time.Millisecond)
		}
		require.False(t, added.Load(), "add must not proceed on a signal that doesn't drop L0")

		drainL0(t, db, 1)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("add did not resume after real drain")
		}
	})
}
