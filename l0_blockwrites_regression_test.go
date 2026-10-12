/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// TestDropPrepareDrainDoesNotDropWrites is a regression guard for Badger #2308.
//
// Expected result:
//   - GREEN on v4.9.2 (busy-sleep drain that waits for room).
//   - GREEN on v4.9.3 + the fix (ensureRoomForWrite bails only on IsClosed()).
//   - RED on v4.9.3 as-shipped (fails at the "writeRequests dropped an accepted
//     write" assertion below).
//
// Why: DropPrefix / DropAll both begin with prepareToDrop() -> blockWrite(),
// which sets db.blockWrites = 1 and then DRAINS any already-accepted pending
// writes through writeRequests (its stated job is "so that we don't miss any
// entries"). #2308 rewrote the write-room path so that, once blockWrites == 1,
// a full flushChan makes ensureRoomForWrite return errNoRoom IMMEDIATELY instead
// of waiting for room — and writeRequests now treats errNoRoom as fatal
// (done(err); return). It conflates "blockWrites == 1" with "the DB is closing",
// but blockWrites is also set transiently by every DropPrefix / DropAll.
//
// v4.9.2 instead looped `for err == errNoRoom { sleep(10ms) }` and only exited
// once the push succeeded, so an accepted write was never dropped — it waited.
//
// This test reconstructs that exact condition deterministically: flush goroutine
// stalled (L0 pinned at the stall threshold, no compactors) so flushChan stays
// full, blockWrites set, then a single accepted write drained via the very
// function prepareToDrop calls (writeRequests). On v4.9.3 the write is dropped
// with errNoRoom before any room is freed; on v4.9.2 it waits and lands once we
// unstall L0.
func TestDropPrepareDrainDoesNotDropWrites(t *testing.T) {
	opt := getTestOptions("")
	opt.InMemory = true
	opt.NumCompactors = 0 // Nothing drains L0 on its own; we control it.
	opt.NumLevelZeroTables = 2
	// Stall threshold is set high and reached only by PINNING fake L0 tables (see
	// below), never by the handful of real memtables we flush. This matters for
	// teardown: v4.9.2's addLevel0Table stall loop has no close-escape (that was
	// added by #2308), so if real tables left L0 at/above the threshold with no
	// compactor to drain it, v4.9.2's Close would hang — unrelated to the bug.
	opt.NumLevelZeroTablesStall = 10
	opt.NumMemtables = 2 // flushChan capacity == NumMemtables.
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l0 := db.lc.levels[0]

		// (1) Pin L0 at the stall threshold. The flush goroutine will pull one
		// memtable off flushChan and then block in addLevel0Table, so it stops
		// draining flushChan. (Same pinning pattern as l0_backpressure_test.)
		pinned := make([]*table.Table, 0, opt.NumLevelZeroTablesStall)
		l0.Lock()
		for i := 0; i < opt.NumLevelZeroTablesStall; i++ {
			tab := createEmptyTable(db)
			l0.tables = append(l0.tables, tab)
			l0.addSize(tab)
			pinned = append(pinned, tab)
		}
		l0.Unlock()

		// unstall is idempotent: remove the pinned tables so the (now or later
		// woken) flush goroutine sees L0 below the stall threshold and can drain.
		var once sync.Once
		unstall := func() {
			once.Do(func() {
				l0.Lock()
				if len(l0.tables) >= len(pinned) {
					l0.tables = l0.tables[len(pinned):]
				}
				for _, tab := range pinned {
					l0.subtractSize(tab)
				}
				l0.Unlock()
				_ = decrRefs(pinned)
				// Wake the flush goroutine parked in addLevel0Table so it drains
				// flushChan (v4.9.2 polls numTables and needs no signal; the cond
				// path added by #2308 does).
				l0.signalL0Drained()
			})
		}
		// Guarantee teardown can't hang: unstall L0 and empty the active memtable
		// so runBadgerTest's db.Close() (which force-flushes a non-empty db.mt into
		// flushChan *before* it unstalls the flusher) always has room to drain.
		defer func() {
			unstall()
			db.lock.Lock()
			if db.mt != nil && !db.mt.sl.Empty() {
				db.mt.DecrRef()
				if mt, err := db.newMemTable(); err == nil {
					db.mt = mt
				}
			}
			db.lock.Unlock()
		}()

		fillMemtable := func(mt *memTable) {
			for i := 0; !mt.isFull(); i++ {
				key := y.KeyWithTs([]byte(fmt.Sprintf("pad-%08d", i)), uint64(i+1))
				require.NoError(t, mt.Put(key, y.ValueStruct{Value: make([]byte, 256)}))
			}
		}

		// (2) Fill flushChan and keep it full. We count SUCCESSFUL pushes and stop
		// at cap+1: the (cap+1)-th push can only succeed after the flush goroutine
		// has consumed one memtable and parked in addLevel0Table — which, with L0
		// pinned and no compactors, it never leaves. So flushChan is now full and
		// stays full. We replicate the push half of ensureRoomForWrite under db.lock
		// so the fill behaves identically on v4.9.2 and v4.9.3.
		pushes := 0
		deadline := time.Now().Add(15 * time.Second)
		for pushes < cap(db.flushChan)+1 {
			require.Falsef(t, time.Now().After(deadline),
				"could not fill flushChan (pushes=%d cap=%d)", pushes, cap(db.flushChan))
			db.lock.Lock()
			fillMemtable(db.mt)
			select {
			case db.flushChan <- db.mt:
				db.imm = append(db.imm, db.mt)
				mt, err := db.newMemTable()
				require.NoError(t, err)
				db.mt = mt
				pushes++
			default:
			}
			db.lock.Unlock()
			time.Sleep(time.Millisecond)
		}

		// (3) Make the current memtable full too, so ensureRoomForWrite cannot just
		// write into it and MUST try (and, on v4.9.3, fail) to push to flushChan.
		db.lock.Lock()
		fillMemtable(db.mt)
		db.lock.Unlock()

		// (4) Enter the state prepareToDrop establishes before draining: writes
		// blocked. (We set the flag directly rather than calling DropPrefix so the
		// test keeps full control of timing and doesn't tear the flusher down.)
		db.blockWrites.Store(1)
		defer db.blockWrites.Store(0)

		// (5) Build one already-accepted write, exactly as prepareToDrop's drain
		// hands it to writeRequests.
		const survKey = "survivor-key"
		const survVal = "survivor-val"
		req := requestPool.Get().(*request)
		req.reset()
		req.Entries = []*Entry{{Key: y.KeyWithTs([]byte(survKey), 1), Value: []byte(survVal)}}
		req.Wg.Add(1)
		req.IncrRef()

		// (6) Drain it via the exact function prepareToDrop calls.
		errCh := make(chan error, 1)
		go func() { errCh <- db.writeRequests([]*request{req}) }()

		// (7) Correct behavior (v4.9.2): writeRequests must WAIT for room, not drop
		// the write. If it returns before we free ANY room, it dropped an accepted
		// write — the #2308 regression.
		select {
		case err := <-errCh:
			require.NoErrorf(t, err,
				"writeRequests dropped an accepted write while blockWrites==1 and "+
					"flushChan was full — Badger #2308 regression (DropPrefix/DropAll "+
					"drain loses in-flight writes under load)")
		case <-time.After(750 * time.Millisecond):
			// Still waiting == correct. Free room: unstall L0 so the flusher drains
			// flushChan (mirrors compaction removing L0 tables during restore).
			unstall()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("writeRequests hung after room was freed")
			}
		}

		// (8) The accepted write must be readable (raw get at max ts to avoid
		// oracle-timestamp coupling; the regression is timestamp-independent).
		vs, err := db.get(y.KeyWithTs([]byte(survKey), math.MaxUint64))
		require.NoError(t, err)
		require.Truef(t, vs.Meta&bitDelete == 0 && string(vs.Value) == survVal,
			"survivor key missing/stale after drain: meta=%d value=%q", vs.Meta, string(vs.Value))
	})
}
