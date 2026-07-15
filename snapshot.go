/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// snapshotList is a doubly-linked list of active Snapshots, inspired by
// Pebble's snapshotList and LevelDB's snapshot registry. It allows the DB
// to track all open snapshots so that metrics, lifecycle management, and
// future GC optimisation have an authoritative view of what read timestamps
// are pinned.
//
// All mutations must hold snapshotList.mu.
type snapshotList struct {
	mu         sync.Mutex
	root       snapshotEntry
	count      atomic.Int64
	generation atomic.Uint64
}

type snapshotEntry struct {
	prev, next *snapshotEntry
	s          *Snapshot // nil for the sentinel root
}

func (l *snapshotList) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *snapshotList) pushBack(s *Snapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e := &s.entry
	e.s = s
	e.prev = l.root.prev
	e.next = &l.root
	l.root.prev.next = e
	l.root.prev = e
	l.count.Add(1)
	l.generation.Add(1)
}

func (l *snapshotList) remove(s *Snapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e := &s.entry
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
	e.s = nil
	l.count.Add(-1)
	l.generation.Add(1)
}

// minReadTs returns the lowest readTs held by any open Snapshot, or 0 if
// there are no open snapshots. The compactor can use this to avoid discarding
// MVCC versions that an active snapshot still needs.
func (l *snapshotList) minReadTs() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	var min uint64
	for e := l.root.next; e != &l.root; e = e.next {
		ts := e.s.readTs
		if min == 0 || ts < min {
			min = ts
		}
	}
	return min
}

// Snapshot provides a consistent, read-only, point-in-time view of the
// database. All reads through a Snapshot observe the same database state,
// regardless of concurrent writes that may occur after the Snapshot was
// created.
//
// A Snapshot prevents garbage collection of MVCC versions at or above its
// read timestamp. Callers must call Close when the Snapshot is no longer
// needed so that old versions can be reclaimed.
//
// The design follows patterns established by Pebble (CockroachDB) and
// LevelDB: each Snapshot is registered in a doubly-linked list on the DB,
// enabling efficient min-readTs queries for GC, and leak detection via
// runtime.SetFinalizer.
//
// Snapshot must not be used with managed-mode databases; use
// NewTransactionAt instead.
type Snapshot struct {
	db     *DB
	readTs uint64
	guard  *Txn // holds readMark.Begin; released on Close
	entry  snapshotEntry

	mu     sync.Mutex
	closed bool
}

// NewSnapshot creates a point-in-time Snapshot of the database.
//
// Every open Snapshot pins its read timestamp in the MVCC watermark,
// preventing garbage collection from advancing past that point.
// Always call Close when the Snapshot is no longer needed.
//
// Panics if the database is opened in managed-transaction mode.
func (db *DB) NewSnapshot() *Snapshot {
	if db.opt.managedTxns {
		panic("Cannot use NewSnapshot with managed transactions. Use NewTransactionAt instead.")
	}
	guard := db.NewTransaction(false)
	s := &Snapshot{
		db:     db,
		readTs: guard.ReadTs(),
		guard:  guard,
	}
	db.snapshots.pushBack(s)

	runtime.SetFinalizer(s, func(s *Snapshot) {
		if !s.closed {
			s.db.opt.Warningf("Snapshot at readTs=%d was not closed; leaking readMark", s.readTs)
			s.Close()
		}
	})

	return s
}

// NumActiveSnapshots returns the number of currently open snapshots.
func (db *DB) NumActiveSnapshots() int64 {
	return db.snapshots.count.Load()
}

// ReadTs returns the read timestamp that this Snapshot is pinned to.
func (s *Snapshot) ReadTs() uint64 {
	return s.readTs
}

// NewTransaction creates a read-only transaction that observes the
// database at this Snapshot's read timestamp. Multiple concurrent
// transactions may be created from the same Snapshot.
//
// The caller must call Discard on the returned Txn when done.
func (s *Snapshot) NewTransaction() *Txn {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		panic("Cannot create transaction from a closed Snapshot.")
	}
	s.mu.Unlock()

	txn := s.db.newTransaction(false, true)
	txn.readTs = s.readTs
	txn.doneRead = true
	return txn
}

// NewIterator is a convenience that creates an Iterator over the Snapshot.
//
// The caller must call Close on the returned Iterator, then Discard on
// the underlying transaction.
func (s *Snapshot) NewIterator(opts IteratorOptions) *Iterator {
	txn := s.NewTransaction()
	return txn.NewIterator(opts)
}

// Get looks up a key in the Snapshot.
func (s *Snapshot) Get(key []byte) (item *Item, rerr error) {
	txn := s.NewTransaction()
	defer txn.Discard()
	return txn.Get(key)
}

// Close releases the Snapshot's read-mark, allowing the garbage collector
// to reclaim MVCC versions that were being protected. Close removes the
// Snapshot from the DB's registry.
//
// Close is idempotent and safe to call concurrently.
func (s *Snapshot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	runtime.SetFinalizer(s, nil)
	s.db.snapshots.remove(s)
	s.guard.Discard()
}
