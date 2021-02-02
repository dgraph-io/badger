// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package replay implements facilities for replaying writes to a database.
package replay

import (
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v3/internal/manifest"
	"github.com/dgraph-io/badger/v3/internal/private"
)

// Table describes a sstable from the reference database whose writes are
// being replayed.
type Table struct {
	Path         string
	FileMetadata *manifest.FileMetadata
}

type bySeqNum []Table

func (s bySeqNum) Len() int      { return len(s) }
func (s bySeqNum) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s bySeqNum) Less(i, j int) bool {
	return s[i].FileMetadata.SmallestSeqNum < s[j].FileMetadata.SmallestSeqNum
}

// Open opens a database for replaying flushed and ingested tables. It's
// intended for use by the `pebble bench compact` command.
func Open(dirname string, opts *pebble.Options) (*DB, error) {
	d, err := pebble.Open(dirname, opts)
	if err != nil {
		return nil, err
	}
	return &DB{d: d}, nil
}

// A DB is a wrapper around a Pebble database for replaying table-level
// writes to a database. It is not safe for concurrent access.
type DB struct {
	done bool
	d    *pebble.DB
}

// Ingest ingests a set of sstables into the DB.
func (d *DB) Ingest(tables []Table) error {
	if d.done {
		panic("replay already finished")
	}
	if len(tables) == 0 {
		return nil
	}

	// Sort the tables by their assigned sequence numbers so that we pass them
	// to Ingest in the same order and ultimately assign sequence numbers in
	// the same order.
	sort.Sort(bySeqNum(tables))

	// The replay database's current allocated sequence number might be less
	// than it was at the time of this ingest in the reference database
	// because only overlapping memtables are flushed before an ingest.
	// We need the ingested files to adopt the same sequence numbers as they
	// originally did to ensure the sequence number invariants still hold.
	// To accomodate this, we bump the sequence number up to just below the
	// sequence number these files' were originally ingested at.
	smallest := tables[0].FileMetadata.SmallestSeqNum
	private.RatchetSeqNum(d.d, smallest)

	paths := make([]string, len(tables))
	for i, tbl := range tables {
		paths[i] = tbl.Path
	}
	return d.d.Ingest(paths)
}

// FlushExternal simulates a flush of the table, linking it directly
// into level zero.
func (d *DB) FlushExternal(tbl Table) error {
	if d.done {
		panic("replay already finished")
	}
	return private.FlushExternalTable(d.d, tbl.Path, tbl.FileMetadata)
}

// Metrics returns the underlying DB's Metrics.
func (d *DB) Metrics() *pebble.Metrics {
	if d.done {
		panic("replay already finished")
	}
	return d.d.Metrics()
}

// Done finishes a replay, returning the underlying database.  All of the
// *replay.DB's methods except Close will error if called after Done.
func (d *DB) Done() *pebble.DB {
	if d.done {
		panic("replay already finished")
	}
	d.done = true
	return d.d
}

// Close closes a replay and the underlying database.
// If Close is called after Done, Close does nothing.
func (d *DB) Close() error {
	if d.done {
		// Allow clients to defer Close()
		return nil
	}
	d.done = true
	return d.d.Close()
}
