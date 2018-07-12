/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

// ManagedDB allows end users to manage the transactions themselves. Transaction
// start and commit timestamps are set by end-user.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
//
// WARNING: This is an experimental feature and may be changed significantly in
// a future release. So please proceed with caution.
type ManagedDB struct {
	*DB
}

// OpenManaged returns a new ManagedDB, which allows more control over setting
// transaction timestamps.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func OpenManaged(opts Options) (*ManagedDB, error) {
	opts.managedTxns = true
	db, err := Open(opts)
	if err != nil {
		return nil, err
	}
	return &ManagedDB{db}, nil
}

// NewTransaction overrides DB.NewTransaction() and panics when invoked. Use
// NewTransactionAt() instead.
func (db *ManagedDB) NewTransaction(update bool) {
	panic("Cannot use NewTransaction() for ManagedDB. Use NewTransactionAt() instead.")
}

// NewTransactionAt follows the same logic as DB.NewTransaction(), but uses the
// provided read timestamp.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func (db *ManagedDB) NewTransactionAt(readTs uint64, update bool) *Txn {
	txn := db.DB.NewTransaction(update)
	txn.readTs = readTs
	return txn
}

// CommitAt commits the transaction, following the same logic as Commit(), but
// at the given commit timestamp. This will panic if not used with ManagedDB.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func (txn *Txn) CommitAt(commitTs uint64, callback func(error)) error {
	if !txn.db.opt.managedTxns {
		return ErrManagedTxn
	}
	txn.commitTs = commitTs
	return txn.Commit(callback)
}

// GetSequence is not supported on ManagedDB. Calling this would result
// in a panic.
func (db *ManagedDB) GetSequence(_ []byte, _ uint64) (*Sequence, error) {
	panic("Cannot use GetSequence for ManagedDB.")
}

// SetDiscardTs sets a timestamp at or below which, any invalid or deleted
// versions can be discarded from the LSM tree, and thence from the value log to
// reclaim disk space.
func (db *ManagedDB) SetDiscardTs(ts uint64) {
	db.orc.setDiscardTs(ts)
}

var errDone = errors.New("Done deleting keys")

// DropAll would drop all the data stored in Badger. It does this in the following way.
// - Stop accepting new writes.
// - Flush out all memtables.
// - Push one update, and flush memtables again.
// - Pause the compactions.
// - Pick all tables from all levels, create a changeset to delete all these tables and apply it to
// manifest. DO not pick up the latest table from level 0, to preserve the (persistent) badgerHead key.
// - Iterate over DB, we should have zero KVs.
// - TODO: Update logic to use flushChan.
// - Iterate over the KVs in Level 0, and run deletes on them via transactions.
//
// NOTE: The timestamp used for writes must be greater than the max timestamp of
// writes before DropAll, to ensure that new writes are not lower than the
// delete markers in terms of versioning. If lower, it would result in new
// writes being seen as absent.
func (db *ManagedDB) DropAll() error {
	// Stop accepting new writes.
	atomic.StoreInt32(&db.blockWrites, 1)

	// Wait for writeCh to reach size of zero. This is not ideal, but a very
	// simple way to allow writeCh to flush out, before we proceed.
	tick := time.NewTicker(100 * time.Millisecond)
	for range tick.C {
		if len(db.writeCh) == 0 {
			break
		}
	}
	tick.Stop()

	// Stop the compactions.
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
	}

	_, err := db.lc.deleteLSMTree()
	// Allow writes so that we can run transactions. Ideally, the user must ensure that they're not
	// doing more writes concurrently while this operation is happening.
	atomic.StoreInt32(&db.blockWrites, 0)
	// Need compactions to happen so deletes below can be flushed out.
	if db.closers.compactors != nil {
		db.closers.compactors = y.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)
	}
	if err != nil {
		return err
	}

	deleteKeys := func() error {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		defer txn.Discard()

		opts := DefaultIteratorOptions
		opts.PrefetchValues = false
		itr := txn.NewIterator(opts)

		var maxTs uint64
		var count int
		for itr.Rewind(); itr.Valid(); itr.Next() {
			count++
			item := itr.Item()
			if item.Version() > maxTs {
				maxTs = item.Version()
			}
			err := txn.Delete(item.KeyCopy(nil))
			if err == ErrTxnTooBig {
				break
			} else if err != nil {
				itr.Close()
				return err
			}
		}
		itr.Close()
		if count == 0 {
			return errDone
		}
		return txn.CommitAt(maxTs, nil)
	}

	// Continue deleting until all data has been marked as deleted.
	for {
		err := deleteKeys()
		if err == errDone {
			return nil
		}
		if err != nil {
			return err
		}
		// Otherwise, continue.
	}
	return nil
}
