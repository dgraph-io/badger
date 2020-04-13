/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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
	"sync"

	"github.com/dgraph-io/badger/v2/y"
)

// WriteBatch holds the necessary info to perform batched writes.
type WriteBatch struct {
	sync.Mutex
	txn      *Txn
	db       *DB
	throttle *y.Throttle
	err      error
	entries  []*Entry // Used when badger is running in managed mode.
}

// NewWriteBatch creates a new WriteBatch. This provides a way to conveniently do a lot of writes,
// batching them up as tightly as possible in a single transaction and using callbacks to avoid
// waiting for them to commit, thus achieving good performance. This API hides away the logic of
// creating and committing transactions. Due to the nature of SSI guaratees provided by Badger,
// blind writes can never encounter transaction conflicts (ErrConflict).
func (db *DB) NewWriteBatch() *WriteBatch {
	if db.opt.managedTxns {
		panic("cannot use NewWriteBatch in managed mode. Use NewWriteBatchAt instead")
	}
	return db.newWriteBatch()
}

func (db *DB) newWriteBatch() *WriteBatch {
	return &WriteBatch{
		db:       db,
		txn:      db.newTransaction(true, true),
		throttle: y.NewThrottle(16),
	}
}

// SetMaxPendingTxns sets a limit on maximum number of pending transactions while writing batches.
// This function should be called before using WriteBatch. Default value of MaxPendingTxns is
// 16 to minimise memory usage.
func (wb *WriteBatch) SetMaxPendingTxns(max int) {
	wb.throttle = y.NewThrottle(max)
}

// Cancel function must be called if there's a chance that Flush might not get
// called. If neither Flush or Cancel is called, the transaction oracle would
// never get a chance to clear out the row commit timestamp map, thus causing an
// unbounded memory consumption. Typically, you can call Cancel as a defer
// statement right after NewWriteBatch is called.
//
// Note that any committed writes would still go through despite calling Cancel.
func (wb *WriteBatch) Cancel() {
	if err := wb.throttle.Finish(); err != nil {
		wb.db.opt.Errorf("WatchBatch.Cancel error while finishing: %v", err)
	}
	wb.txn.Discard()
}

func (wb *WriteBatch) callback(err error) {
	// sync.WaitGroup is thread-safe, so it doesn't need to be run inside wb.Lock.
	defer wb.throttle.Done(err)
	if err == nil {
		return
	}

	wb.Lock()
	defer wb.Unlock()
	if wb.err != nil {
		return
	}
	wb.err = err
}

// SetEntry is the equivalent of Txn.SetEntry.
func (wb *WriteBatch) SetEntry(e *Entry) error {
	wb.Lock()
	defer wb.Unlock()
	err := wb.txn.SetEntry(e)
	switch err {
	case nil:
		if wb.db.opt.managedTxns {
			wb.entries = append(wb.entries, e)
		}
		return nil
	case ErrTxnTooBig:
		// Don't do anything.
	default:
		return err
	}
	// Txn has reached it's zenith. Commit now.
	if cerr := wb.commit(); cerr != nil {
		return cerr
	}
	// This time the error must not be ErrTxnTooBig, otherwise, we make the
	// error permanent.
	if err := wb.txn.SetEntry(e); err != nil {
		wb.err = err
		return err
	}
	wb.entries = append(wb.entries, e)
	return nil
}

// Set is equivalent of Txn.Set().
func (wb *WriteBatch) Set(k, v []byte) error {
	e := &Entry{Key: k, Value: v}
	return wb.SetEntry(e)
}

// Delete is equivalent of Txn.Delete.
func (wb *WriteBatch) Delete(k []byte) error {
	wb.Lock()
	defer wb.Unlock()

	err := wb.txn.Delete(k)
	switch err {
	case nil:
		if wb.db.opt.managedTxns {
			e := &Entry{
				Key:  k,
				meta: bitDelete,
			}
			wb.entries = append(wb.entries, e)
		}
		return nil
	case ErrTxnTooBig:
		// Don't do anything.
	default:
		return err
	}
	if err := wb.commit(); err != nil {
		return err
	}
	if err := wb.txn.Delete(k); err != nil {
		wb.err = err
		return err
	}
	wb.entries = append(wb.entries, &Entry{
		Key:  k,
		meta: bitDelete,
	})
	return nil
}

// Caller to commit must hold a write lock.
func (wb *WriteBatch) commit() error {
	if wb.err != nil {
		return wb.err
	}
	if err := wb.throttle.Do(); err != nil {
		return err
	}
	if wb.db.opt.managedTxns {
		wb.err = wb.db.batchSetAsync(wb.entries, func(err error) {
			defer wb.throttle.Done(err)
			if err != nil {
				wb.err = err
				return
			}
		})
		wb.entries = make([]*Entry, 0, 1000)
		wb.txn.Discard()
		wb.txn = wb.db.newTransaction(true, true)
		return wb.err
	}
	wb.txn.CommitWith(wb.callback)
	wb.txn = wb.db.newTransaction(true, true)
	wb.txn.readTs = 0 // We're not reading anything.
	return wb.err
}

// Flush must be called at the end to ensure that any pending writes get committed to Badger. Flush
// returns any error stored by WriteBatch.
func (wb *WriteBatch) Flush() error {
	wb.Lock()
	_ = wb.commit()
	wb.txn.Discard()
	wb.Unlock()

	if err := wb.throttle.Finish(); err != nil {
		return err
	}

	return wb.err
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
	wb.Lock()
	defer wb.Unlock()
	return wb.err
}
