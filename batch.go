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
	"fmt"
	"sync"
)

type WriteBatch struct {
	sync.Mutex
	txn *Txn
	db  *DB
	wg  sync.WaitGroup
	err error
}

// NewWriteBatch creates a new WriteBatch. This provides a way to conveniently do a lot of writes,
// batching them up as tightly as possible in a single transaction and using callbacks to avoid
// waiting for them to commit, thus achieving good performance. This API hides away the logic of
// creating and committing transactions. Due to the nature of SSI guaratees provided by Badger,
// blind writes can never encounter transaction conflicts (ErrConflict).
func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{db: db, txn: db.NewTransaction(true)}
}

func (wb *WriteBatch) callback(err error) {
	// sync.WaitGroup is thread-safe, so it doesn't need to be run inside wb.Lock.
	defer wb.wg.Done()
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

// Set is equivalent of Txn.SetWithMeta.
func (wb *WriteBatch) Set(k, v []byte, meta byte) error {
	wb.Lock()
	defer wb.Unlock()

	err := wb.txn.SetWithMeta(k, v, meta)
	if err == ErrTxnTooBig {
		return wb.commit()
	}
	return err
}

// Delete is equivalent of Txn.Delete.
func (wb *WriteBatch) Delete(k []byte) error {
	wb.Lock()
	defer wb.Unlock()

	err := wb.txn.Delete(k)
	if err == ErrTxnTooBig {
		return wb.commit()
	}
	return err
}

// This must hold a lock.
func (wb *WriteBatch) commit() error {
	fmt.Println("Doing a commit")
	if wb.err != nil {
		fmt.Printf("Got error: %v\n", wb.err)
		return wb.err
	}
	// Get a new txn before we commit this one. So, the new txn doesn't need
	// to wait for this one to commit.
	newTxn := wb.db.NewTransaction(true)
	fmt.Println("newtxn")
	wb.wg.Add(1)
	wb.txn.CommitWith(wb.callback)
	wb.txn = newTxn
	return wb.err
}

// Flush must be called at the end to ensure that any pending writes get committed to Badger. Flush
// returns any error stored by WriteBatch.
func (wb *WriteBatch) Flush() error {
	wb.Lock()
	_ = wb.commit()
	wb.txn.Discard()
	wb.Unlock()

	fmt.Println("waiting for wait")
	wb.wg.Wait()
	// Safe to access error without any synchronization here.
	return wb.err
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
	wb.Lock()
	defer wb.Unlock()
	return wb.err
}
