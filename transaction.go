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
	"bytes"
	"container/heap"
	"encoding/binary"
	"log"
	"math"
	"strconv"
	"sync"

	farm "github.com/dgryski/go-farm"
	"github.com/pkg/errors"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int               { return len(u) }
func (u uint64Heap) Less(i int, j int) bool { return u[i] < u[j] }
func (u uint64Heap) Swap(i int, j int)      { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{})    { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

type globalTxnState struct {
	sync.RWMutex
	curRead    uint64
	nextCommit uint64

	// These two structures are used to figure out when a commit is done. The minimum done commit is
	// used to update curRead.
	commitMark     uint64Heap
	pendingCommits map[uint64]struct{}

	// commits stores a key fingerprint and latest commit counter for it.
	commits map[uint64]uint64
}

func (gs *globalTxnState) readTs() uint64 {
	gs.RLock()
	defer gs.RUnlock()
	return gs.curRead
}

func (gs *globalTxnState) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	gs.RLock()
	defer gs.RUnlock()
	for _, ro := range txn.reads {
		if ts, has := gs.commits[ro]; has && ts > txn.readTs {
			return true
		}
	}
	return false
}

func (gs *globalTxnState) newCommitTs(txn *Txn) uint64 {
	if gs.hasConflict(txn) {
		return 0
	}

	gs.Lock()
	defer gs.Unlock()

	ts := gs.nextCommit
	for _, w := range txn.writes {
		gs.commits[w] = ts // Update the commitTs.
	}
	heap.Push(&gs.commitMark, ts)
	_, has := gs.pendingCommits[ts]
	if has {
		log.Fatal("We shouldn't already have the commit ts: %d", ts)
	}
	gs.pendingCommits[ts] = struct{}{}

	gs.nextCommit++
	return ts
}

func (gs *globalTxnState) doneCommit(ts uint64) {
	gs.Lock()
	defer gs.Unlock()

	_, has := gs.pendingCommits[ts]
	if !has {
		log.Fatal("We should already have the commit ts: %d", ts)
	}
	delete(gs.pendingCommits, ts)

	var min uint64
	for len(gs.commitMark) > 0 {
		ts := gs.commitMark[0]
		if _, has := gs.pendingCommits[ts]; has {
			// Still waiting for a txn to commit.
			break
		}
		min = ts
		heap.Pop(&gs.commitMark)
	}
	if min == 0 {
		return
	}
	gs.curRead = min
	gs.nextCommit = min + 1
}

type Txn struct {
	readTs   uint64
	commitTs uint64

	// The following contain fingerprints of the keys.
	reads  []uint64
	writes []uint64

	cache map[uint64]*Entry

	gs *globalTxnState
	kv *KV
}

func (txn *Txn) Set(key, val []byte, userMeta byte) {
	fp := farm.Fingerprint64(key) // Avoid dealing with byte arrays.
	txn.writes = append(txn.writes, fp)

	e := &Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	}
	txn.cache[fp] = e
}

func (txn *Txn) Delete(key []byte) {
	fp := farm.Fingerprint64(key) // Avoid dealing with byte arrays.
	txn.writes = append(txn.writes, fp)

	e := &Entry{
		Key:  key,
		Meta: BitDelete,
	}
	txn.cache[fp] = e
}

func keyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

func (txn *Txn) Get(key []byte, item *KVItem) error {
	fp := farm.Fingerprint64(key)
	if e, has := txn.cache[fp]; has && bytes.Compare(key, e.Key) == 0 {
		// Fulfill from cache.
		item.val = e.Value
		item.userMeta = e.UserMeta
		item.key = key
		item.status = prefetched
		return nil
	}

	txn.reads = append(txn.reads, fp)

	seek := keyWithTs(key, txn.readTs)
	vs, err := txn.kv.get(seek)
	if err != nil {
		return errors.Wrapf(err, "KV::Get key: %q", key)
	}

	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.casCounter = vs.CASCounter
	item.key = key
	item.kv = txn.kv
	item.vptr = vs.Value
	return nil
}

var errConflict = errors.New("Transaction Conflict. Please retry.")

func (txn *Txn) Commit() error {
	if len(txn.writes) == 0 {
		return nil // Read only transaction.
	}

	cts := txn.gs.newCommitTs(txn)
	if cts == 0 {
		return errConflict
	}
	defer txn.gs.doneCommit(cts)

	var entries []*Entry
	for _, e := range txn.cache {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = keyWithTs(e.Key, txn.commitTs)
		entries = append(entries, e)
	}
	// TODO: Add logic in replay to deal with this.
	entry := &Entry{
		Key:   txnKey,
		Value: []byte(strconv.Itoa(txn.commitTs)),
		Meta:  BitFinTxn,
	}
	entries = append(entries, entry)
	return txn.kv.BatchSet(entries)
}

func (kv *KV) NewTransaction() (*Txn, error) {
	txn := &Txn{
		gs:     kv.txnState,
		kv:     kv,
		readTs: kv.txnState.readTs(),
		cache:  make(map[uint64]*Entry),
	}

	return txn, nil
}
