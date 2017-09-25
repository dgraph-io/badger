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
	"log"
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
	curReadTs    uint64
	nextCommitTs uint64

	curCommits     uint64Heap
	pendingCommits map[uint64]struct{}

	commits map[uint64]uint64 // Avoid dealing with byte arrays.
}

func (gs *globalTxnState) readTs() uint64 {
	gs.RLock()
	defer gs.RUnlock()
	return gs.curReadTs
}

func (gs *globalTxnState) newCommitTs(txn *Txn) uint64 {
	gs.RLock()
	for _, ro := range txn.reads {
		if ts, has := gs.commits[ro]; has {
			if ts > txn.readTs {
				gs.RUnlock()
				return 0
			}
		}
	}
	gs.RUnlock()

	gs.Lock()
	defer gs.Unlock()

	ts := gs.nextCommitTs
	for _, w := range txn.writes {
		// Update the commitTs.
		gs.commits[w] = ts
	}
	heap.Push(&gs.curCommits, ts)
	_, has := gs.pendingCommits[ts]
	if has {
		log.Fatal("We shouldn't already have the commit ts: %d", ts)
	}
	gs.pendingCommits[ts] = struct{}{}

	gs.nextCommitTs++
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
	for len(gs.curCommits) > 0 {
		ts := gs.curCommits[0]
		if _, has := gs.pendingCommits[ts]; has {
			// Still waiting for a txn to commit.
			break
		}
		min = ts
		heap.Pop(&gs.curCommits)
	}
	if min == 0 {
		return
	}
	gs.curReadTs = min
	gs.nextCommitTs = min + 1
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

	vs, err := txn.kv.get(key)
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

func (txn *Txn) Commit() error {
	cts := txn.gs.newCommitTs(txn)

	var entries []*Entry
	for _, e := range txn.cache {
		entries = append(entries, e)
	}
	err := txn.kv.BatchSet(entries)
	txn.gs.doneCommit(cts)
	return err
}

func (kv *KV) Begin() (*Txn, error) {
	txn := &Txn{
		gs:     kv.txnState,
		kv:     kv,
		readTs: kv.txnState.readTs(),
		cache:  make(map[uint64]*Entry),
	}

	return txn, nil
}
