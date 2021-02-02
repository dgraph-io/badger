// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"bytes"
	"log"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v3"
)

// Adapters for Badger.
type badgerDB struct {
	db *badger.DB
}

func newBadgerDB(dir string) DB {
	db, err := badger.Open(badger.DefaultOptions(dir).WithBlockCacheSize(cacheSize))
	if err != nil {
		log.Fatal(err)
	}
	return &badgerDB{db}
}

func (b badgerDB) NewIter(opts *pebble.IterOptions) iterator {
	txn := b.db.NewTransaction(false)
	iopts := badger.DefaultIteratorOptions
	iopts.PrefetchValues = false
	iter := txn.NewIterator(iopts)
	return &badgerIterator{
		txn:   txn,
		iter:  iter,
		lower: opts.GetLowerBound(),
		upper: opts.GetUpperBound(),
	}
}

func (b badgerDB) NewBatch() batch {
	txn := b.db.NewTransaction(true)
	return &badgerBatch{txn}
}

func (b badgerDB) Scan(iter iterator, key []byte, count int64, reverse bool) error {
	var entriesRead uint64
	var sizeRead uint64
        txn := b.db.NewTransaction(false)
	if reverse {
	   badger.DefaultIteratorOptions.Reverse = true
	}
	it := txn.NewIterator(badger.DefaultIteratorOptions)
        for it.Rewind(); it.Valid(); it.Next() {
		i := it.Item()
		atomic.AddUint64(&entriesRead, 1)
		atomic.AddUint64(&sizeRead, uint64(i.EstimatedSize()))
	}
	return nil
}

func (b badgerDB) Metrics() *pebble.Metrics {
	return &pebble.Metrics{}
}

func (b badgerDB) Flush() error {
	return nil
}

type badgerIterator struct {
	txn   *badger.Txn
	iter  *badger.Iterator
	buf   []byte
	lower []byte
	upper []byte
}

func (i *badgerIterator) SeekLT(key []byte) bool {
	i.iter.Seek(key)
	if !i.iter.Valid() {
		return false
	}
	if i.lower != nil && bytes.Compare(i.Key(), i.lower) >= 0 {
		return false
	}
	return true
}

func (i *badgerIterator) SeekGE(key []byte) bool {
	i.iter.Seek(key)
	if !i.iter.Valid() {
		return false
	}
	if i.upper != nil && bytes.Compare(i.Key(), i.upper) >= 0 {
		return false
	}
	return true
}

func (i *badgerIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *badgerIterator) Key() []byte {
	return i.iter.Item().Key()
}

func (i *badgerIterator) Value() []byte {
	var err error
	i.buf, err = i.iter.Item().ValueCopy(i.buf[:0])
	if err != nil {
		log.Fatal(err)
	}
	return i.buf
}

func (i *badgerIterator) First() bool {
	return i.SeekGE(i.lower)
}

func (i *badgerIterator) Next() bool {
	i.iter.Next()
	if !i.iter.Valid() {
		return false
	}
	if i.upper != nil && bytes.Compare(i.Key(), i.upper) >= 0 {
		return false
	}
	return true
}

func (i *badgerIterator) Last() bool {
	return false
}

func (i *badgerIterator) Prev() bool {
	return false
}

func (i *badgerIterator) Close() error {
	i.iter.Close()
	i.txn.Discard()
	return nil
}

type badgerBatch struct {
	txn *badger.Txn
}

func (b badgerBatch) Close() error {
	return nil
}

func (b badgerBatch) Commit(opts *pebble.WriteOptions) error {
	return b.txn.Commit()
}

func (b badgerBatch) Set(key, value []byte, _ *pebble.WriteOptions) error {
	return b.txn.Set(key, value)
}

func (b badgerBatch) Delete(key []byte, _ *pebble.WriteOptions) error {
	return b.txn.Delete(key)
}

func (b badgerBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	return fmt.Errorf("badgerBatch.logData: unimplemented")
}
