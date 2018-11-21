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
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/protos"
	"github.com/stretchr/testify/require"
)

var randSrc = rand.NewSource(time.Now().UnixNano())

func createEntries(n int) []*protos.KVPair {
	entries := make([]*protos.KVPair, n)
	for i := 0; i < n; i++ {
		entries[i] = &protos.KVPair{
			Key:      []byte(fmt.Sprint("key", i)),
			Value:    []byte{1},
			UserMeta: []byte{0},
			Meta:     []byte{0},
		}
	}
	return entries
}

func populateEntries(db *DB, entries []*protos.KVPair) error {
	return db.Update(func(txn *Txn) error {
		var err error
		for i, e := range entries {
			if err = txn.Set(e.Key, e.Value); err != nil {
				return err
			}
			entries[i].Version = 1
		}
		return nil
	})
}

func TestBackup(t *testing.T) {
	var bb bytes.Buffer

	tmpdir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	opts := DefaultOptions
	opts.Dir = filepath.Join(tmpdir, "backup0")
	opts.ValueDir = opts.Dir
	db1, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	N := 1000
	entries := createEntries(N)
	require.NoError(t, populateEntries(db1, entries))

	_, err = db1.Backup(&bb, 0)
	require.NoError(t, err)

	err = db1.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			idx, err := strconv.Atoi(string(item.Key())[3:])
			if err != nil {
				return err
			}
			if idx > N || !bytes.Equal(entries[idx].Key, item.Key()) {
				return fmt.Errorf("%s: %s", string(item.Key()), ErrKeyNotFound)
			}
			count++
		}
		if N != count {
			return fmt.Errorf("wrong number of items: %d expected, %d actual", N, count)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBackupLoad(t *testing.T) {
	var bb bytes.Buffer

	tmpdir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	opts := DefaultOptions
	N := 1000
	entries := createEntries(N)

	// backup
	{
		opts.Dir = filepath.Join(tmpdir, "backup1")
		opts.ValueDir = opts.Dir
		db1, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}

		require.NoError(t, populateEntries(db1, entries))

		_, err = db1.Backup(&bb, 0)
		require.NoError(t, err)
		require.NoError(t, db1.Close())
	}
	require.True(t, len(entries) == N)
	require.True(t, bb.Len() > 0)

	// restore
	opts.Dir = filepath.Join(tmpdir, "restore1")
	opts.ValueDir = opts.Dir
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, db2.Load(&bb))

	// verify
	err = db2.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			idx, err := strconv.Atoi(string(item.Key())[3:])
			if err != nil {
				return err
			}
			if idx > N || !bytes.Equal(entries[idx].Key, item.Key()) {
				return fmt.Errorf("%s: %s", string(item.Key()), ErrKeyNotFound)
			}
			count++
		}
		if N != count {
			return fmt.Errorf("wrong number of items: %d expected, %d actual", N, count)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBackupLoadIncremental(t *testing.T) {
	var bb bytes.Buffer

	tmpdir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	opts := DefaultOptions
	N := 100
	entries := createEntries(N)
	updates := make(map[int]byte)

	// backup
	{
		var since uint64

		opts.Dir = filepath.Join(tmpdir, "backup2")
		opts.ValueDir = opts.Dir
		db1, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}

		require.NoError(t, populateEntries(db1, entries))
		since, err = db1.Backup(&bb, 0)
		require.NoError(t, err)

		ints := rand.New(randSrc).Perm(N)

		// pick 10 items to mark as deleted.
		err = db1.Update(func(txn *Txn) error {
			var err error
			for _, i := range ints[:10] {
				err = txn.Delete(entries[i].Key)
				if err != nil {
					return err
				}
				updates[i] = bitDelete
			}
			return nil
		})
		require.NoError(t, err)
		since, err = db1.Backup(&bb, since)
		require.NoError(t, err)

		// pick 5 items to mark as expired.
		err = db1.Update(func(txn *Txn) error {
			var err error
			for _, i := range (ints)[10:15] {
				err = txn.SetWithTTL(entries[i].Key, entries[i].Value, -time.Hour)
				if err != nil {
					return err
				}
				updates[i] = bitDelete // expired
			}
			return nil
		})
		require.NoError(t, err)
		since, err = db1.Backup(&bb, since)
		require.NoError(t, err)

		// pick 5 items to mark as discard.
		err = db1.Update(func(txn *Txn) error {
			var err error
			for _, i := range ints[15:20] {
				err = txn.SetWithDiscard(entries[i].Key, entries[i].Value, 0)
				if err != nil {
					return err
				}
				updates[i] = bitDiscardEarlierVersions
			}
			return nil
		})
		require.NoError(t, err)
		since, err = db1.Backup(&bb, since)
		require.NoError(t, err)
		require.NoError(t, db1.Close())
	}
	require.True(t, len(entries) == N)
	require.True(t, bb.Len() > 0)

	// restore
	opts.Dir = filepath.Join(tmpdir, "restore2")
	opts.ValueDir = opts.Dir
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, db2.Load(&bb))

	// verify
	actual := make(map[int]byte)
	err = db2.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			idx, err := strconv.Atoi(string(item.Key())[3:])
			if err != nil {
				return err
			}
			if item.IsDeletedOrExpired() {
				_, ok := updates[idx]
				if !ok {
					return fmt.Errorf("%s: not expected to be updated but it is",
						string(item.Key()))
				}
				actual[idx] = item.meta
				count++
				continue
			}
		}
		if len(updates) != count {
			return fmt.Errorf("mismatched updated items: %d expected, %d actual",
				len(updates), count)
		}
		return nil
	})
	require.NoError(t, err, "%v %v", updates, actual)
}
