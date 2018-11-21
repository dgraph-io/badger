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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
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
		db1.Close()
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

		db1.Close()
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

func TestDumpLoad(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)

	// Write some stuff
	entries := []struct {
		key      []byte
		val      []byte
		userMeta byte
		version  uint64
	}{
		{key: []byte("answer1"), val: []byte("42"), version: 1},
		{key: []byte("answer2"), val: []byte("43"), userMeta: 1, version: 2},
	}

	err = db.Update(func(txn *Txn) error {
		e := entries[0]
		err := txn.SetWithMeta(e.key, e.val, e.userMeta)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(txn *Txn) error {
		e := entries[1]
		err := txn.SetWithMeta(e.key, e.val, e.userMeta)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Use different directory.
	dir, err = ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	bak, err := ioutil.TempFile(dir, "badgerbak")
	require.NoError(t, err)
	ts, err := db.Backup(bak, 0)
	t.Logf("New ts: %d\n", ts)
	require.NoError(t, err)
	require.NoError(t, bak.Close())
	require.NoError(t, db.Close())

	db, err = Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	bak, err = os.Open(bak.Name())
	require.NoError(t, err)
	defer bak.Close()

	require.NoError(t, db.Load(bak))

	err = db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, entries[count].key, item.Key())
			require.Equal(t, entries[count].val, val)
			require.Equal(t, entries[count].version, item.Version())
			require.Equal(t, entries[count].userMeta, item.UserMeta())
			count++
		}
		require.Equal(t, count, 2)
		return nil
	})
	require.NoError(t, err)
}

func Test_BackupRestore(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.RemoveAll(tmpdir)
	}()

	s1Path := filepath.Join(tmpdir, "test1")
	s2Path := filepath.Join(tmpdir, "test2")
	s3Path := filepath.Join(tmpdir, "test3")

	opts := DefaultOptions
	opts.Dir = s1Path
	opts.ValueDir = s1Path
	db1, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	key1 := []byte("key1")
	key2 := []byte("key2")
	rawValue := []byte("NotLongValue")
	N := byte(251)
	err = db1.Update(func(tx *Txn) error {
		if err := tx.Set(key1, rawValue); err != nil {
			return err
		}
		return tx.Set(key2, rawValue)
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i < N; i++ {
		err = db1.Update(func(tx *Txn) error {
			if err := tx.Set(append(key1, i), rawValue); err != nil {
				return err
			}
			return tx.Set(append(key2, i), rawValue)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var backup bytes.Buffer
	_, err = db1.Backup(&backup, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("backup1 length:", backup.Len())

	opts = DefaultOptions
	opts.Dir = s2Path
	opts.ValueDir = s2Path
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	err = db2.Load(&backup)
	if err != nil {
		t.Fatal(err)
	}

	for i := byte(1); i < N; i++ {
		err = db2.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(v, rawValue) {
				return fmt.Errorf("Values not match, got %v, expected %v", v, rawValue)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := byte(1); i < N; i++ {
		err = db2.Update(func(tx *Txn) error {
			if err := tx.Set(append(key1, i), rawValue); err != nil {
				return err
			}
			return tx.Set(append(key2, i), rawValue)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	backup.Reset()
	_, err = db2.Backup(&backup, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("backup2 length:", backup.Len())
	opts = DefaultOptions
	opts.Dir = s3Path
	opts.ValueDir = s3Path
	db3, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	err = db3.Load(&backup)
	if err != nil {
		t.Fatal(err)
	}

	for i := byte(1); i < N; i++ {
		err = db3.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(v, rawValue) {
				return fmt.Errorf("Values not match, got %v, expected %v", v, rawValue)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

}
