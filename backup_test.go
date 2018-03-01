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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.Value()
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
	for i := byte(0); i < N; i++ {
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

	for i := byte(0); i < N; i++ {
		err = db2.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.Value()
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

	for i := byte(0); i < N; i++ {
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

	for i := byte(0); i < N; i++ {
		err = db3.View(func(tx *Txn) error {
			k := append(key1, i)
			item, err := tx.Get(k)
			if err != nil {
				if err == ErrKeyNotFound {
					return fmt.Errorf("Key %q has been not found, but was set\n", k)
				}
				return err
			}
			v, err := item.Value()
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
