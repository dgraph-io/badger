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
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxnSimple(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	txn := kv.NewTransaction(true)

	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key=%d", i))
		v := []byte(fmt.Sprintf("val=%d", i))
		txn.Set(k, v, 0)
	}

	item, err := txn.Get([]byte("key=8"))
	require.NoError(t, err)
	fn := func(val []byte) error {
		require.Equal(t, []byte("val=8"), val)
		return nil
	}
	require.NoError(t, item.Value(fn))
	require.NoError(t, txn.Commit(nil))
}

func TestTxnVersions(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	k := []byte("key")
	for i := 1; i < 10; i++ {
		txn := kv.NewTransaction(true)

		txn.Set(k, []byte(fmt.Sprintf("valversion=%d", i)), 0)
		require.NoError(t, txn.Commit(nil))
		require.Equal(t, uint64(i), kv.txnState.readTs())
	}

	checkIterator := func(itr *Iterator, i int) {
		count := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			require.Equal(t, k, item.Key())

			err := item.Value(func(val []byte) error {
				exp := fmt.Sprintf("valversion=%d", i)
				require.Equal(t, exp, string(val), "i=%d", i)
				count++
				return nil
			})
			require.NoError(t, err)
		}
		require.Equal(t, 1, count, "i=%d", i) // Should only loop once.
	}

	for i := 1; i < 10; i++ {
		txn := kv.NewTransaction(true)
		require.NoError(t, err)
		txn.readTs = uint64(i) // Read version at i.

		item, err := txn.Get(k)
		require.NoError(t, err)

		var found bool
		err = item.Value(func(val []byte) error {
			found = true
			require.Equal(t, []byte(fmt.Sprintf("valversion=%d", i)), val,
				"Expected versions to match up at i=%d", i)
			return nil
		})
		require.NoError(t, err)
		require.True(t, found)

		itr := txn.NewIterator(DefaultIteratorOptions)
		checkIterator(itr, i)

		opt := DefaultIteratorOptions
		opt.Reverse = true
		itr = txn.NewIterator(opt)
		checkIterator(itr, i)
	}
	txn := kv.NewTransaction(true)
	require.NoError(t, err)
	item, err := txn.Get(k)
	require.NoError(t, err)

	item.Value(func(val []byte) error {
		require.Equal(t, []byte("valversion=9"), val)
		return nil
	})
}

func TestTxnWriteSkew(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	// Accounts
	ax := []byte("x")
	ay := []byte("y")

	// Set balance to $100 in each account.
	txn := kv.NewTransaction(true)
	val := []byte(strconv.Itoa(100))
	txn.Set(ax, val, 0)
	txn.Set(ay, val, 0)
	require.NoError(t, txn.Commit(nil))
	require.Equal(t, uint64(1), kv.txnState.readTs())

	getBal := func(txn *Txn, key []byte) (bal int) {
		item, err := txn.Get(key)
		require.NoError(t, err)

		err = item.Value(func(val []byte) error {
			var err error
			bal, err = strconv.Atoi(string(val))
			return err
		})
		require.NoError(t, err)
		return bal
	}

	// Start two transactions, each would read both accounts and deduct from one account.
	txn1 := kv.NewTransaction(true)

	sum := getBal(txn1, ax)
	sum += getBal(txn1, ay)
	require.Equal(t, 200, sum)
	txn1.Set(ax, []byte("0"), 0) // Deduct 100 from ax.

	// Let's read this back.
	sum = getBal(txn1, ax)
	require.Equal(t, 0, sum)
	sum += getBal(txn1, ay)
	require.Equal(t, 100, sum)
	// Don't commit yet.

	txn2 := kv.NewTransaction(true)

	sum = getBal(txn2, ax)
	sum += getBal(txn2, ay)
	require.Equal(t, 200, sum)
	txn2.Set(ay, []byte("0"), 0) // Deduct 100 from ay.

	// Let's read this back.
	sum = getBal(txn2, ax)
	require.Equal(t, 100, sum)
	sum += getBal(txn2, ay)
	require.Equal(t, 100, sum)

	// Commit both now.
	require.NoError(t, txn1.Commit(nil))
	require.Error(t, txn2.Commit(nil)) // This should fail.

	require.Equal(t, uint64(2), kv.txnState.readTs())
}

// a2, a3, b4 (del), b3, c2, c1
// Read at ts=4 -> a3, c2
// Read at ts=3 -> a3, b3, c2
// Read at ts=2 -> a2, c2
// Read at ts=1 -> c1
func TestTxnIterationEdgeCase(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")

	// c1
	txn := kv.NewTransaction(true)
	txn.Set(kc, []byte("c1"), 0)
	require.NoError(t, txn.Commit(nil))
	require.Equal(t, uint64(1), kv.txnState.readTs())

	// a2, c2
	txn = kv.NewTransaction(true)
	txn.Set(ka, []byte("a2"), 0)
	txn.Set(kc, []byte("c2"), 0)
	require.NoError(t, txn.Commit(nil))
	require.Equal(t, uint64(2), kv.txnState.readTs())

	// b3
	txn = kv.NewTransaction(true)
	txn.Set(ka, []byte("a3"), 0)
	txn.Set(kb, []byte("b3"), 0)
	require.NoError(t, txn.Commit(nil))
	require.Equal(t, uint64(3), kv.txnState.readTs())

	// b4 (del)
	txn = kv.NewTransaction(true)
	txn.Delete(kb)
	require.NoError(t, txn.Commit(nil))
	require.Equal(t, uint64(4), kv.txnState.readTs())

	checkIterator := func(itr *Iterator, expected []string) {
		var i int
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			err := item.Value(func(val []byte) error {
				require.Equal(t, expected[i], string(val), "readts=%d", itr.readTs)
				return nil
			})
			require.NoError(t, err)
			i++
		}
		require.Equal(t, len(expected), i)
	}
	txn = kv.NewTransaction(true)
	itr := txn.NewIterator(DefaultIteratorOptions)
	checkIterator(itr, []string{"a3", "c2"})

	rev := DefaultIteratorOptions
	rev.Reverse = true
	itr = txn.NewIterator(rev)
	checkIterator(itr, []string{"c2", "a3"})

	txn.readTs = 3
	itr = txn.NewIterator(DefaultIteratorOptions)
	checkIterator(itr, []string{"a3", "b3", "c2"})
	itr = txn.NewIterator(rev)
	checkIterator(itr, []string{"c2", "b3", "a3"})

	txn.readTs = 2
	itr = txn.NewIterator(DefaultIteratorOptions)
	checkIterator(itr, []string{"a2", "c2"})
	itr = txn.NewIterator(rev)
	checkIterator(itr, []string{"c2", "a2"})

	txn.readTs = 1
	itr = txn.NewIterator(DefaultIteratorOptions)
	checkIterator(itr, []string{"c1"})
	itr = txn.NewIterator(rev)
	checkIterator(itr, []string{"c1"})
}

func TestTxnManaged(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	key := func(i int) []byte {
		return []byte(fmt.Sprintf("key-%02d", i))
	}

	val := func(i int) []byte {
		return []byte(fmt.Sprintf("val-%d", i))
	}

	// Write data at t=3.
	txn := kv.NewTransactionAt(3, true)
	for i := 0; i <= 3; i++ {
		require.NoError(t, txn.Set(key(i), val(i), 0))
	}
	require.NoError(t, txn.CommitAt(3, nil))

	// Read data at t=2.
	txn = kv.NewTransactionAt(2, false)
	for i := 0; i <= 3; i++ {
		_, err := txn.Get(key(i))
		require.Equal(t, ErrKeyNotFound, err)
	}

	// Read data at t=3.
	txn = kv.NewTransactionAt(3, false)
	for i := 0; i <= 3; i++ {
		item, err := txn.Get(key(i))
		require.NoError(t, err)
		require.Equal(t, uint64(3), item.Version())
		require.NoError(t, item.Value(func(v []byte) error {
			require.Equal(t, val(i), v)
			return nil
		}))
	}

	// Write data at t=7.
	txn = kv.NewTransactionAt(6, true)
	for i := 0; i <= 7; i++ {
		_, err := txn.Get(key(i))
		if err == nil {
			continue // Don't overwrite existing keys.
		}
		require.NoError(t, txn.Set(key(i), val(i), 0))
	}
	require.NoError(t, txn.CommitAt(7, nil))

	// Read data at t=9.
	txn = kv.NewTransactionAt(9, false)
	for i := 0; i <= 9; i++ {
		item, err := txn.Get(key(i))
		if i <= 7 {
			require.NoError(t, err)
		} else {
			require.Equal(t, ErrKeyNotFound, err)
		}

		if i <= 3 {
			require.Equal(t, uint64(3), item.Version())
		} else if i <= 7 {
			require.Equal(t, uint64(7), item.Version())
		}
		if i <= 7 {
			require.NoError(t, item.Value(func(v []byte) error {
				require.Equal(t, val(i), v)
				return nil
			}))
		}
	}
}
