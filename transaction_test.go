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

func TestSimpleTxn(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	txn, err := kv.NewTransaction()
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key=%d", i))
		v := []byte(fmt.Sprintf("val=%d", i))
		txn.Set(k, v, 0)
	}

	var item KVItem
	require.NoError(t, txn.Get([]byte("key=8"), &item))

	fn := func(val []byte) error {
		require.Equal(t, []byte("val=8"), val)
		return nil
	}
	require.NoError(t, item.Value(fn))
	require.NoError(t, txn.Commit())
}

func TestWriteSkew(t *testing.T) {
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
	txn, err := kv.NewTransaction()
	require.NoError(t, err)
	val := []byte(strconv.Itoa(100))
	txn.Set(ax, val, 0)
	txn.Set(ay, val, 0)
	require.NoError(t, txn.Commit())
	require.Equal(t, uint64(1), kv.txnState.readTs())

	getBal := func(txn *Txn, key []byte) (bal int) {
		var item KVItem
		require.NoError(t, txn.Get(key, &item))
		err := item.Value(func(val []byte) error {
			var err error
			bal, err = strconv.Atoi(string(val))
			return err
		})
		require.NoError(t, err)
		return bal
	}

	// Start two transactions, each would read both accounts and deduct from one account.
	txn1, err := kv.NewTransaction()
	require.NoError(t, err)

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

	txn2, err := kv.NewTransaction()
	require.NoError(t, err)

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
	require.NoError(t, txn1.Commit())
	require.Error(t, txn2.Commit()) // This should fail.

	require.Equal(t, uint64(2), kv.txnState.readTs())
}
