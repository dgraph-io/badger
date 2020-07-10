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
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"testing"

	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

func keyWithPrefix(prefix string, k int) []byte {
	return []byte(fmt.Sprintf("%s-%d", prefix, k))
}

func keyToInt(k []byte) (string, int) {
	splits := strings.Split(string(k), "-")
	key, err := strconv.Atoi(splits[1])
	y.Check(err)
	return splits[0], key
}

func value(k int) []byte {
	return []byte(fmt.Sprintf("%08d", k))
}

type collector struct {
	kv []*bpb.KV
}

func (c *collector) Send(list *bpb.KVList) error {
	c.kv = append(c.kv, list.Kv...)
	return nil
}

var ctxb = context.Background()

func TestStream(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = func(list *bpb.KVList) error {
		return c.Send(list)
	}

	// Test case 1. Retrieve everything.
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 300, len(c.kv), "Expected 300. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 2. Retrieve only 1 predicate.
	stream.Prefix = []byte("p1")
	c.kv = c.kv[:0]
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 100, len(c.kv), "Expected 100. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 1, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 3. Retrieve select keys within the predicate.
	c.kv = c.kv[:0]
	stream.ChooseKey = func(item *Item) bool {
		_, k := keyToInt(item.Key())
		return k%2 == 0
	}
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 50, len(c.kv), "Expected 50. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 1, len(m))
	for pred, count := range m {
		require.Equal(t, 50, count, "Count mismatch for pred: %s", pred)
	}

	// Test case 4. Retrieve select keys from all predicates.
	c.kv = c.kv[:0]
	stream.Prefix = []byte{}
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 150, len(c.kv), "Expected 150. Got: %d", len(c.kv))

	m = make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 50, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

func TestStreamWithThreadId(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.KeyToList = func(key []byte, itr *Iterator) (
		*bpb.KVList, error) {
		require.Less(t, itr.ThreadId, stream.NumGo)
		return stream.ToList(key, itr)
	}
	c := &collector{}
	stream.Send = func(list *bpb.KVList) error {
		return c.Send(list)
	}

	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 300, len(c.kv), "Expected 300. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, 100, count, "Count mismatch for pred: %s", pred)
	}
	require.NoError(t, db.Close())
}

func TestBigStream(t *testing.T) {
	// Set the maxStreamSize to 1MB for the duration of the test so that the it can use a smaller
	// dataset than it would otherwise need.
	originalMaxStreamSize := maxStreamSize
	maxStreamSize = 1 << 20
	defer func() {
		maxStreamSize = originalMaxStreamSize
	}()

	testSize := int(1e6)
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= testSize; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
			if i % 1000 == 0 {
				require.NoError(t, txn.CommitAt(5, nil))
				txn = db.NewTransactionAt(math.MaxUint64, true)
			}
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = func(list *bpb.KVList) error {
		return c.Send(list)
	}

	// Test case 1. Retrieve everything.
	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)
	require.Equal(t, 3*testSize, len(c.kv), "Expected 30000. Got: %d", len(c.kv))

	m := make(map[string]int)
	for _, kv := range c.kv {
		prefix, ki := keyToInt(kv.Key)
		expected := value(ki)
		require.Equal(t, expected, kv.Value)
		m[prefix]++
	}
	require.Equal(t, 3, len(m))
	for pred, count := range m {
		require.Equal(t, testSize, count, "Count mismatch for pred: %s", pred)
	}
}

