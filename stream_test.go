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
	"os"
	"strconv"
	"strings"
	"testing"

	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

func openManaged(dir string) (*DB, error) {
	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir

	return OpenManaged(opt)
}

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
	defer os.RemoveAll(dir)

	db, err := openManaged(dir)
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
}
