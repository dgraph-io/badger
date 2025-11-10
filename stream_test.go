/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

func keyWithPrefix(prefix string, k int) []byte {
	return []byte(fmt.Sprintf("%s-%d", prefix, k))
}

func TestStreamKeyToListWithThreadId_MapReduceWordFreq(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	// Seed dataset with words: beta, gamma, alpha
	// Mapping: i%3==0 -> beta (33), i%3==1 -> gamma (34), i%3==2 -> alpha (33) per 1..100
	words := []string{"beta", "gamma", "alpha"}
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			w := words[i%3]
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), []byte(w))))
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.NumGo = 16
	stream.UseKeyToListWithThreadId = true

	// Accumulate per-thread word counts to be emitted in FinishThread.
	// Use a slice indexed by threadId. Each threadId only writes to its own slot.
	toCounts := make([]map[string]int, stream.NumGo)

	stream.KeyToListWithThreadId = func(key []byte, itr *Iterator, threadId int) (*bpb.KVList, error) {
		item := itr.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		w := string(val)
		if toCounts[threadId] == nil {
			toCounts[threadId] = make(map[string]int)
		}
		toCounts[threadId][w]++
		// Return nothing here; flushing happens in FinishThread.
		return &bpb.KVList{}, nil
	}

	stream.FinishThread = func(threadId int) (*bpb.KVList, error) {
		counts := toCounts[threadId]
		if len(counts) == 0 {
			return &bpb.KVList{}, nil
		}
		out := make([]*bpb.KV, 0, len(counts))
		for w, c := range counts {
			out = append(out, &bpb.KV{Key: []byte("wc-" + w), Value: []byte(fmt.Sprintf("%d", c))})
		}
		return &bpb.KVList{Kv: out}, nil
	}

	// Use a sink, but expect no data as KeyToListWithThreadId returns nothing and FinishThread returns empty list
	c := &collector{}
	stream.Send = c.Send

	require.NoError(t, stream.Orchestrate(ctxb))

	// Reduce: aggregate per-word totals from partial outputs
	totals := map[string]int{}
	for _, kv := range c.kv {
		if strings.HasPrefix(string(kv.Key), "wc-") {
			w := strings.TrimPrefix(string(kv.Key), "wc-")
			n, err := strconv.Atoi(string(kv.Value))
			require.NoError(t, err)
			totals[w] += n
		}
	}

	// Expected totals across 3 prefixes x 100 items with distribution defined above
	require.Equal(t, 99, totals["alpha"])  // 33 per prefix * 3
	require.Equal(t, 99, totals["beta"])   // 33 per prefix * 3
	require.Equal(t, 102, totals["gamma"]) // 34 per prefix * 3

	require.NoError(t, db.Close())
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

func (c *collector) Send(buf *z.Buffer) error {
	list, err := BufferToKVList(buf)
	if err != nil {
		return err
	}
	for _, kv := range list.Kv {
		if kv.StreamDone == true {
			return nil
		}
		cp := proto.Clone(kv).(*bpb.KV)
		c.kv = append(c.kv, cp)
	}
	return err
}

var ctxb = context.Background()

func TestStream(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
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
	stream.Send = c.Send

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

func TestStreamKeyToListWithThreadId(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	// Seed small dataset
	for _, prefix := range []string{"p0", "p1", "p2"} {
		txn := db.NewTransactionAt(math.MaxUint64, true)
		for i := 1; i <= 100; i++ {
			require.NoError(t, txn.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
		}
		require.NoError(t, txn.CommitAt(5, nil))
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.NumGo = 4 // fix number of threads for deterministic assertions
	stream.UseKeyToListWithThreadId = true

	// Ensure threadId passed to KeyToListWithThreadId matches iterator's ThreadId
	stream.KeyToListWithThreadId = func(key []byte, itr *Iterator, threadId int) (*bpb.KVList, error) {
		require.Equal(t, threadId, itr.ThreadId)
		return stream.ToList(key, itr)
	}

	// Emit a per-thread marker to verify FinishThread is invoked once per thread
	stream.FinishThread = func(threadId int) (*bpb.KVList, error) {
		kv := &bpb.KV{Key: []byte(fmt.Sprintf("done-%d", threadId))}
		return &bpb.KVList{Kv: []*bpb.KV{kv}}, nil
	}

	c := &collector{}
	stream.Send = c.Send

	err = stream.Orchestrate(ctxb)
	require.NoError(t, err)

	// Verify presence of FinishThread markers and totals
	markers := make(map[string]struct{})
	for _, kv := range c.kv {
		if strings.HasPrefix(string(kv.Key), "done-") {
			markers[string(kv.Key)] = struct{}{}
		}
	}
	// Total should be data KVs plus marker count
	require.Equal(t, 300+len(markers), len(c.kv))
	// We expect at least one marker and at most NumGo markers (ranges may be fewer than NumGo)
	require.GreaterOrEqual(t, len(markers), 1)
	require.LessOrEqual(t, len(markers), stream.NumGo)

	require.NoError(t, db.Close())
}

func TestStreamMaxSize(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	// Set the maxStreamSize to 1MB for the duration of the test so that the it can use a smaller
	// dataset than it would otherwise need.
	originalMaxStreamSize := maxStreamSize
	maxStreamSize = 1 << 20
	defer func() {
		maxStreamSize = originalMaxStreamSize
	}()

	testSize := int(1e6)
	dir, err := os.MkdirTemp("", "badger-big-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	wb := db.NewWriteBatchAt(5)
	for _, prefix := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= testSize; i++ {
			require.NoError(t, wb.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
	}
	require.NoError(t, wb.Flush())

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = c.Send

	// default value
	require.Equal(t, stream.MaxSize, maxStreamSize)

	// reset maxsize
	stream.MaxSize = 1024 * 1024 * 50

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
	require.NoError(t, db.Close())
}

func TestStreamWithThreadId(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
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
	stream.Send = c.Send

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
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	// Set the maxStreamSize to 1MB for the duration of the test so that the it can use a smaller
	// dataset than it would otherwise need.
	originalMaxStreamSize := maxStreamSize
	maxStreamSize = 1 << 20
	defer func() {
		maxStreamSize = originalMaxStreamSize
	}()

	testSize := int(1e6)
	dir, err := os.MkdirTemp("", "badger-big-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	wb := db.NewWriteBatchAt(5)
	for _, prefix := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= testSize; i++ {
			require.NoError(t, wb.SetEntry(NewEntry(keyWithPrefix(prefix, i), value(i))))
			count++
		}
	}
	require.NoError(t, wb.Flush())

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	c := &collector{}
	stream.Send = c.Send

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
	require.NoError(t, db.Close())
}

// There was a bug in the stream writer code which would cause allocators to be
// freed up twice if the default keyToList was not used. This test verifies that issue.
func TestStreamCustomKeyToList(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := OpenManaged(DefaultOptions(dir))
	require.NoError(t, err)

	var count int
	for _, key := range []string{"p0", "p1", "p2"} {
		for i := 1; i <= 100; i++ {
			txn := db.NewTransactionAt(math.MaxUint64, true)
			require.NoError(t, txn.SetEntry(NewEntry([]byte(key), value(i))))
			count++
			require.NoError(t, txn.CommitAt(uint64(i), nil))
		}
	}

	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = "Testing"
	stream.KeyToList = func(key []byte, itr *Iterator) (*bpb.KVList, error) {
		item := itr.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kv := &bpb.KV{
			Key:   y.Copy(item.Key()),
			Value: val,
		}
		return &bpb.KVList{
			Kv: []*bpb.KV{kv},
		}, nil
	}
	res := map[string]struct{}{"p0": {}, "p1": {}, "p2": {}}
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		require.NoError(t, err)
		for _, kv := range list.Kv {
			key := string(kv.Key)
			if _, ok := res[key]; !ok {
				panic(fmt.Sprintf("%s key not found", key))
			}
			delete(res, key)
		}
		return nil
	}
	require.NoError(t, stream.Orchestrate(ctxb))
	require.Zero(t, len(res))
}
