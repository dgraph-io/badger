/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Benchmarks for Txn.GetBatch vs an equivalent loop of Txn.Get, and for the cost of the
// useGetBatch flag on the plain single-key Get path. The DB is populated once per
// benchmark invocation and read-only afterwards, so ns/op reflects pure read cost.
// The serial variant opens a transaction per key, matching how dgraph's
// readPostingListAt issues its point reads; the batch variant opens one transaction per
// batch, matching GetBatchSinglePosting.

const benchGetBatchKeys = 100000

func benchOpenPopulated(b *testing.B, useGetBatch bool) (*DB, [][]byte, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "badger-bench")
	require.NoError(b, err)
	opts := getTestOptions(dir)
	opts.managedTxns = true
	opts.useGetBatch = useGetBatch
	db, err := Open(opts)
	require.NoError(b, err)

	val := bytes.Repeat([]byte("v"), 64)
	wb := db.NewWriteBatchAt(2)
	keys := make([][]byte, benchGetBatchKeys)
	for i := 0; i < benchGetBatchKeys; i++ {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		require.NoError(b, wb.Set(keys[i], val))
	}
	require.NoError(b, wb.Flush())
	cleanup := func() {
		require.NoError(b, db.Close())
		removeDir(dir)
	}
	return db, keys, cleanup
}

func readItemValue(b *testing.B, item *Item) {
	b.Helper()
	if item == nil {
		return
	}
	if _, err := item.ValueCopy(nil); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkGetBatchVsSerial: fetch `batch` keys per op — serial (txn+Get per key) vs one
// txn.GetBatch — over adjacent key runs (dgraph's access pattern) and random keys.
func BenchmarkGetBatchVsSerial(b *testing.B) {
	db, keys, cleanup := benchOpenPopulated(b, true)
	defer cleanup()
	readTs := uint64(10)

	rng := rand.New(rand.NewSource(42))
	randomIdx := rng.Perm(len(keys))

	pick := func(pattern string, off, j int) []byte {
		if pattern == "adjacent" {
			return keys[off+j]
		}
		return keys[randomIdx[off+j]]
	}

	for _, pattern := range []string{"adjacent", "random"} {
		for _, batch := range []int{1, 10, 32, 100} {
			window := len(keys) - batch
			b.Run(fmt.Sprintf("%s/batch=%d/serial", pattern, batch), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					off := (i * batch) % window
					for j := 0; j < batch; j++ {
						txn := db.NewTransactionAt(readTs, false)
						item, err := txn.Get(pick(pattern, off, j))
						if err != nil {
							b.Fatal(err)
						}
						readItemValue(b, item)
						txn.Discard()
					}
				}
			})
			b.Run(fmt.Sprintf("%s/batch=%d/getbatch", pattern, batch), func(b *testing.B) {
				b.ReportAllocs()
				batchKeys := make([][]byte, batch)
				for i := 0; i < b.N; i++ {
					off := (i * batch) % window
					for j := 0; j < batch; j++ {
						batchKeys[j] = pick(pattern, off, j)
					}
					txn := db.NewTransactionAt(readTs, false)
					items, err := txn.GetBatch(batchKeys)
					if err != nil {
						b.Fatal(err)
					}
					for _, item := range items {
						readItemValue(b, item)
					}
					txn.Discard()
				}
			})
		}
	}
}

// BenchmarkSingleGetFlag: the cost of routing the plain single-key Get through the
// batched internals (useGetBatch=true, the PR's default) vs the classic path.
func BenchmarkSingleGetFlag(b *testing.B) {
	for _, flag := range []bool{false, true} {
		b.Run(fmt.Sprintf("useGetBatch=%v", flag), func(b *testing.B) {
			db, keys, cleanup := benchOpenPopulated(b, flag)
			defer cleanup()
			readTs := uint64(10)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(readTs, false)
				item, err := txn.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatal(err)
				}
				readItemValue(b, item)
				txn.Discard()
			}
		})
	}
}
