/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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

package table

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
)

func TestTableIndex(t *testing.T) {
	rand.Seed(time.Now().Unix())
	keyPrefix := "key"
	t.Run("single key", func(t *testing.T) {
		opts := Options{Compression: options.ZSTD}
		f := buildTestTable(t, keyPrefix, 1, opts)
		tbl, err := OpenTable(f, opts)
		require.NoError(t, err)
		require.Len(t, tbl.blockIndex, 1)
	})

	t.Run("multiple keys", func(t *testing.T) {
		opts := []Options{}
		// Normal mode.
		opts = append(opts, Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01})
		// Encryption mode.
		key := make([]byte, 32)
		_, err := rand.Read(key)
		require.NoError(t, err)
		opts = append(opts, Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01,
			DataKey: &pb.DataKey{Data: key}})
		// Compression mode.
		opts = append(opts, Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01,
			Compression: options.ZSTD})
		keysCount := 10000
		for _, opt := range opts {
			builder := NewTableBuilder(opt)
			filename := fmt.Sprintf("%s%c%d.sst", os.TempDir(), os.PathSeparator, rand.Int63())
			f, err := y.OpenSyncedFile(filename, true)
			require.NoError(t, err)

			blockFirstKeys := make([][]byte, 0)
			blockCount := 0
			for i := 0; i < keysCount; i++ {
				k := []byte(fmt.Sprintf("%016x", i))
				v := fmt.Sprintf("%d", i)
				vs := y.ValueStruct{Value: []byte(v)}
				if i == 0 { // This is first key for first block.
					blockFirstKeys = append(blockFirstKeys, k)
					blockCount = 1
				} else if builder.shouldFinishBlock(k, vs) {
					blockCount++
					blockFirstKeys = append(blockFirstKeys, k)
				}
				builder.Add(k, vs)
			}
			_, err = f.Write(builder.Finish())
			require.NoError(t, err, "unable to write to file")

			tbl, err := OpenTable(f, opt)
			if opt.DataKey == nil {
				// key id is zero if thre is no datakey.
				require.Equal(t, tbl.KeyID(), uint64(0))
			}
			require.NoError(t, err, "unable to open table")

			// Ensure index is built correctly
			require.Equal(t, blockCount, len(tbl.blockIndex))
			for i, ko := range tbl.blockIndex {
				require.Equal(t, ko.Key, blockFirstKeys[i])
			}
			f.Close()
			require.NoError(t, os.RemoveAll(filename))
		}
	})
}

func TestInvalidCompression(t *testing.T) {
	keyPrefix := "key"
	opts := Options{Compression: options.ZSTD}
	f := buildTestTable(t, keyPrefix, 1000, opts)
	t.Run("with correct decompression algo", func(t *testing.T) {
		_, err := OpenTable(f, opts)
		require.NoError(t, err)
	})
	t.Run("with incorrect decompression algo", func(t *testing.T) {
		// Set incorrect compression algorithm.
		opts.Compression = options.Snappy
		_, err := OpenTable(f, opts)
		require.Error(t, err)
	})
}

func BenchmarkBuilder(b *testing.B) {
	rand.Seed(time.Now().Unix())
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%032d", i))
	}

	val := make([]byte, 32)
	rand.Read(val)
	vs := y.ValueStruct{Value: []byte(val)}

	keysCount := 1300000 // This number of entries consumes ~64MB of memory.
	for i := 0; i < b.N; i++ {
		opts := Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01}
		builder := NewTableBuilder(opts)

		for i := 0; i < keysCount; i++ {
			builder.Add(key(i), vs)
		}

		_ = builder.Finish()
	}
}

func TestKVSize(t *testing.T) {
	opts := Options{BlockSize: 4 * 1024, BloomFalsePositive: 0.01}
	b := NewTableBuilder(opts)
	defer b.Close()

	k := y.KeyWithTs([]byte(key("foo", 0)), 0)
	v := []byte(fmt.Sprintf("%d", 0))

	b.Add(k, y.ValueStruct{Value: v, Meta: 'A', UserMeta: 0})
	var entrySize uint64 = 15 /* DiffKey len */ + 4 /* Header Size */ + 4 /* Encoded vp */
	require.Equal(t, b.tableIndex.KvSize, entrySize)
}
