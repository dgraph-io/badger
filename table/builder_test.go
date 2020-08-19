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

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
)

func TestTableIndex(t *testing.T) {
	rand.Seed(time.Now().Unix())
	keysCount := 100000
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	subTest := []struct {
		name string
		opts Options
	}{
		{
			name: "No encyption/compression",
			opts: Options{
				BlockSize:          4 * 1024,
				BloomFalsePositive: 0.01,
				TableSize:          30 << 20,
			},
		},
		{
			// Encryption mode.
			name: "Only encryption",
			opts: Options{
				BlockSize:          4 * 1024,
				BloomFalsePositive: 0.01,
				TableSize:          30 << 20,
				DataKey:            &pb.DataKey{Data: key},
			},
		},
		{
			// Compression mode.
			name: "Only compression",
			opts: Options{
				BlockSize:            4 * 1024,
				BloomFalsePositive:   0.01,
				TableSize:            30 << 20,
				Compression:          options.ZSTD,
				ZSTDCompressionLevel: 3,
			},
		},
		{
			// Compression mode and encryption.
			name: "Compression and encryption",
			opts: Options{
				BlockSize:            4 * 1024,
				BloomFalsePositive:   0.01,
				TableSize:            30 << 20,
				Compression:          options.ZSTD,
				ZSTDCompressionLevel: 3,
				DataKey:              &pb.DataKey{Data: key},
			},
		},
	}

	for _, tt := range subTest {
		t.Run(tt.name, func(t *testing.T) {
			opt := tt.opts
			builder := NewTableBuilder(opt)
			filename := fmt.Sprintf("%s%c%d.sst", os.TempDir(), os.PathSeparator, rand.Uint32())
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
				builder.Add(k, vs, 0)
			}
			_, err = f.Write(builder.Finish(false))
			require.NoError(t, err, "unable to write to file")

			tbl, err := OpenTable(f, opt)
			require.NoError(t, err, "unable to open table")

			if opt.DataKey == nil {
				// key id is zero if there is no datakey.
				require.Equal(t, tbl.KeyID(), uint64(0))
			}

			// Ensure index is built correctly
			require.Equal(t, blockCount, tbl.noOfBlocks)
			for i, ko := range tbl.readTableIndex().Offsets {
				require.Equal(t, ko.Key, blockFirstKeys[i])
			}
			f.Close()
			require.NoError(t, os.RemoveAll(filename))
		})
	}
}

func TestInvalidCompression(t *testing.T) {
	keyPrefix := "key"
	opts := Options{BlockSize: 4 << 10, Compression: options.ZSTD}
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

	var keyList [][]byte
	for i := 0; i < keysCount; i++ {
		keyList = append(keyList, key(i))
	}
	bench := func(b *testing.B, opt *Options) {
		b.SetBytes(int64(keysCount) * (32 + 32))
		opt.BlockSize = 4 * 1024
		opt.BloomFalsePositive = 0.01
		opt.TableSize = 5 << 20
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			builder := NewTableBuilder(*opt)
			for j := 0; j < keysCount; j++ {
				builder.Add(keyList[j], vs, 0)
			}
			_ = builder.Finish(false)
		}
	}

	b.Run("no compression", func(b *testing.B) {
		var opt Options
		opt.Compression = options.None
		bench(b, &opt)
	})
	b.Run("encryption", func(b *testing.B) {
		var opt Options
		key := make([]byte, 32)
		rand.Read(key)
		opt.DataKey = &pb.DataKey{Data: key}
		bench(b, &opt)
	})
	b.Run("zstd compression", func(b *testing.B) {
		var opt Options
		opt.Compression = options.ZSTD
		b.Run("level 1", func(b *testing.B) {
			opt.ZSTDCompressionLevel = 1
			bench(b, &opt)
		})
		b.Run("level 3", func(b *testing.B) {
			opt.ZSTDCompressionLevel = 3
			bench(b, &opt)
		})
		b.Run("level 15", func(b *testing.B) {
			opt.ZSTDCompressionLevel = 15
			bench(b, &opt)
		})
	})
}

func TestBloomfilter(t *testing.T) {
	keyPrefix := "p"
	keyCount := 1000

	createAndTest := func(t *testing.T, withBlooms bool) {
		opts := Options{
			BloomFalsePositive: 0.0,
			LoadBloomsOnOpen:   false,
		}
		if withBlooms {
			opts = Options{
				BloomFalsePositive: 0.01,
				LoadBloomsOnOpen:   true,
			}
		}
		f := buildTestTable(t, keyPrefix, keyCount, opts)
		tab, err := OpenTable(f, opts)
		require.NoError(t, err)
		require.Equal(t, withBlooms, tab.hasBloomFilter)
		// Forward iteration
		it := tab.NewIterator(false)
		c := 0
		for it.Rewind(); it.Valid(); it.Next() {
			c++
			hash := farm.Fingerprint64(y.ParseKey(it.Key()))
			require.False(t, tab.DoesNotHave(hash))
		}
		require.Equal(t, keyCount, c)

		// Backward iteration
		it = tab.NewIterator(true)
		c = 0
		for it.Rewind(); it.Valid(); it.Next() {
			c++
			hash := farm.Fingerprint64(y.ParseKey(it.Key()))
			require.False(t, tab.DoesNotHave(hash))
		}
		require.Equal(t, keyCount, c)

		// Ensure tab.DoesNotHave works
		hash := farm.Fingerprint64([]byte("foo"))
		require.Equal(t, withBlooms, tab.DoesNotHave(hash))
	}

	t.Run("build with bloom filter", func(t *testing.T) {
		createAndTest(t, true)
	})
	t.Run("build without bloom filter", func(t *testing.T) {
		createAndTest(t, false)
	})
}
