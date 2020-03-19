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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/FastFilter/xorfilter"
	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
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
				k := []byte(fmt.Sprintf("%016d", i))
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
			_, err = f.Write(builder.Finish())
			require.NoError(t, err, "unable to write to file")

			tbl, err := OpenTable(f, opt)
			require.NoError(t, err, "unable to open table")

			if opt.DataKey == nil {
				// key id is zero if there is no datakey.
				require.Equal(t, tbl.KeyID(), uint64(0))
			}

			// Ensure index is built correctly
			require.Equal(t, blockCount, len(tbl.blockIndex))
			for i, ko := range tbl.blockIndex {
				require.Equal(t, ko.Key, blockFirstKeys[i])
			}
			f.Close()
			require.NoError(t, os.RemoveAll(filename))
		})
	}
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

	// Approximate number of 32 byte entries in 64 MB table.
	keysCount := 840000

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
			_ = builder.Finish()
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

//name                           time/op
//Bloom/Set/bloom-8              23.2ms ± 3%
//Bloom/Set/xorFilter-8           121ms ± 4%
//Bloom/Get/success/bloom-8      20.9ns ± 5%
//Bloom/Get/success/xorFilter-8  7.41ns ± 7%
//Bloom/Get/failure/bloom-8      8.27ns ± 6%
//Bloom/Get/failure/xorFilter-8  7.52ns ± 5%
func BenchmarkBloom(b *testing.B) {
	numKeys := 840000 // Amount of keys in a 64 MB table.
	// Random key for lookup. Key1 exists, key2 does not.
	key1 := farm.Fingerprint64([]byte(fmt.Sprintf("%032d", numKeys/2+1283)))
	key2 := farm.Fingerprint64([]byte(fmt.Sprintf("%032d", numKeys+12312)))
	keyHashes := make([]uint64, numKeys)

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("%032d", i))
		keyHashes[i] = farm.Fingerprint64(key)
	}

	var res []byte
	var zbloom *z.Bloom
	var xorFilter *xorfilter.Xor8

	prepareZbloom := func() {
		bf := z.NewBloomFilter(float64(len(keyHashes)), 0.03)
		for _, h := range keyHashes {
			bf.Add(h)
		}
		zbloom = bf
	}
	prepareXORFilter := func() {
		filter, err := xorfilter.Populate(keyHashes)
		if err != nil {
			panic(err)
		}
		xorFilter = filter

	}
	b.Run("Set", func(b *testing.B) {
		b.Run("bloom", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Use 3% false positive rate because that's the default in xor filter.
				bf := z.NewBloomFilter(float64(len(keyHashes)), 0.03)
				for _, h := range keyHashes {
					bf.Add(h)
				}
				res = bf.JSONMarshal()
			}
		})
		b.Run("xorFilter", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				filter, err := xorfilter.Populate(keyHashes)
				if i == 0 && err != nil {
					b.Log(err)
					b.Fail()
				}
				res, err = json.Marshal(filter)
				if i == 0 && err != nil {
					b.Log(err)
					b.Fail()
				}
			}
		})
	})
	_ = res
	b.Run("Get", func(b *testing.B) {
		b.Run("success", func(b *testing.B) {
			b.Run("bloom", func(b *testing.B) {
				prepareZbloom()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if !zbloom.Has(key1) {
						b.Fail()
					}
				}
			})
			b.Run("xorFilter", func(b *testing.B) {
				prepareXORFilter()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if !xorFilter.Contains(key1) {
						b.Fail()
					}
				}
			})
		})
		b.Run("failure", func(b *testing.B) {
			b.Run("bloom", func(b *testing.B) {
				prepareZbloom()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if zbloom.Has(key2) {
						b.Fail()
					}
				}
			})
			b.Run("xorFilter", func(b *testing.B) {
				prepareXORFilter()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if xorFilter.Contains(key2) {
						b.Fail()
					}
				}
			})
		})
	})
}
