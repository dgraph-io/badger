/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2"
)

func TestTableIndex(t *testing.T) {
	rand.Seed(time.Now().Unix())
	keysCount := 100000
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	cache, err := ristretto.NewCache[uint64, *fb.TableIndex](&ristretto.Config[uint64, *fb.TableIndex]{
		NumCounters: 1000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	require.NoError(t, err)
	subTest := []struct {
		name string
		opts Options
	}{
		{
			name: "No encryption/compression",
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
				IndexCache:         cache,
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
				IndexCache:           cache,
			},
		},
	}

	for _, tt := range subTest {
		t.Run(tt.name, func(t *testing.T) {
			opt := tt.opts
			builder := NewTableBuilder(opt)
			defer builder.Close()
			filename := fmt.Sprintf("%s%c%d.sst", os.TempDir(), os.PathSeparator, rand.Uint32())

			blockFirstKeys := make([][]byte, 0)
			blockCount := 0
			for i := 0; i < keysCount; i++ {
				k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), uint64(i+1))
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
			tbl, err := CreateTable(filename, builder)
			require.NoError(t, err, "unable to open table")

			if opt.DataKey == nil {
				// key id is zero if there is no datakey.
				require.Equal(t, tbl.KeyID(), uint64(0))
			}

			// Ensure index is built correctly
			require.Equal(t, blockCount, tbl.offsetsLength())
			idx, err := tbl.readTableIndex()
			require.NoError(t, err)
			for i := 0; i < idx.OffsetsLength(); i++ {
				var bo fb.BlockOffset
				require.True(t, idx.Offsets(&bo, i))
				require.Equal(t, blockFirstKeys[i], bo.KeyBytes())
			}
			require.Equal(t, keysCount, int(tbl.MaxVersion()))
			tbl.Close(-1)
			require.NoError(t, os.RemoveAll(filename))
		})
	}
}

func TestInvalidCompression(t *testing.T) {
	keyPrefix := "key"
	opts := Options{BlockSize: 4 << 10, Compression: options.ZSTD}
	tbl := buildTestTable(t, keyPrefix, 1000, opts)
	defer func() { require.NoError(t, tbl.DecrRef()) }()
	mf := tbl.MmapFile
	t.Run("with correct decompression algo", func(t *testing.T) {
		_, err := OpenTable(mf, opts)
		require.NoError(t, err)
	})
	t.Run("with incorrect decompression algo", func(t *testing.T) {
		// Set incorrect compression algorithm.
		opts.Compression = options.Snappy
		_, err := OpenTable(mf, opts)
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
	vs := y.ValueStruct{Value: val}

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
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := NewTableBuilder(*opt)
			for j := 0; j < keysCount; j++ {
				builder.Add(keyList[j], vs, 0)
			}

			_ = builder.Finish()
			builder.Close()
		}
	}

	b.Run("no compression", func(b *testing.B) {
		var opt Options
		opt.Compression = options.None
		bench(b, &opt)
	})
	b.Run("encryption", func(b *testing.B) {
		var opt Options
		cache, err := ristretto.NewCache(&ristretto.Config[uint64, *fb.TableIndex]{
			NumCounters: 1000,
			MaxCost:     1 << 20,
			BufferItems: 64,
		})
		require.NoError(b, err)
		opt.IndexCache = cache
		key := make([]byte, 32)
		rand.Read(key)
		opt.DataKey = &pb.DataKey{Data: key}
		bench(b, &opt)
	})
	b.Run("snappy compression", func(b *testing.B) {
		var opt Options
		opt.Compression = options.Snappy
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
		}
		if withBlooms {
			opts.BloomFalsePositive = 0.01
		}
		tab := buildTestTable(t, keyPrefix, keyCount, opts)
		defer func() { require.NoError(t, tab.DecrRef()) }()
		require.Equal(t, withBlooms, tab.hasBloomFilter)
		// Forward iteration
		it := tab.NewIterator(0)
		c := 0
		for it.Rewind(); it.Valid(); it.Next() {
			c++
			hash := y.Hash(y.ParseKey(it.Key()))
			require.False(t, tab.DoesNotHave(hash))
		}
		require.Equal(t, keyCount, c)

		// Backward iteration
		it = tab.NewIterator(REVERSED)
		c = 0
		for it.Rewind(); it.Valid(); it.Next() {
			c++
			hash := y.Hash(y.ParseKey(it.Key()))
			require.False(t, tab.DoesNotHave(hash))
		}
		require.Equal(t, keyCount, c)

		// Ensure tab.DoesNotHave works
		hash := y.Hash([]byte("foo"))
		require.Equal(t, withBlooms, tab.DoesNotHave(hash))
	}

	t.Run("build with bloom filter", func(t *testing.T) {
		createAndTest(t, true)
	})
	t.Run("build without bloom filter", func(t *testing.T) {
		createAndTest(t, false)
	})
}
func TestEmptyBuilder(t *testing.T) {
	opts := Options{BloomFalsePositive: 0.1}
	b := NewTableBuilder(opts)
	defer b.Close()
	require.Equal(t, []byte{}, b.Finish())

}

func TestKeyDiff(t *testing.T) {
	// keyDiff only reads b.curBlock.baseKey; construct a minimal Builder.
	mk := func(base []byte) *Builder {
		return &Builder{curBlock: &bblock{baseKey: base}}
	}

	cases := []struct {
		name string
		base []byte
		k    []byte
		want []byte
	}{
		{"empty base", nil, []byte("anything"), []byte("anything")},
		{"empty new", []byte("base"), nil, []byte{}},
		{"identical short", []byte("abc"), []byte("abc"), []byte{}},
		{"identical 8 bytes", []byte("01234567"), []byte("01234567"), []byte{}},
		{"identical 16 bytes", []byte("0123456789abcdef"), []byte("0123456789abcdef"), []byte{}},
		{"diff at byte 0 (tail-only path: len<8)", []byte("abc"), []byte("xbc"), []byte("xbc")},
		{"diff at byte 1 (tail-only)", []byte("abc"), []byte("aXc"), []byte("Xc")},
		{"diff at last tail byte", []byte("abcdefg"), []byte("abcdefX"), []byte("X")},
		{"diff in first word: byte 0", []byte("01234567abcd"), []byte("X1234567abcd"), []byte("X1234567abcd")},
		{"diff in first word: byte 3", []byte("01234567abcd"), []byte("012X4567abcd"), []byte("X4567abcd")},
		{"diff in first word: byte 7", []byte("01234567abcd"), []byte("0123456Xabcd"), []byte("Xabcd")},
		{"diff in second word at byte 8", []byte("0123456789abcdef"), []byte("01234567X9abcdef"), []byte("X9abcdef")},
		{"diff in tail after one word (byte 8 of 9)", []byte("012345678"), []byte("012345679"), []byte("9")},
		{"diff in tail after one word (byte 10 of 11)", []byte("0123456789a"), []byte("0123456789X"), []byte("X")},
		{"new longer, full prefix match", []byte("0123"), []byte("0123XYZ"), []byte("XYZ")},
		{"new shorter, full prefix match", []byte("0123XYZ"), []byte("0123"), []byte{}},
		{"new longer, full 8-byte prefix match", []byte("01234567"), []byte("01234567tail"), []byte("tail")},
		{"new longer than base by exactly 8", []byte("01234567"), []byte("0123456789ABCDEF"), []byte("89ABCDEF")},
		{"binary keys with zero bytes", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{0, 1, 2, 3, 4, 5, 6, 0xff, 8, 9}, []byte{0xff, 8, 9}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := mk(c.base)
			got := b.keyDiff(c.k)
			require.Equal(t, string(c.want), string(got))
		})
	}
}

// TestKeyDiffMatchesNaive cross-checks the word-wise implementation against a
// byte-by-byte reference over randomized inputs.
func TestKeyDiffMatchesNaive(t *testing.T) {
	naive := func(base, newKey []byte) []byte {
		n := len(newKey)
		if m := len(base); m < n {
			n = m
		}
		i := 0
		for i < n && newKey[i] == base[i] {
			i++
		}
		return newKey[i:]
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 2000; i++ {
		baseLen := rng.Intn(40)
		newLen := rng.Intn(40)
		base := make([]byte, baseLen)
		newKey := make([]byte, newLen)
		_, _ = rng.Read(base)
		_, _ = rng.Read(newKey)
		// With ~50% probability, share a random prefix to stress LCP boundary.
		if rng.Intn(2) == 0 && baseLen > 0 && newLen > 0 {
			share := rng.Intn(min(baseLen, newLen) + 1)
			copy(newKey[:share], base[:share])
		}
		b := &Builder{curBlock: &bblock{baseKey: base}}
		require.Equal(t, string(naive(base, newKey)), string(b.keyDiff(newKey)))
	}
}
