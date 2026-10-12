/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// compressibleData returns a highly compressible buffer of length n, by repeating
// a fixed pseudo-random block.
func compressibleData(n int) []byte {
	r := rand.New(rand.NewSource(42))
	block := make([]byte, 4096)
	r.Read(block)

	data := make([]byte, 0, n)
	for len(data) < n {
		data = append(data, block...)
	}
	return data[:n]
}

func TestZSTDCompressDecompressRoundTrip(t *testing.T) {
	data := compressibleData(1 << 20) // 1 MiB
	for _, level := range []int{1, 3, 15, 19} {
		t.Run(fmt.Sprintf("level-%d", level), func(t *testing.T) {
			dst := make([]byte, 0, ZSTDCompressBound(len(data)))
			out, err := ZSTDCompress(dst, data, level)
			require.NoError(t, err)
			require.NotEmpty(t, out)

			dec, err := ZSTDDecompress(nil, out)
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, dec), "round trip mismatch at level %d", level)
		})
	}
}

// TestZSTDCompressHonorsLevel ensures the compression level argument is honored
// across calls, and not pinned to the level used on the first call.
func TestZSTDCompressHonorsLevel(t *testing.T) {
	data := compressibleData(1 << 20)

	fastest := make([]byte, 0, ZSTDCompressBound(len(data)))
	outFastest, err := ZSTDCompress(fastest, data, 1)
	require.NoError(t, err)

	best := make([]byte, 0, ZSTDCompressBound(len(data)))
	outBest, err := ZSTDCompress(best, data, 19)
	require.NoError(t, err)

	require.False(t, bytes.Equal(outFastest, outBest),
		"level 1 and level 19 must produce different compressed output")
}

// TestZSTDCompressConcurrent deterministically exercises the first-use race in
// ZSTDCompress: after clearing the cache, it releases many goroutines against two
// levels at once so they all miss the Load and collide on LoadOrStore. Each level
// must resolve to exactly one encoder, independent of the other.
func TestZSTDCompressConcurrent(t *testing.T) {
	data := compressibleData(64 << 10)

	// Clear the cache entries for both levels so every goroutine below takes the
	// first-use path (other tests in this package may have already populated them).
	levels := []int{1, 19}
	resolved := make([]zstd.EncoderLevel, len(levels))
	for i, l := range levels {
		resolved[i] = zstd.EncoderLevelFromZstd(l)
		encoders.Delete(resolved[i])
	}

	const numGoroutines = 128
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	addErr := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	for i := range numGoroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			level := levels[i%len(levels)]
			<-start // barrier: release every goroutine at the same instant
			out, err := ZSTDCompress(nil, data, level)
			if err != nil {
				addErr(err)
				return
			}
			dec, err := ZSTDDecompress(nil, out)
			if err != nil {
				addErr(err)
				return
			}
			if !bytes.Equal(data, dec) {
				addErr(fmt.Errorf("round trip mismatch at level %d", level))
			}
		}(i)
	}
	close(start)
	wg.Wait()

	require.Empty(t, errs)

	// Exactly one encoder must survive for each level; the losers Close their
	// duplicates via LoadOrStore.
	countFor := func(level zstd.EncoderLevel) int {
		var n int
		encoders.Range(func(key, _ any) bool {
			if key == level {
				n++
			}
			return true
		})
		return n
	}
	for i, l := range levels {
		require.Equalf(t, 1, countFor(resolved[i]),
			"exactly one encoder should survive for level %d", l)
	}
}
