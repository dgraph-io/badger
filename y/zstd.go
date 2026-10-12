/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	decoder *zstd.Decoder
	decOnce sync.Once

	// encoders caches one zstd.Encoder per resolved encoder level. A zstd.Encoder
	// is safe for concurrent use and its level is fixed once created, so a single
	// encoder per level can be shared across all callers.
	encoders sync.Map // zstd.EncoderLevel -> *zstd.Encoder
)

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func ZSTDDecompress(dst, src []byte) ([]byte, error) {
	decOnce.Do(func() {
		var err error
		decoder, err = zstd.NewReader(nil)
		Check(err)
	})
	return decoder.DecodeAll(src, dst[:0])
}

// ZSTDCompress compresses a block using ZSTD algorithm. The compression level is
// resolved using zstd.EncoderLevelFromZstd, which maps an arbitrary integer onto
// one of a small number of encoder levels. A single encoder is cached per resolved
// level, so callers with different levels each get their own encoder.
func ZSTDCompress(dst, src []byte, compressionLevel int) ([]byte, error) {
	level := zstd.EncoderLevelFromZstd(compressionLevel)

	if enc, ok := encoders.Load(level); ok {
		return enc.(*zstd.Encoder).EncodeAll(src, dst[:0]), nil
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, err
	}
	actual, loaded := encoders.LoadOrStore(level, enc)
	if loaded {
		// We lost the race to populate this level; drop our duplicate encoder.
		_ = enc.Close()
		enc = actual.(*zstd.Encoder)
	}
	return enc.EncodeAll(src, dst[:0]), nil
}

// ZSTDCompressBound returns the worst case size needed for a destination buffer.
// Klauspost ZSTD library does not provide any API for Compression Bound. This
// calculation is based on the DataDog ZSTD library.
// See https://pkg.go.dev/github.com/DataDog/zstd#CompressBound
func ZSTDCompressBound(srcSize int) int {
	lowLimit := 128 << 10 // 128 kB
	var margin int
	if srcSize < lowLimit {
		margin = (lowLimit - srcSize) >> 11
	}
	return srcSize + (srcSize >> 8) + margin
}
