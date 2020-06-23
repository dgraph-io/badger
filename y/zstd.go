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

package y

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdDec *zstd.Decoder
	zstdEnc *zstd.Encoder

	zstdEncOnce, zstdDecOnce sync.Once
)

// ZSTDDecompress decompresses a block using ZSTD algorithm.
func ZSTDDecompress(dst, src []byte) ([]byte, error) {
	var err error
	zstdDecOnce.Do(func() {
		zstdDec, err = zstd.NewReader(nil)
		AssertTrue(err == nil)
	})
	return zstdDec.DecodeAll(src, dst[:0])
}

// ZSTDCompress compresses a block using ZSTD algorithm.
func ZSTDCompress(dst, src []byte, level int) ([]byte, error) {
	var err error
	zstdEncOnce.Do(func() {
		zstdEnc, err = zstd.NewWriter(
			nil, zstd.WithZeroFrames(true),
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
			zstd.WithEncoderCRC(false))

		AssertTrue(err == nil)
	})
	return zstdEnc.EncodeAll(src, dst), nil
}
