/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCallocBuffer(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var bytesBuffer bytes.Buffer // This is just for verifying result.
	bytesBuffer.Grow(512)

	cBuffer := NewBuffer(512)

	// Writer small []byte
	var smallBytes [256]byte
	rand.Read(smallBytes[:])
	var bigBytes [1024]byte
	rand.Read(bigBytes[:])

	_, err := cBuffer.Write(smallBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")
	_, err = cBuffer.Write(bigBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")

	// Write data to bytesBuffer also, just to match result.
	bytesBuffer.Write(smallBytes[:])
	bytesBuffer.Write(bigBytes[:])

	require.True(t, bytes.Equal(cBuffer.Bytes(), bytesBuffer.Bytes()))
}

func TestCallocBufferWrite(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [128]byte
	rand.Read(wb[:])

	cb := NewBuffer(32)
	bb := new(bytes.Buffer)

	end := 32
	for i := 0; i < 3; i++ {
		n, err := cb.Write(wb[:end])
		require.NoError(t, err, "unable to write bytes to buffer")
		require.Equal(t, n, end, "length of buffer and length written should be equal")

		// append to bb also for testing.
		bb.Write(wb[:end])

		require.True(t, bytes.Equal(cb.Bytes(), bb.Bytes()), "Both bytes should match")
		end = end * 2
	}
}

func TestSliceAlloc(t *testing.T) {
	var buf Buffer
	count := 10000
	expectedSlice := make([][]byte, 0, count)

	// Create "count" number of slices.
	for i := 0; i < count; i++ {
		sz := rand.Intn(1000)
		testBuf := make([]byte, sz)
		rand.Read(testBuf)

		newSlice := buf.SliceAllocate(sz)
		require.Equal(t, sz, copy(newSlice, testBuf))

		// Save testBuf for verification.
		expectedSlice = append(expectedSlice, testBuf)
	}

	offsets := buf.SliceOffsets(nil)
	require.Equal(t, len(expectedSlice), len(offsets))
	for i, off := range offsets {
		// All the slices returned by the buffer should be equal to what we
		// inserted earlier.
		require.Equal(t, expectedSlice[i], buf.Slice(off))
	}
}
