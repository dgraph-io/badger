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

package badger

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetMergeOperatorAddandGetSlices(t *testing.T) {
	// Merge function to merge two byte slices
	add := func(originalValue, newValue []byte) []byte {
		// We append original value to new value because the values
		// are retrieved in reverse order (Last insertion will be the first value)
		return append(newValue, originalValue...)
	}
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		m := db.GetMergeOperator([]byte("fooprefix"), add, 2*time.Millisecond)
		defer m.Stop()

		require.Nil(t, m.Add([]byte("1")))
		require.Nil(t, m.Add([]byte("2")))
		require.Nil(t, m.Add([]byte("3")))

		value, err := m.Get()
		require.Nil(t, err)
		require.Equal(t, "123", string(value))
	})
}

func TestMergeOperatorGetBeforeAdd(t *testing.T) {
	key := []byte("merge")
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		m := db.GetMergeOperator(key, add, 200*time.Millisecond)
		defer m.Stop()

		val, err := m.Get()
		require.Equal(t, ErrKeyNotFound, err)
		require.Nil(t, val)
	})
}

func TestMergeOperatorAddAndGet(t *testing.T) {
	key := []byte("merge")
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		m := db.GetMergeOperator(key, add, 200*time.Millisecond)
		defer m.Stop()

		err := m.Add(uint64ToBytes(1))
		require.NoError(t, err)
		m.Add(uint64ToBytes(2))
		require.NoError(t, err)
		m.Add(uint64ToBytes(3))
		require.NoError(t, err)

		res, err := m.Get()
		require.NoError(t, err)
		require.Equal(t, uint64(6), bytesToUint64(res))
	})
}

func TestMergeOperatorCompactBeforeGet(t *testing.T) {
	key := []byte("merge")
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		m := db.GetMergeOperator(key, add, 200*time.Millisecond)
		defer m.Stop()

		err := m.Add(uint64ToBytes(1))
		require.NoError(t, err)
		m.Add(uint64ToBytes(2))
		require.NoError(t, err)
		m.Add(uint64ToBytes(3))
		require.NoError(t, err)

		time.Sleep(250 * time.Millisecond) // wait for merge to happen

		res, err := m.Get()
		require.NoError(t, err)
		require.Equal(t, uint64(6), bytesToUint64(res))
	})
}

func TestMergeOperatorGetAfterStop(t *testing.T) {
	key := []byte("merge")
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		m := db.GetMergeOperator(key, add, 1*time.Second)

		err := m.Add(uint64ToBytes(1))
		require.NoError(t, err)
		m.Add(uint64ToBytes(2))
		require.NoError(t, err)
		m.Add(uint64ToBytes(3))
		require.NoError(t, err)

		m.Stop()
		res, err := m.Get()
		require.NoError(t, err)
		require.Equal(t, uint64(6), bytesToUint64(res))
	})
}

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Merge function to add two uint64 numbers
func add(existing, new []byte) []byte {
	return uint64ToBytes(bytesToUint64(existing) + bytesToUint64(new))
}
