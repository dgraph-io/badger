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

package badger

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"
)

func farEnough(itrKey, key []byte) int {
	n := len(itrKey)
	m := len(key)
	if m > n {
		m = n
	}

	for i := 0; i < m; i++ {
		if itrKey[i] != key[i] {
			return m - i
		}
	}

	return 0

}

type ByteSliceArray [][]byte

// Implementing the sort.Interface for ByteSliceArray

// Len returns the length of the ByteSliceArray.
func (b ByteSliceArray) Len() int {
	return len(b)
}

// Less compares two byte arrays at given indices and returns true if the byte array at index i is less than the byte array at index j.
func (b ByteSliceArray) Less(i, j int) bool {
	return bytesLessThan(b[i], b[j])
}

// Swap swaps the byte arrays at given indices.
func (b ByteSliceArray) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// bytesLessThan compares two byte arrays lexicographically.
func bytesLessThan(a, b []byte) bool {
	return bytes.Compare(a, b) >= 0
}

func TestReadC(t *testing.T) {
	allKeysF, err := os.Open("/home/harshil/all_keys_2")
	require.NoError(t, err)
	defer allKeysF.Close()

	scanner := bufio.NewScanner(allKeysF)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	keysList := [][]byte{}
	for scanner.Scan() {
		f := strings.Fields(scanner.Text())
		b := []byte{}
		for _, c := range f {
			ic, err := strconv.Atoi(c)
			require.NoError(t, err)
			b = append(b, uint8(ic))
		}
		keysList = append(keysList, b)
	}

	dir := "/home/harshil/data/p/"
	opt := DefaultOptions(dir)
	opt.managedTxns = true
	opt.Compression = 0
	opt.IndexCacheSize = 0
	db, err := Open(opt)
	require.NoError(t, err)

	numCh := 64
	numPer := len(keysList) / numCh

	var wg sync.WaitGroup
	defer profile.Start(profile.CPUProfile).Stop()

	s := 0

	calculateS := func(start int) {
		m := 0

		for i := start * numPer; i < start*numPer+numPer; i += 1 {
			txn := db.NewTransactionAt(270005, false)

			key := keysList[i]
			item, err := txn.Get(key)
			require.NoError(t, err)

			item.Value(func(val []byte) error {
				m += len(val) + len(key)
				return nil
			})
			txn.Discard()
		}
		wg.Done()
		s += m
	}

	calculate := func(start int) {
		m := 0

		num := 500
		for i := start * numPer; i < start*numPer+numPer; i += num {
			txn := db.NewTransactionAt(270005, false)

			keys := ByteSliceArray{}
			for j := i; j < start*numPer+numPer && j < i+num; j++ {
				keys = append(keys, keysList[j])
			}
			sort.Sort(keys)
			items, err := txn.GetBatch(keys)
			require.NoError(t, err)

			for j, item := range items {
				item.Value(func(val []byte) error {
					m += len(val) + len(keys[j])
					return nil
				})
			}
			txn.Discard()
		}
		wg.Done()
		s += m
	}

	t1 := time.Now()
	for i := 0; i < numCh; i++ {
		wg.Add(1)
		go func(startPos int) {
			calculateS(startPos)
		}(i)
	}

	wg.Wait()
	fmt.Println(time.Since(t1), s)

	s = 0
	t1 = time.Now()
	for i := 0; i < numCh; i++ {
		wg.Add(1)
		go func(startPos int) {
			calculate(startPos)
		}(i)
	}

	wg.Wait()

	fmt.Println(time.Since(t1), s)
}

func TestDiscardStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	ds, err := InitDiscardStats(opt)
	require.NoError(t, err)
	require.Zero(t, ds.nextEmptySlot)
	fid, _ := ds.MaxDiscard()
	require.Zero(t, fid)

	for i := uint32(0); i < 20; i++ {
		require.Equal(t, int64(i*100), ds.Update(i, int64(i*100)))
	}
	ds.Iterate(func(id, val uint64) {
		require.Equal(t, id*100, val)
	})
	for i := uint32(0); i < 10; i++ {
		require.Equal(t, 0, int(ds.Update(i, -1)))
	}
	ds.Iterate(func(id, val uint64) {
		if id < 10 {
			require.Zero(t, val)
			return
		}
		require.Equal(t, int(id*100), int(val))
	})
}

func TestReloadDiscardStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	ds := db.vlog.discardStats

	ds.Update(uint32(1), 1)
	ds.Update(uint32(2), 1)
	ds.Update(uint32(1), -1)
	require.NoError(t, db.Close())

	// Reopen the DB, discard stats should be same.
	db2, err := Open(opt)
	require.NoError(t, err)
	ds2 := db2.vlog.discardStats
	require.Zero(t, ds2.Update(uint32(1), 0))
	require.Equal(t, 1, int(ds2.Update(uint32(2), 0)))
}
