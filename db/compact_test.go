/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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

package db

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"testing"
	//	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *tableHandler {
	b := table.TableBuilder{}
	b.Reset()
	f, err := ioutil.TempFile("", "badger")
	require.NoError(t, err)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		require.NoError(t, b.Add([]byte(kv[0]), []byte(kv[1])))
	}
	f.Write(b.Finish())
	table, err := newTableHandler(f)
	require.NoError(t, err)
	return table
}

func extractTable(table *tableHandler) [][]string {
	var out [][]string
	it := table.table.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		it.KV(func(k, v []byte) {
			out = append(out, []string{string(k), string(v)})
		})
	}
	return out
}

// TestDoCompact tests the merging logic which is done in internal doCompact function.
// We might remove this internal test eventually.
func TestDoCompact(t *testing.T) {
	c := newLevelsController(DefaultCompactOptions())
	t0 := buildTable(t, [][]string{
		{"k2", "z2"},
		{"k22", "z22"},
		{"k5", "z5"},
	})
	t1a := buildTable(t, [][]string{
		{"k0", "v0"},
	})
	t1b := buildTable(t, [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
	})
	t1c := buildTable(t, [][]string{
		{"k3", "v3"},
		{"k4", "v4"},
	})

	// Very low-level setup to do this low-level test.
	c.levels[0].tables = []*tableHandler{t0}
	c.levels[1].tables = []*tableHandler{t1a, t1b, t1c}
	c.doCompact(0)

	require.Len(t, c.levels[1].tables, 2)
	require.Empty(t, c.levels[0].tables)

	require.EqualValues(t, [][]string{
		{"k0", "v0"},
	}, extractTable(c.levels[1].tables[0]))

	require.EqualValues(t, [][]string{
		{"k1", "v1"},
		{"k2", "z2"},
		{"k22", "z22"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "z5"},
	}, extractTable(c.levels[1].tables[1]))
}

func randomKey() string {
	return fmt.Sprintf("%09d", rand.Uint32()%10000000)
}

func TestCompactBasic(t *testing.T) {
	n := 200 // Vary these settings. Be sure to try n being non-multiples of 100.
	opt := CompactOptions{
		NumLevelZeroTables: 5,
		LevelOneSize:       5 << 14,
		MaxLevels:          4,
		NumCompactWorkers:  3,
		MaxTableSize:       1 << 14,
	}
	c := newLevelsController(opt)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		keyValues[i] = []string{"", ""}
	}
	for {
		// Keep writing random keys to level 0.
		for i := 0; i < n; i++ {
			k := randomKey()
			keyValues[i][0] = k
			keyValues[i][1] = k
		}
		tbl := buildTable(t, keyValues)
		c.addLevel0Table(tbl)

		// Ensure that every level makes sense.
		for _, level := range c.levels {
			level.check()
		}
	}
}

func TestGet(t *testing.T) {
	n := 200 // Vary these settings. Be sure to try n being non-multiples of 100.
	opt := CompactOptions{
		NumLevelZeroTables: 5,
		LevelOneSize:       5 << 14,
		MaxLevels:          4,
		NumCompactWorkers:  3,
		MaxTableSize:       1 << 14,
	}
	c := newLevelsController(opt)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		keyValues[i] = []string{"", ""}
	}
	for j := 0; j < 10; j++ {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%05d_%05d", j, i)
			v := fmt.Sprintf("v%05d_%05d", j, i)
			keyValues[i][0] = k
			keyValues[i][1] = v
		}
		tbl := buildTable(t, keyValues)
		c.addLevel0Table(tbl)
	}
	require.Nil(t, c.get([]byte("abc")))
	require.EqualValues(t, "v00002_00123", c.get([]byte("00002_00123")))
	// Overwrite.
	for j := 0; j < 10; j++ {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("%05d_%05d", j, i)
			v := fmt.Sprintf("z%05d_%05d", j, i)
			keyValues[i][0] = k
			keyValues[i][1] = v
		}
		tbl := buildTable(t, keyValues)
		c.addLevel0Table(tbl)
	}
	require.Nil(t, c.get([]byte("abc")))
	require.EqualValues(t, "z00002_00123", c.get([]byte("00002_00123")))
}
