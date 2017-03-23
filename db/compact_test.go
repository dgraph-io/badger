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
	"time"

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
		k, v := it.KeyValue()
		out = append(out, []string{string(k), string(v)})
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

// Not really a test! Just run with -v and leave it running as a "stress test".
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

// Try iterating over a level as we compact. Check that we don't skip any keys.
// WARNING: Test might be flaky!
func TestLevelIterator(t *testing.T) {
	n := 200 // Vary these settings. Be sure to try n being non-multiples of 100.
	opt := CompactOptions{
		NumLevelZeroTables: 5,
		LevelOneSize:       5 << 14,
		MaxLevels:          2, // Very few levels so that everything is on level 0 or 1.
		NumCompactWorkers:  3,
		MaxTableSize:       1 << 14,
	}
	c := newLevelsController(opt)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		keyValues[i] = []string{"", ""}
	}

	// Populate around 3 levels.
	for j := 0; j < 50; j++ {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%05d_%05d", j, i)
			v := fmt.Sprintf("v%05d_%05d", j, i)
			keyValues[i][0] = k
			keyValues[i][1] = v
		}
		tbl := buildTable(t, keyValues)
		c.addLevel0Table(tbl)
	}

	// Force level 0 to all compact to level 1. Everything should be on level 1 now. A bit hackish.
	c.opt.NumLevelZeroTables = 0
	time.Sleep(5 * time.Second) // Wait a while for level 0 to be completely moved to level 1.

	// Rewrite all the values while we iterate over level 1.
	levelIter := c.levels[1].NewIterator()
	require.False(t, levelIter.Valid())
	levelIter.Seek([]byte("k00005_00003")) // SEEK.
	require.True(t, levelIter.Valid())
	itKey, itVal := levelIter.KeyValue()
	require.EqualValues(t, "k00005_00003", string(itKey))
	require.EqualValues(t, "v00005_00003", string(itVal))

	// Allow level 0 to have stuff again.
	c.opt.NumLevelZeroTables = 5

	// While iterating, we will push some stuff.
	var hasOverwritten, hasFinishedOverwriting bool
	go func() {
		// Replace from the back so that the forward iterating will "clash" with this.
		for j := 49; j >= 0; j-- {
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("k%05d_%05d", j, i)
				v := fmt.Sprintf("z%05d_%05d", j, i) // New value.
				keyValues[i][0] = k
				keyValues[i][1] = v
			}
			tbl := buildTable(t, keyValues)
			c.addLevel0Table(tbl)
			hasOverwritten = true
		}
		hasFinishedOverwriting = true
	}()
	// Give the above a bit of time.
	time.Sleep(100 * time.Millisecond)
	require.True(t, hasOverwritten)
	require.False(t, hasFinishedOverwriting)

	// We expect to see a mix of old and new values.
	// But all the keys should be available and in ascending order.
	var numKeys int
	var lastKey string
	var hasOldVal, hasNewVal bool
	for ; levelIter.Valid(); levelIter.Next() {
		keyBytes, valBytes := levelIter.KeyValue()
		key := string(keyBytes)
		val := string(valBytes)
		require.True(t, lastKey < key)
		if val[0] == 'v' {
			hasOldVal = true
		} else if val[0] == 'z' {
			hasNewVal = true
		}
		//		fmt.Printf("%s %s\n", key, val)
		lastKey = key
		numKeys++
	}
	// Rather delicate test with some timing hacks...
	// Make sure while iterating, we have started overwriting but NOT finished overwriting.
	require.True(t, hasOverwritten)
	require.False(t, hasFinishedOverwriting)

	// Make sure counts is right.
	require.EqualValues(t, n*45-3, numKeys)
	// Make sure we see a mix of old and new values.
	require.True(t, hasOldVal)
	require.True(t, hasNewVal)
}

func TestMergeLevelIterator(t *testing.T) {
	n := 200 // Vary these settings. Be sure to try n being non-multiples of 100.
	opt := CompactOptions{
		NumLevelZeroTables: 5,
		LevelOneSize:       5 << 14,
		MaxLevels:          3, // Very few levels so that everything is on level 0 or 1.
		NumCompactWorkers:  3,
		MaxTableSize:       1 << 14,
	}
	c := newLevelsController(opt)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		keyValues[i] = []string{"", ""}
	}

	for j := 0; j < 50; j++ {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%05d_%05d", j, i)
			v := fmt.Sprintf("v%05d_%05d", j, i)
			keyValues[i][0] = k
			keyValues[i][1] = v
		}
		tbl := buildTable(t, keyValues)
		c.addLevel0Table(tbl)
	}

	it0 := c.levels[0].NewIterator()
	it1 := c.levels[1].NewIterator()
	it2 := c.levels[2].NewIterator()
	it := y.NewMergeIterator([]y.Iterator{it0, it1, it2})
	var lastKey string
	var count int
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, _ := it.KeyValue()
		key := string(k)
		require.True(t, lastKey <= key)
		lastKey = key
		count++
	}
	// The compaction here doesn't remove any entry. The count should be the number of items added.
	require.EqualValues(t, n*50, count)
}
