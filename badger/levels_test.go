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

package badger

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *tableHandler {
	b := table.NewTableBuilder()
	defer b.Close()

	fileID, f := tempFile("/tmp")
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		require.NoError(t, b.Add([]byte(kv[0]), []byte(kv[1])))
	}
	f.Write(b.Finish())
	table, err := newTableHandler(fileID, f)

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
	c := newLevelsController(DefaultDBOptions)
	t0 := buildTable(t, [][]string{
		{"k2", "z2"},
		{"k22", "z22"},
		{"k5", "z5"},
	})
	defer t0.decrRef()

	t1a := buildTable(t, [][]string{
		{"k0", "v0"},
	})
	defer t1a.decrRef()

	t1b := buildTable(t, [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
	})
	defer t1b.decrRef()

	t1c := buildTable(t, [][]string{
		{"k3", "v3"},
		{"k4", "v4"},
	})
	defer t1c.decrRef()

	c.levels[0].replaceTables(0, 0, []*tableHandler{t0})
	c.levels[1].replaceTables(0, 0, []*tableHandler{t1a, t1b, t1c})
	c.doCompact(0) // Assume this happens before the compact workers start work.

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
