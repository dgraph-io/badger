package db

import (
	//	"fmt"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *tableWrapper {
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
	table, err := table.OpenTable(f)
	require.NoError(t, err)
	return newTableWrapper(f, table)
}

func extractTable(table *tableWrapper) [][]string {
	var out [][]string
	it := table.table.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		it.KV(func(k, v []byte) {
			out = append(out, []string{string(k), string(v)})
		})
	}
	return out
}

// TestCompact tests most basic compaction logic.
func TestCompact(t *testing.T) {
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

	levels[0].tables = []*tableWrapper{t0}
	levels[1].tables = []*tableWrapper{t1a, t1b, t1c}
	levels[0].compact(0)

	require.Len(t, levels[1].tables, 2)
	require.Empty(t, levels[0].tables)

	levels[1].sortTables()

	require.EqualValues(t, [][]string{
		{"k0", "v0"},
	}, extractTable(levels[1].tables[0]))

	require.EqualValues(t, [][]string{
		{"k1", "v1"},
		{"k2", "z2"},
		{"k22", "z22"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "z5"},
	}, extractTable(levels[1].tables[1]))
}
