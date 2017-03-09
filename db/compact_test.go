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

	lvlsController.levels[0].tables = []*tableHandler{t0}
	lvlsController.levels[1].tables = []*tableHandler{t1a, t1b, t1c}
	lvlsController.doCompact(0)

	require.Len(t, lvlsController.levels[1].tables, 2)
	require.Empty(t, lvlsController.levels[0].tables)

	require.EqualValues(t, [][]string{
		{"k0", "v0"},
	}, extractTable(lvlsController.levels[1].tables[0]))

	require.EqualValues(t, [][]string{
		{"k1", "v1"},
		{"k2", "z2"},
		{"k22", "z22"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "z5"},
	}, extractTable(lvlsController.levels[1].tables[1]))
}

func TestCompactBasic(t *testing.T) {

}
