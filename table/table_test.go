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

package table

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func key(i int) string {
	return fmt.Sprintf("k%04d", i)
}

func buildTestTable(t *testing.T, n int) *os.File {
	y.AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues)
}

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	b := NewTableBuilder()
	defer b.Close()
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.
	f, err := ioutil.TempFile("/tmp", "table_")
	require.NoError(t, err)

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		require.NoError(t, b.Add([]byte(kv[0]), []byte(kv[1]), 'A'))
	}
	f.Write(b.Finish([]byte("somemetadata")))
	return f
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f, MemoryMap)
			require.NoError(t, err)
			defer table.DecrRef()
			require.EqualValues(t, "somemetadata", string(table.metadata))
			it := table.NewIterator()
			defer it.Close()
			it.SeekToFirst()
			require.True(t, it.Valid())
			v, meta := it.Value()
			require.EqualValues(t, "0", string(v))
			require.EqualValues(t, 'A', meta)
		})
	}
}

func TestSeekToLast(t *testing.T) {
	//	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
	for _, n := range []int{101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f, MemoryMap)
			require.NoError(t, err)
			defer table.DecrRef()
			it := table.NewIterator()
			defer it.Close()
			it.SeekToLast()
			require.True(t, it.Valid())
			v, meta := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-1), string(v))
			require.EqualValues(t, 'A', meta)
			it.Prev()
			require.True(t, it.Valid())
			v, meta = it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-2), string(v))
			require.EqualValues(t, 'A', meta)
		})
	}
}

func TestSeek(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f, MemoryMap)
	require.NoError(t, err)
	defer table.DecrRef()

	it := table.NewIterator()
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", true, "k0000"},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0101"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1235"},
		{"k9999", true, "k9999"},
		{"z", false, ""}, // Test case where every element is < input key.
	}

	for _, tt := range data {
		it.Seek([]byte(tt.in))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(k))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f, MemoryMap)
			require.NoError(t, err)
			defer table.DecrRef()
			ti := table.NewIterator()
			defer ti.Close()
			ti.Reset()
			ti.Seek([]byte(""))
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.Next() {
				v, meta := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v))
				require.EqualValues(t, 'A', meta)
				count++
			}
			require.EqualValues(t, n, count)
		})
	}
}

func TestIterateFromEnd(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f, Nothing)
			require.NoError(t, err)
			defer table.DecrRef()
			ti := table.NewIterator()
			defer ti.Close()
			ti.Reset()
			ti.Seek([]byte("zzzzzz")) // Seek to end, an invalid element.
			require.False(t, ti.Valid())

			for i := n - 1; i >= 0; i-- {
				ti.Prev()
				require.True(t, ti.Valid())
				v, meta := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", i), string(v))
				require.EqualValues(t, 'A', meta)
			}
		})
	}
}

func TestTable(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f, Nothing)
	require.NoError(t, err)
	defer table.DecrRef()
	ti := table.NewIterator()
	defer ti.Close()
	kid := 1010
	seek := []byte(key(kid))
	for ti.Seek(seek); ti.Valid(); ti.Next() {
		k := ti.Key()
		require.EqualValues(t, k, key(kid))
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.Seek([]byte(key(99999)))
	require.False(t, ti.Valid())

	ti.Seek([]byte(key(-1)))
	require.True(t, ti.Valid())
	k := ti.Key()
	require.EqualValues(t, k, key(0))
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f, MemoryMap)
	require.NoError(t, err)
	defer table.DecrRef()

	seek := []byte(key(1010))
	it := table.NewIterator()
	defer it.Close()
	it.Seek(seek)
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, seek, k)

	it.Prev()
	it.Prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key(1008), string(k))

	it.Next()
	it.Next()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key(1010), k)

	it.Seek([]byte(key(2000)))
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key(2000), k)

	it.Prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key(1999), k)

	it.SeekToFirst()
	k = it.Key()
	require.EqualValues(t, key(0), string(k))
}

func newConcatIterator(tables []*Table) y.Iterator {
	iters := make([]y.Iterator, 0, len(tables))
	for _, t := range tables {
		iters = append(iters, t.NewIterator())
	}
	return y.NewConcatIterator(iters)
}

// Try having only one table.
func TestConcatIteratorOneTable(t *testing.T) {
	f := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	tbl, err := OpenTable(f, MemoryMap)
	require.NoError(t, err)
	defer tbl.DecrRef()

	it := newConcatIterator([]*Table{tbl})
	defer it.Close()

	it.SeekToFirst()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	v, meta := it.Value()
	require.EqualValues(t, "a1", string(v))
	require.EqualValues(t, 'A', meta)
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, 10000)
	f2 := buildTestTable(t, 10000)
	tbl, err := OpenTable(f, MemoryMap)
	require.NoError(t, err)
	defer tbl.DecrRef()
	tbl2, err := OpenTable(f2, LoadToRAM)
	require.NoError(t, err)
	defer tbl2.DecrRef()

	it := newConcatIterator([]*Table{tbl, tbl2})
	defer it.Close()

	it.SeekToFirst()
	require.True(t, it.Valid())
	var count int
	for ; it.Valid(); it.Next() {
		v, meta := it.Value()
		require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(v))
		require.EqualValues(t, 'A', meta)
		count++
	}
	require.EqualValues(t, 20000, count)
}

func TestMergingIterator(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	tbl1, err := OpenTable(f1, LoadToRAM)
	require.NoError(t, err)
	defer tbl1.DecrRef()
	tbl2, err := OpenTable(f2, LoadToRAM)
	require.NoError(t, err)
	defer tbl2.DecrRef()
	it1 := tbl1.NewIterator()
	it2 := newConcatIterator([]*Table{tbl2})
	it := y.NewMergeIterator([]y.Iterator{it1, it2})
	defer it.Close()

	it.SeekToFirst()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	v, meta := it.Value()
	require.EqualValues(t, "a1", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
	v, meta = it.Value()
	require.EqualValues(t, "a2", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the first iterator.
func TestMergingIteratorTakeOne(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{})

	t1, err := OpenTable(f1, LoadToRAM)
	require.NoError(t, err)
	defer t1.DecrRef()
	t2, err := OpenTable(f2, LoadToRAM)
	require.NoError(t, err)
	defer t2.DecrRef()

	it1 := newConcatIterator([]*Table{t1})
	it2 := newConcatIterator([]*Table{t2})
	it := y.NewMergeIterator([]y.Iterator{it1, it2})
	defer it.Close()

	it.SeekToFirst()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	v, meta := it.Value()
	require.EqualValues(t, "a1", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
	v, meta = it.Value()
	require.EqualValues(t, "a2", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the second iterator.
func TestMergingIteratorTakeTwo(t *testing.T) {
	f1 := buildTable(t, [][]string{})
	f2 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	t1, err := OpenTable(f1, LoadToRAM)
	require.NoError(t, err)
	defer t1.DecrRef()
	t2, err := OpenTable(f2, LoadToRAM)
	require.NoError(t, err)
	defer t2.DecrRef()

	it1 := newConcatIterator([]*Table{t1})
	it2 := newConcatIterator([]*Table{t2})
	it := y.NewMergeIterator([]y.Iterator{it1, it2})
	defer it.Close()

	it.SeekToFirst()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	v, meta := it.Value()
	require.EqualValues(t, "a1", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
	v, meta = it.Value()
	require.EqualValues(t, "a2", string(v))
	require.EqualValues(t, 'A', meta)
	it.Next()

	require.False(t, it.Valid())
}
