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
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

const (
	KB = 1024
	MB = KB * 1024
)

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	y.AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues)
}

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	b := NewTableBuilder()
	defer b.Close()
	rand.Seed(time.Now().UnixNano())
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.

	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		b.Add(y.KeyWithTs([]byte(kv[0]), 0), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: 0})
	}
	f.Write(b.Finish())
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	return f
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
			require.NoError(t, err)
			defer table.DecrRef()
			it := table.NewIterator(false)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Value()
				k := y.KeyWithTs([]byte(key("key", count)), 0)
				require.EqualValues(t, k, it.Key())
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
		})
	}
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
			require.NoError(t, err)
			defer table.DecrRef()
			it := table.NewIterator(false)
			defer it.Close()
			it.seekToFirst()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, "0", string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeekToLast(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
			require.NoError(t, err)
			defer table.DecrRef()
			it := table.NewIterator(false)
			defer it.Close()
			it.seekToLast()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-1), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			it.prev()
			require.True(t, it.Valid())
			v = it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-2), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeek(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer table.DecrRef()

	it := table.NewIterator(false)
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
		{"z", false, ""},
	}

	for _, tt := range data {
		it.seek(y.KeyWithTs([]byte(tt.in), 0))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(y.ParseKey(k)))
	}
}

func TestSeekForPrev(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer table.DecrRef()

	it := table.NewIterator(false)
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", false, ""},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0100"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1234"},
		{"k9999", true, "k9999"},
		{"z", true, "k9999"},
	}

	for _, tt := range data {
		it.seekForPrev(y.KeyWithTs([]byte(tt.in), 0))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(y.ParseKey(k)))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
			require.NoError(t, err)
			defer table.DecrRef()
			ti := table.NewIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seekToFirst()
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.next() {
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
				count++
			}
			require.EqualValues(t, n, count)
		})
	}
}

func TestIterateFromEnd(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTable(f, options.FileIO, options.OnTableAndBlockRead)
			require.NoError(t, err)
			defer table.DecrRef()
			ti := table.NewIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seek(y.KeyWithTs([]byte("zzzzzz"), 0)) // Seek to end, an invalid element.
			require.False(t, ti.Valid())
			for i := n - 1; i >= 0; i-- {
				ti.prev()
				require.True(t, ti.Valid())
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", i), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
			}
			ti.prev()
			require.False(t, ti.Valid())
		})
	}
}

func TestTable(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.FileIO, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer table.DecrRef()
	ti := table.NewIterator(false)
	defer ti.Close()
	kid := 1010
	seek := y.KeyWithTs([]byte(key("key", kid)), 0)
	for ti.seek(seek); ti.Valid(); ti.next() {
		k := ti.Key()
		require.EqualValues(t, string(y.ParseKey(k)), key("key", kid))
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.seek(y.KeyWithTs([]byte(key("key", 99999)), 0))
	require.False(t, ti.Valid())

	ti.seek(y.KeyWithTs([]byte(key("key", -1)), 0))
	require.True(t, ti.Valid())
	k := ti.Key()
	require.EqualValues(t, string(y.ParseKey(k)), key("key", 0))
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer table.DecrRef()

	seek := y.KeyWithTs([]byte(key("key", 1010)), 0)
	it := table.NewIterator(false)
	defer it.Close()
	it.seek(seek)
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, seek, k)

	it.prev()
	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1008), string(y.ParseKey(k)))

	it.next()
	it.next()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1010), y.ParseKey(k))

	it.seek(y.KeyWithTs([]byte(key("key", 2000)), 0))
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 2000), y.ParseKey(k))

	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1999), y.ParseKey(k))

	it.seekToFirst()
	k = it.Key()
	require.EqualValues(t, key("key", 0), y.ParseKey(k))
}

func TestUniIterator(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer table.DecrRef()
	{
		it := table.NewIterator(false)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
	}
	{
		it := table.NewIterator(true)
		defer it.Close()
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-1-count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
	}
}

// Try having only one table.
func TestConcatIteratorOneTable(t *testing.T) {
	f := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	tbl, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl.DecrRef()

	it := NewConcatIterator([]*Table{tbl}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, "keya", 10000)
	f2 := buildTestTable(t, "keyb", 10000)
	f3 := buildTestTable(t, "keyc", 10000)
	tbl, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl.DecrRef()
	tbl2, err := OpenTable(f2, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl2.DecrRef()
	tbl3, err := OpenTable(f3, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl3.DecrRef()

	{
		it := NewConcatIterator([]*Table{tbl, tbl2, tbl3}, false)
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek(y.KeyWithTs([]byte("a"), 0))
		require.EqualValues(t, "keya0000", string(y.ParseKey(it.Key())))
		vs := it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb"), 0))
		require.EqualValues(t, "keyb0000", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb9999b"), 0))
		require.EqualValues(t, "keyc0000", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyd"), 0))
		require.False(t, it.Valid())
	}
	{
		it := NewConcatIterator([]*Table{tbl, tbl2, tbl3}, true)
		defer it.Close()
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-(count%10000)-1), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek(y.KeyWithTs([]byte("a"), 0))
		require.False(t, it.Valid())

		it.Seek(y.KeyWithTs([]byte("keyb"), 0))
		require.EqualValues(t, "keya9999", string(y.ParseKey(it.Key())))
		vs := it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyb9999b"), 0))
		require.EqualValues(t, "keyb9999", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek(y.KeyWithTs([]byte("keyd"), 0))
		require.EqualValues(t, "keyc9999", string(y.ParseKey(it.Key())))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))
	}
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
	tbl1, err := OpenTable(f1, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl1.DecrRef()
	tbl2, err := OpenTable(f2, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl2.DecrRef()
	it1 := tbl1.NewIterator(false)
	it2 := NewConcatIterator([]*Table{tbl2}, false)
	it := y.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

func TestMergingIteratorReversed(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	tbl1, err := OpenTable(f1, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl1.DecrRef()
	tbl2, err := OpenTable(f2, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer tbl2.DecrRef()
	it1 := tbl1.NewIterator(true)
	it2 := NewConcatIterator([]*Table{tbl2}, true)
	it := y.NewMergeIterator([]y.Iterator{it1, it2}, true)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
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

	t1, err := OpenTable(f1, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer t1.DecrRef()
	t2, err := OpenTable(f2, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer t2.DecrRef()

	it1 := NewConcatIterator([]*Table{t1}, false)
	it2 := NewConcatIterator([]*Table{t2}, false)
	it := y.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
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

	t1, err := OpenTable(f1, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer t1.DecrRef()
	t2, err := OpenTable(f2, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err)
	defer t2.DecrRef()

	it1 := NewConcatIterator([]*Table{t1}, false)
	it2 := NewConcatIterator([]*Table{t2}, false)
	it := y.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(y.ParseKey(k)))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(y.ParseKey(k)))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()
	require.False(t, it.Valid())
}

func TestTableBigValues(t *testing.T) {
	value := func(i int) []byte {
		return []byte(fmt.Sprintf("%01048576d", i)) // Return 1MB value which is > math.MaxUint16.
	}

	rand.Seed(time.Now().UnixNano())
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	require.NoError(t, err, "unable to create file")

	n := 100 // Insert 100 keys.
	builder := NewTableBuilder()
	for i := 0; i < n; i++ {
		key := y.KeyWithTs([]byte(key("", i)), 0)
		vs := y.ValueStruct{Value: value(i)}
		builder.Add(key, vs)
	}

	f.Write(builder.Finish())
	tbl, err := OpenTable(f, options.LoadToRAM, options.OnTableAndBlockRead)
	require.NoError(t, err, "unable to open table")
	defer tbl.DecrRef()

	itr := tbl.NewIterator(false)
	require.True(t, itr.Valid())

	count := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		require.Equal(t, []byte(key("", count)), y.ParseKey(itr.Key()), "keys are not equal")
		require.Equal(t, value(count), itr.Value().Value, "values are not equal")
		count++
	}
	require.False(t, itr.Valid(), "table iterator should be invalid now")
	require.Equal(t, n, count)
}

// This test is for verifying checksum failure during table open.
func TestTableChecksum(t *testing.T) {
	rand.Seed(time.Now().Unix())
	// we are going to write random byte at random location in table file.
	rb := make([]byte, 1)
	rand.Read(rb)
	f := buildTestTable(t, "k", 10000)
	fi, err := f.Stat()
	require.NoError(t, err, "unable to get file information")
	f.WriteAt(rb, rand.Int63n(fi.Size()))

	_, err = OpenTable(f, options.LoadToRAM, options.OnTableAndBlockRead)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatal("Test should have been failed with checksum mismatch error")
	}
}

func BenchmarkRead(b *testing.B) {
	n := int(5 * 1e6)
	tbl := getTableForBenchmarks(b, n)
	defer tbl.DecrRef()

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.seekToFirst(); it.Valid(); it.next() {
			}
		}()
	}
}

func BenchmarkReadAndBuild(b *testing.B) {
	n := int(5 * 1e6)
	tbl := getTableForBenchmarks(b, n)
	defer tbl.DecrRef()

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			newBuilder := NewTableBuilder()
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.seekToFirst(); it.Valid(); it.next() {
				vs := it.Value()
				newBuilder.Add(it.Key(), vs)
			}
			newBuilder.Finish()
		}()
	}
}

func BenchmarkReadMerged(b *testing.B) {
	n := int(5 * 1e6)
	m := 5 // Number of tables.
	y.AssertTrue((n % m) == 0)
	tableSize := n / m
	var tables []*Table
	for i := 0; i < m; i++ {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
		builder := NewTableBuilder()
		f, err := y.OpenSyncedFile(filename, true)
		y.Check(err)
		for j := 0; j < tableSize; j++ {
			id := j*m + i // Arrays are interleaved.
			// id := i*tableSize+j (not interleaved)
			k := fmt.Sprintf("%016x", id)
			v := fmt.Sprintf("%d", id)
			builder.Add([]byte(k), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: 0})
		}
		f.Write(builder.Finish())
		tbl, err := OpenTable(f, options.LoadToRAM, options.OnTableAndBlockRead)
		y.Check(err)
		tables = append(tables, tbl)
		defer tbl.DecrRef()
	}

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			var iters []y.Iterator
			for _, tbl := range tables {
				iters = append(iters, tbl.NewIterator(false))
			}
			it := y.NewMergeIterator(iters, false)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
			}
		}()
	}
}

func BenchmarkChecksum(b *testing.B) {
	keySz := []int{KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB, 128 * KB, 256 * KB, MB}
	for _, kz := range keySz {
		key := make([]byte, kz)
		b.Run(fmt.Sprintf("CRC %d", kz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				crc32.ChecksumIEEE(key)
			}
		})
		b.Run(fmt.Sprintf("xxHash64 %d", kz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				xxhash.Sum64(key)
			}
		})
		b.Run(fmt.Sprintf("SHA256 %d", kz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sha256.Sum256(key)
			}
		})
	}
}

func BenchmarkRandomRead(b *testing.B) {
	n := int(5 * 1e6)
	tbl := getTableForBenchmarks(b, n)
	defer tbl.DecrRef()

	r := rand.New(rand.NewSource(time.Now().Unix()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		itr := tbl.NewIterator(false)
		no := r.Intn(n)
		k := []byte(fmt.Sprintf("%016x", no))
		v := []byte(fmt.Sprintf("%d", no))
		itr.Seek(k)
		if !itr.Valid() {
			b.Fatal("itr should be valid")
		}
		v1 := itr.Value().Value

		if !bytes.Equal(v, v1) {
			fmt.Println("value does not match")
			b.Fatal()
		}
		itr.Close()
	}
}

func getTableForBenchmarks(b *testing.B, count int) *Table {
	rand.Seed(time.Now().Unix())
	builder := NewTableBuilder()
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	require.NoError(b, err)
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)
		builder.Add([]byte(k), y.ValueStruct{Value: []byte(v)})
	}

	f.Write(builder.Finish())
	tbl, err := OpenTable(f, options.LoadToRAM, options.NoVerification)
	require.NoError(b, err, "unable to open table")
	return tbl
}
