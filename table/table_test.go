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
	b := TableBuilder{}
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

	//	expectedSize := b.FinalSize()
	f.Write(b.Finish())
	//	fileInfo, err := f.Stat()
	//	require.NoError(t, err)
	//	require.EqualValues(t, fileInfo.Size(), expectedSize)
	// TODO: Enable this check after we figure out the discrepancy.
	return f
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f)
			require.NoError(t, err)
			it := table.NewIterator()
			it.SeekToFirst()
			require.True(t, it.Valid())
			it.KV(func(k, v []byte) {
				require.EqualValues(t, "0", string(v))
			})
		})
	}
}

func TestSeekToLast(t *testing.T) {
	//	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
	for _, n := range []int{101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f)
			require.NoError(t, err)
			it := table.NewIterator()
			it.SeekToLast()
			require.True(t, it.Valid())
			it.KV(func(k, v []byte) {
				require.EqualValues(t, fmt.Sprintf("%d", n-1), string(v))
			})
			it.Prev()
			require.True(t, it.Valid())
			it.KV(func(k, v []byte) {
				require.EqualValues(t, fmt.Sprintf("%d", n-2), string(v))
			})
		})
	}
}

func TestSeek(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f)
	require.NoError(t, err)

	it := table.NewIterator()

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
		it.Seek([]byte(tt.in), ORIGIN)
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		var hit bool
		it.KV(func(k, v []byte) {
			require.EqualValues(t, tt.out, string(k))
			hit = true
			// This KV interface is somewhat clumsy. Also, it potentially invokes an additional
			// function call where k, v have to be passed around anyway. And if there is an error,
			// the KV routine is not called. People might forget to check error.
		})
		require.True(t, hit)
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, n)
			table, err := OpenTable(f)
			require.NoError(t, err)

			ti := table.NewIterator()
			ti.Reset()
			ti.Seek([]byte(""), ORIGIN)
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.Next() {
				var hit bool
				ti.KV(func(k, v []byte) {
					require.EqualValues(t, fmt.Sprintf("%d", count), string(v))
					count++
					hit = true
				})
				require.True(t, hit)
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
			table, err := OpenTable(f)
			require.NoError(t, err)
			ti := table.NewIterator()
			ti.Reset()
			ti.Seek([]byte("zzzzzz"), ORIGIN) // Seek to end, an invalid element.
			require.False(t, ti.Valid())

			for i := n - 1; i >= 0; i-- {
				ti.Prev()
				require.True(t, ti.Valid())
				var hit bool
				ti.KV(func(k, v []byte) {
					require.EqualValues(t, fmt.Sprintf("%d", i), string(v))
					hit = true
				})
				require.True(t, hit)
			}
		})
	}
}

func TestTable(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f)
	require.NoError(t, err)

	ti := table.NewIterator()
	kid := 1010
	seek := []byte(key(kid))
	for ti.Seek(seek, 0); ti.Valid(); ti.Next() {
		ti.KV(func(k, v []byte) {
			require.EqualValues(t, k, key(kid))
		})
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.Seek([]byte(key(99999)), 0)
	require.False(t, ti.Valid())

	ti.Seek([]byte(key(-1)), 0)
	require.True(t, ti.Valid())

	ti.KV(func(k, v []byte) {
		require.EqualValues(t, k, key(0))
	})
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, 10000)
	table, err := OpenTable(f)
	require.NoError(t, err)

	seek := []byte(key(1010))
	it := table.NewIterator()
	it.Seek(seek, 0)
	require.True(t, it.Valid())
	it.KV(func(k, v []byte) {
		require.EqualValues(t, seek, k)
	})

	it.Prev()
	it.Prev()
	require.True(t, it.Valid())
	it.KV(func(k, v []byte) {
		require.EqualValues(t, key(1008), string(k))
	})

	it.Next()
	it.Next()
	require.True(t, it.Valid())
	it.KV(func(k, v []byte) {
		require.EqualValues(t, key(1010), k)
	})

	it.Seek([]byte(key(2000)), 1)
	require.True(t, it.Valid())
	it.KV(func(k, v []byte) {
		require.EqualValues(t, key(2000), k)
	})

	it.Prev()
	require.True(t, it.Valid())
	it.KV(func(k, v []byte) {
		require.EqualValues(t, key(1999), k)
	})

	it.SeekToFirst()
	it.KV(func(k, v []byte) {
		require.EqualValues(t, key(0), string(k))
	})
}

// Try having only one table.
func TestConcatIteratorOneTable(t *testing.T) {
	f := buildTable(t, [][]string{
		[]string{"k1", "a1"},
		[]string{"k2", "a2"},
	})

	tbl, err := OpenTable(f)
	require.NoError(t, err)

	it := NewConcatIterator([]*Table{tbl})
	require.True(t, it.Valid())
	k, v := it.KeyValue()
	require.EqualValues(t, "a1", string(v))
	require.EqualValues(t, "k1", string(k))
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, 10000)
	f2 := buildTestTable(t, 10000)
	tbl, err := OpenTable(f)
	require.NoError(t, err)
	tbl2, err := OpenTable(f2)
	require.NoError(t, err)
	it := NewConcatIterator([]*Table{tbl, tbl2})
	require.True(t, it.Valid())

	var count int
	for ; it.Valid(); it.Next() {
		_, v := it.KeyValue()
		require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(v))
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
	tbl1, err := OpenTable(f1)
	require.NoError(t, err)
	tbl2, err := OpenTable(f2)
	require.NoError(t, err)
	it1 := NewConcatIterator([]*Table{tbl1})
	it2 := NewConcatIterator([]*Table{tbl2})
	it := NewMergingIterator(it1, it2)

	require.True(t, it.Valid())
	k, v := it.KeyValue()
	require.EqualValues(t, "k1", string(k))
	require.EqualValues(t, "a1", string(v))
	it.Next()

	require.True(t, it.Valid())
	k, v = it.KeyValue()
	require.EqualValues(t, "k1", string(k))
	require.EqualValues(t, "b1", string(v))
	it.Next()

	require.True(t, it.Valid())
	k, v = it.KeyValue()
	require.EqualValues(t, "k2", string(k))
	require.EqualValues(t, "a2", string(v))
	it.Next()

	require.True(t, it.Valid())
	k, v = it.KeyValue()
	require.EqualValues(t, "k2", string(k))
	require.EqualValues(t, "b2", string(v))
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

	t1, err := OpenTable(f1)
	require.NoError(t, err)
	t2, err := OpenTable(f2)
	require.NoError(t, err)

	it1 := NewConcatIterator([]*Table{t1})
	it2 := NewConcatIterator([]*Table{t2})
	it := NewMergingIterator(it1, it2)

	require.True(t, it.Valid())
	k, v := it.KeyValue()
	require.EqualValues(t, "k1", string(k))
	require.EqualValues(t, "a1", string(v))
	it.Next()

	require.True(t, it.Valid())
	k, v = it.KeyValue()
	require.EqualValues(t, "k2", string(k))
	require.EqualValues(t, "a2", string(v))
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

	t1, err := OpenTable(f1)
	require.NoError(t, err)
	t2, err := OpenTable(f2)
	require.NoError(t, err)

	it1 := NewConcatIterator([]*Table{t1})
	it2 := NewConcatIterator([]*Table{t2})
	it := NewMergingIterator(it1, it2)

	require.True(t, it.Valid())
	k, v := it.KeyValue()
	require.EqualValues(t, "k1", string(k))
	require.EqualValues(t, "a1", string(v))
	it.Next()

	require.True(t, it.Valid())
	k, v = it.KeyValue()
	require.EqualValues(t, "k2", string(k))
	require.EqualValues(t, "a2", string(v))
	it.Next()

	require.False(t, it.Valid())
}
