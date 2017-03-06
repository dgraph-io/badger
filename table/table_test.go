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
	return fmt.Sprintf("%04d-%04d", i/37, i)
}

func buildTestTable(t *testing.T) *os.File {
	keyValues := make([][]string, 10000)
	for i := 0; i < 10000; i++ {
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
	f.Write(b.Finish())
	return f
}

func TestBuild(t *testing.T) {
	f := buildTestTable(t)
	table := Table{
		offset: 0,
		fd:     f,
	}
	require.NoError(t, table.ReadIndex())

	seek := []byte(key(1010))
	t.Logf("Seeking to: %q", seek)
	block, err := table.BlockForKey(seek)
	if err != nil {
		t.Fatalf("While getting iterator: %v", err)
	}

	fn := func(k, v []byte) {
		t.Logf("ITERATOR key=%q. val=%q.\n", k, v)
	}

	bi := block.NewIterator()
	for bi.Init(); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}
	fmt.Println("SEEKING")
	for bi.Seek(seek, 0); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}

	fmt.Println("SEEKING BACKWARDS")
	for bi.Seek(seek, 0); bi.Valid(); bi.Prev() {
		bi.KV(fn)
	}

	bi.Seek(seek, 0)
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, k, seek)
	})

	bi.Prev()
	bi.Prev()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1008))
	})
	bi.Next()
	bi.Next()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, k, seek)
	})

	for bi.Seek([]byte(key(2000)), 1); bi.Valid(); bi.Next() {
		t.Fatalf("This shouldn't be triggered.")
	}
	bi.Seek([]byte(key(1010)), 0)
	for bi.Seek([]byte(key(2000)), 1); bi.Valid(); bi.Prev() {
		t.Fatalf("This shouldn't be triggered.")
	}
	bi.Seek([]byte(key(2000)), 0)
	bi.Prev()
	require.True(t, bi.Valid(), "This should point to the last element in the block.")
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1099))
	})

	bi.Reset()
	bi.Prev()
	bi.Next()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1000))
	})

	bi.Seek([]byte(key(1001)), 0)
	bi.Prev()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1000))
	})
	bi.Prev()
	require.False(t, bi.Valid())
	bi.Next()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1000))
	})
	bi.Prev()
	require.False(t, bi.Valid())
	bi.Next()
	bi.KV(func(k, v []byte) {
		require.EqualValues(t, string(k), key(1000))
	})
}

func TestTable(t *testing.T) {
	f := buildTestTable(t)
	table := Table{
		offset: 0,
		fd:     f,
	}
	if err := table.ReadIndex(); err != nil {
		t.Error(err)
		t.Fail()
	}

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
	require.False(t, ti.Valid())

	ti.Next()
	require.True(t, ti.Valid())

	ti.KV(func(k, v []byte) {
		require.EqualValues(t, k, key(0))
	})
}

func TestIterateFromStart(t *testing.T) {
	f := buildTestTable(t)
	table := Table{
		offset: 0,
		fd:     f,
	}
	require.NoError(t, table.ReadIndex())
	ti := table.NewIterator()
	ti.Reset()
	ti.Seek([]byte(""), ORIGIN)
	ti.Next()

	var count int
	for ; ti.Valid(); ti.Next() {
		ti.KV(func(k, v []byte) {
			require.EqualValues(t, fmt.Sprintf("%d", count), string(v))
			count++
		})
	}
	require.EqualValues(t, 10000, count)
}

// Seek of table is a bit strange. Sometimes we need to Next. Sometimes we should not.
func TestSeekUnusual(t *testing.T) {
	f := buildTestTable(t)
	tbl := Table{fd: f}
	require.NoError(t, tbl.ReadIndex())

	it := tbl.NewIterator()
	it.Reset()
	it.Seek([]byte(""), ORIGIN) // Assume no such key.
	require.False(t, it.Valid())
	it.Next()
	it.KV(func(k, v []byte) {
		require.EqualValues(t, "0000-0000", string(k)) // First key.
		require.EqualValues(t, "0", string(v))
	})
	f.Close()

	// Now try a different setup.
	fCopy := buildTable(t, [][]string{{"0000-0000", "0"}})
	tblCopy := Table{fd: fCopy}
	require.NoError(t, tblCopy.ReadIndex())

	itCopy := tblCopy.NewIterator()
	itCopy.Reset()
	itCopy.Seek([]byte(""), ORIGIN) // Assume no such key.
	require.True(t, itCopy.Valid()) // Unlike the earlier case, Valid returns true!
	itCopy.KV(func(k, v []byte) {
		require.EqualValues(t, "0000-0000", string(k)) // First key.
		require.EqualValues(t, "0", string(v))
	})
	fCopy.Close()
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
	f := buildTestTable(t)
	f2 := buildTestTable(t)
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

//// Take only the first iterator.
//func TestMergingIteratorTwo(t *testing.T) {
//	f1 := buildTable(t, [][]string{
//		{"k1", "a1"},
//		{"k2", "a2"},
//	})
//	f2 := buildTable(t, [][]string{})
//	fname1, fname2 := f1.Name(), f2.Name()
//	f1.Close()
//	f2.Close()
//	it1 := NewConcatIterator([]string{fname1})
//	defer it1.Close()

//	it2 := NewConcatIterator([]string{fname2})
//	defer it2.Close()
//	it := NewMergingIterator(it1, it2)

//	require.True(t, it.Valid())
//	k, v := it.KeyValue()
//	require.EqualValues(t, "k1", string(k))
//	require.EqualValues(t, "a1", string(v))
//	it.Next()

//	require.True(t, it.Valid())
//	k, v = it.KeyValue()
//	require.EqualValues(t, "k2", string(k))
//	require.EqualValues(t, "a2", string(v))
//	it.Next()

//	require.False(t, it.Valid())
//}

//// Take only the second iterator.
//func TestMergingIteratorOne(t *testing.T) {
//	f1 := buildTable(t, [][]string{})
//	f2 := buildTable(t, [][]string{
//		{"k1", "a1"},
//		{"k2", "a2"},
//	})
//	fname1, fname2 := f1.Name(), f2.Name()
//	f1.Close()
//	f2.Close()
//	it1 := NewConcatIterator([]string{fname1})
//	defer it1.Close()

//	it2 := NewConcatIterator([]string{fname2})
//	defer it2.Close()
//	it := NewMergingIterator(it1, it2)

//	require.True(t, it.Valid())
//	k, v := it.KeyValue()
//	require.EqualValues(t, "k1", string(k))
//	require.EqualValues(t, "a1", string(v))
//	it.Next()

//	require.True(t, it.Valid())
//	k, v = it.KeyValue()
//	require.EqualValues(t, "k2", string(k))
//	require.EqualValues(t, "a2", string(v))
//	it.Next()

//	require.False(t, it.Valid())
//}
