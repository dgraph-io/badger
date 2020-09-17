package skl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func newValue(v int) uint64 {
	return uint64(v)
}

const arenaSize = 1 << 20

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist(arenaSize)
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	l.Put("key1", val1)
	l.Put("key2", val2)
	l.Put("key3", val3)

	v := l.Get("key")
	require.True(t, v == 0)

	v = l.Get("key1")
	require.Equal(t, val1, v)
	// require.True(t, v.Value != nil)
	// require.EqualValues(t, "00042", string(v.Value))
	// require.EqualValues(t, 55, v.Meta)

	v = l.Get("key2")
	require.True(t, v == 52)

	v = l.Get("key3")
	require.Equal(t, val3, v)
	// require.True(t, v.Value != nil)
	// require.EqualValues(t, "00062", string(v.Value))
	// require.EqualValues(t, 57, v.Meta)

	l.Put("key3", val4)
	v = l.Get("key3")
	require.Equal(t, val4, v)
	// require.True(t, v.Value != nil)
	// require.EqualValues(t, "00072", string(v.Value))
	// require.EqualValues(t, 12, v.Meta)

}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	it := l.NewIterator()
	defer it.Close()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := n - 1; i >= 0; i-- {
		l.Put(fmt.Sprintf("%05d", i), uint64(i))
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v)
		it.Next()
	}
	require.False(t, it.Valid())
}
