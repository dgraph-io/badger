package memtable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestBasic(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)
	m.Add(123, y.ValueTypeValue, []byte("somekey"), []byte("hohoho"))
	m.Add(120, y.ValueTypeValue, []byte("somekey"), []byte("hohoho"))
}

func TestIterateAll(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)
	m.Add(123, y.ValueTypeValue, []byte("somekey"), []byte("hohoho1"))
	m.Add(120, y.ValueTypeValue, []byte("somekey"), []byte("hohoho3"))
	m.Add(200, y.ValueTypeValue, []byte("zzz"), []byte("hohoho4"))
	m.Add(123, y.ValueTypeDeletion, []byte("somekey"), []byte("hohoho2"))

	it := m.Iterator()
	it.SeekToFirst()

	require.True(t, it.Valid())
	key := it.Key()
	require.Len(t, key, 15)
	require.EqualValues(t, "somekey", key[:7])
	require.EqualValues(t, []byte{0, 0, 0, 0, 0, 0, 123, 1}, key[7:])
	require.EqualValues(t, "hohoho1", it.Value())

	it.Next()
	require.True(t, it.Valid())
	key = it.Key()
	require.Len(t, key, 15)
	require.EqualValues(t, "somekey", key[:7])
	require.EqualValues(t, []byte{0, 0, 0, 0, 0, 0, 123, 0}, key[7:])
	require.EqualValues(t, "hohoho2", it.Value())

	it.Next()
	require.True(t, it.Valid())
	key = it.Key()
	require.Len(t, key, 15)
	require.EqualValues(t, "somekey", key[:7])
	require.EqualValues(t, []byte{0, 0, 0, 0, 0, 0, 120, 1}, key[7:])
	require.EqualValues(t, "hohoho3", it.Value())

	it.Next()
	require.True(t, it.Valid())
	key = it.Key()
	require.Len(t, key, 11)
	require.EqualValues(t, "zzz", key[:3])
	require.EqualValues(t, []byte{0, 0, 0, 0, 0, 0, 200, 1}, key[3:])
	require.EqualValues(t, "hohoho4", it.Value())

	it.Next()
	require.False(t, it.Valid())
}

func TestGet(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)
	m.Add(223, y.ValueTypeValue, []byte("somekey"), []byte("hohoho"))
	m.Add(123, y.ValueTypeDeletion, []byte("somekey"), []byte("aaa"))
	m.Add(23, y.ValueTypeValue, []byte("somekey"), []byte("nonono"))
	m.Add(200, y.ValueTypeValue, []byte("abckey"), []byte("bbb"))

	v, hit := m.Get(y.NewLookupKey([]byte("abckey"), y.MaxSequenceNumber))
	require.EqualValues(t, "bbb", v)
	require.True(t, hit)

	// Try a key that looks like existing keys.
	v, hit = m.Get(y.NewLookupKey([]byte("somekeyaaa"), y.MaxSequenceNumber))
	require.Nil(t, v)
	require.False(t, hit)

	// Try different sequence numbers to get different values.
	v, hit = m.Get(y.NewLookupKey([]byte("somekey"), y.MaxSequenceNumber))
	require.EqualValues(t, "hohoho", v)
	require.True(t, hit)

	v, hit = m.Get(y.NewLookupKey([]byte("somekey"), 200))
	require.Nil(t, v)
	require.True(t, hit) // There's a hit but key has been deleted.

	v, hit = m.Get(y.NewLookupKey([]byte("somekey"), 100))
	require.EqualValues(t, "nonono", v)
	require.True(t, hit)
}

func TestMemUssage(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	for i := 0; i < 10000; i++ {
		m.Add(uint64(i), y.ValueTypeValue, []byte(fmt.Sprintf("k%05d", i)),
			[]byte(fmt.Sprintf("v%05d", i)))
	}
	expected := 10000 * (8 + 6 + 1 + 1 + 6)
	require.InEpsilon(t, expected, m.ApproximateMemoryUsage(), 0.1)
}

// BenchmarkAdd-4   	 1000000	      1320 ns/op
func BenchmarkAdd(b *testing.B) {
	m := NewMemtable(DefaultKeyComparator)
	for i := 0; i < b.N; i++ {
		m.Add(uint64(i), y.ValueTypeValue, []byte(fmt.Sprintf("k%09d", i)),
			[]byte(fmt.Sprintf("v%09d", i)))
	}
}
