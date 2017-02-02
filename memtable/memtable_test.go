package memtable

import (
	//	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestBasic(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)
	m.Add(123, y.TypeValue, []byte("somekey"), []byte("hohoho"))
	m.Add(120, y.TypeValue, []byte("somekey"), []byte("hohoho"))
}

func TestIterateAll(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)
	m.Add(123, y.TypeValue, []byte("somekey"), []byte("hohoho1"))
	m.Add(120, y.TypeValue, []byte("somekey"), []byte("hohoho3"))
	m.Add(200, y.TypeValue, []byte("zzz"), []byte("hohoho4"))
	m.Add(123, y.TypeDeletion, []byte("somekey"), []byte("hohoho2"))

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
