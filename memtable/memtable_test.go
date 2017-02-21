package memtable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	//	"github.com/dgraph-io/badger/y"
)

func extract(m *Memtable) ([]string, []string) {
	var keys, vals []string
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		v := it.Value()
		if IsDeleted(v) {
			vals = append(vals, "DEL")
		} else {
			vals = append(vals, string(v[1:]))
		}
	}
	return keys, vals
}

func TestBasic(t *testing.T) {
	m := NewMemtable()
	require.NotNil(t, m)
	m.Put([]byte("somekey"), []byte("hohoho"))
	m.Put([]byte("somekey"), []byte("hahaha"))
	k, v := extract(m)
	require.EqualValues(t, []string{"somekey"}, k)
	require.EqualValues(t, []string{"hahaha"}, v)

	m.Delete([]byte("akey"))
	m.Delete([]byte("somekey"))
	k, v = extract(m)
	require.EqualValues(t, []string{"akey", "somekey"}, k)
	require.EqualValues(t, []string{"DEL", "DEL"}, v)

	m.Put([]byte("somekey"), []byte("yes"))
	k, v = extract(m)
	require.EqualValues(t, []string{"akey", "somekey"}, k)
	require.EqualValues(t, []string{"DEL", "yes"}, v)
}

func TestMemUssage(t *testing.T) {
	m := NewMemtable()
	for i := 0; i < 10000; i++ {
		m.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)))
	}
	expected := 10000 * (6 + 6 + 1)
	require.InEpsilon(t, expected, m.MemUsage(), 0.1)
}

// BenchmarkAdd-4   	 1000000	      1289 ns/op
func BenchmarkAdd(b *testing.B) {
	m := NewMemtable()
	for i := 0; i < b.N; i++ {
		m.Put([]byte(fmt.Sprintf("k%09d", i)), []byte(fmt.Sprintf("v%09d", i)))
	}
}
