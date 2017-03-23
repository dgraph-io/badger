package y

import (
	//	"fmt"
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

type SimpleIterator struct {
	keys [][]byte
	vals [][]byte
	idx  int
}

func (s *SimpleIterator) Next() {
	s.idx++
}

func (s *SimpleIterator) SeekToFirst() {
	s.idx = 0
}

func (s *SimpleIterator) Seek(key []byte) {
	s.idx = sort.Search(len(s.keys), func(i int) bool {
		return bytes.Compare(s.keys[i], key) >= 0
	})
}

func (s *SimpleIterator) KeyValue() ([]byte, []byte) {
	return s.keys[s.idx], s.vals[s.idx]
}

func (s *SimpleIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.keys)
}

func newSimpleIterator(keys []string, vals []string) *SimpleIterator {
	k := make([][]byte, len(keys))
	v := make([][]byte, len(vals))
	AssertTrue(len(keys) == len(vals))
	for i := 0; i < len(keys); i++ {
		k[i] = []byte(keys[i])
		v[i] = []byte(vals[i])
	}
	return &SimpleIterator{
		keys: k,
		vals: v,
		idx:  -1,
	}
}

func getAll(it Iterator) ([]string, []string) {
	var keys, vals []string
	for ; it.Valid(); it.Next() {
		k, v := it.KeyValue()
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	}
	return keys, vals
}

func TestSimpleIterator(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals)
	it.SeekToFirst()
	k, v := getAll(it)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeSingle(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals)
	mergeIt := NewMergeIterator([]Iterator{it})
	mergeIt.SeekToFirst()
	k, v := getAll(mergeIt)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeMore(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"})
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"})
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"})
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"})

	mergeIt := NewMergeIterator([]Iterator{it, it2, it3, it4})
	mergeIt.SeekToFirst()
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"1", "2", "3", "5", "7", "9"}, k)
	require.EqualValues(t, []string{
		"a1",
		"b2",
		"a3",
		"b5",
		"a7",
		"d9",
	}, v)
}

// Ensure MergeIterator satisfies the Iterator interface
func TestMergeIteratorNested(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals)
	mergeIt := NewMergeIterator([]Iterator{it})
	mergeIt2 := NewMergeIterator([]Iterator{mergeIt})
	mergeIt2.SeekToFirst()
	k, v := getAll(mergeIt2)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeIteratorSeek(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"})
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"})
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"})
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"})

	mergeIt := NewMergeIterator([]Iterator{it, it2, it3, it4})
	mergeIt.Seek([]byte("4"))
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"5", "7", "9"}, k)
	require.EqualValues(t, []string{
		"b5",
		"a7",
		"d9",
	}, v)
}

func TestMergeIteratorSeekInvalid(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"})
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"})
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"})
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"})

	mergeIt := NewMergeIterator([]Iterator{it, it2, it3, it4})
	mergeIt.Seek([]byte("f"))
	require.False(t, mergeIt.Valid())
}
