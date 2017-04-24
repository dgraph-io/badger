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

var (
	closeCount int
)

func (s *SimpleIterator) Close() {
	closeCount++
}

func (s *SimpleIterator) Name() string { return "SimpleIterator" }

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

func (s *SimpleIterator) Key() []byte {
	return s.keys[s.idx]
}

func (s *SimpleIterator) Value() ([]byte, byte) {
	return s.vals[s.idx], 55
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
		k := it.Key()
		keys = append(keys, string(k))
		v, _ := it.Value()
		vals = append(vals, string(v))
	}
	return keys, vals
}

func closeAndCheck(t *testing.T, it Iterator, expected int) {
	closeCount = 0
	it.Close()
	require.EqualValues(t, expected, closeCount)
}

func TestSimpleIterator(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals)
	it.SeekToFirst()
	k, v := getAll(it)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)

	closeAndCheck(t, it, 1)
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

	closeAndCheck(t, mergeIt, 1)
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
	require.EqualValues(t, []string{"a1", "b2", "a3", "b5", "a7", "d9"}, v)

	closeAndCheck(t, mergeIt, 4)
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

	closeAndCheck(t, mergeIt2, 1)
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
	require.EqualValues(t, []string{"b5", "a7", "d9"}, v)

	closeAndCheck(t, mergeIt, 4)
}

func TestMergeIteratorSeekInvalid(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"})
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"})
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"})
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"})

	mergeIt := NewMergeIterator([]Iterator{it, it2, it3, it4})
	mergeIt.Seek([]byte("f"))
	require.False(t, mergeIt.Valid())
	closeAndCheck(t, mergeIt, 4)
}
