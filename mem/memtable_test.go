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

package mem

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

func extract(m *Table) ([]string, []string) {
	var keys, vals []string
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		v := it.Value()
		if (value.BitDelete & v[0]) != 0 {
			vals = append(vals, "DEL")
		} else {
			vals = append(vals, string(v[1:]))
		}
	}
	return keys, vals
}

func TestBasic(t *testing.T) {
	m := NewTable()
	require.NotNil(t, m)
	m.Put([]byte("somekey"), []byte("hohoho"), 0)
	m.Put([]byte("somekey"), []byte("hahaha"), 0)
	k, v := extract(m)
	require.EqualValues(t, []string{"somekey"}, k)
	require.EqualValues(t, []string{"hahaha"}, v)

	m.Put([]byte("akey"), nil, value.BitDelete)
	m.Put([]byte("somekey"), nil, value.BitDelete)
	k, v = extract(m)
	require.EqualValues(t, []string{"akey", "somekey"}, k)
	require.EqualValues(t, []string{"DEL", "DEL"}, v)

	m.Put([]byte("somekey"), []byte("yes"), 0)
	k, v = extract(m)
	require.EqualValues(t, []string{"akey", "somekey"}, k)
	require.EqualValues(t, []string{"DEL", "yes"}, v)
}

func TestMemUssage(t *testing.T) {
	m := NewTable()
	for i := 0; i < 10000; i++ {
		m.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)), 0)
	}
	expected := 10000 * (6 + 6 + 1)
	require.InEpsilon(t, expected, m.MemUsage(), 0.1)
}

func TestMergeIterator(t *testing.T) {
	m := NewTable()
	it := m.NewIterator()
	mergeIt := y.NewMergeIterator([]y.Iterator{it})
	require.False(t, mergeIt.Valid())
}

// BenchmarkAdd-4   	 1000000	      1289 ns/op
func BenchmarkAdd(b *testing.B) {
	m := NewTable()
	for i := 0; i < b.N; i++ {
		m.Put([]byte(fmt.Sprintf("k%09d", i)), []byte(fmt.Sprintf("v%09d", i)), 0)
	}
}
