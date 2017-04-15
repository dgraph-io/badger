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
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/value"
)

func extract(m *Table) ([]string, []string) {
	var keys, vals []string
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		v, meta := it.Value()
		if (value.BitDelete & meta) != 0 {
			vals = append(vals, "DEL")
		} else {
			vals = append(vals, string(v))
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

func TestSize(t *testing.T) {
	m := NewTable()
	for i := 0; i < 10000; i++ {
		m.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)), 0)
	}
	expected := 10000 * (6 + 6 + 1)
	require.InEpsilon(t, expected, m.Size(), 0.1)
}

func TestConcurrentWrite(t *testing.T) {
	tbl := NewTable()
	n := 20
	m := 50000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < m; j++ {
				tbl.Put([]byte(fmt.Sprintf("k%05d_%08d", i, j)),
					[]byte(fmt.Sprintf("v%05d_%08d", i, j)), 5)
			}
		}(i)
	}
	wg.Wait()
	it := tbl.NewIterator()
	defer it.Close()
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			require.True(t, it.Valid(), "%d %d", i, j)
			k := it.Key()
			require.EqualValues(t, fmt.Sprintf("k%05d_%08d", i, j), string(k))
			v, meta := it.Value()
			require.EqualValues(t, fmt.Sprintf("v%05d_%08d", i, j), string(v))
			require.EqualValues(t, 5, meta)
			it.Next()
		}
	}
	require.False(t, it.Valid())
}

func BenchmarkAdd(b *testing.B) {
	m := NewTable()
	for i := 0; i < b.N; i++ {
		m.Put([]byte(fmt.Sprintf("k%09d", i)), []byte(fmt.Sprintf("v%09d", i)), 0)
	}
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func BenchmarkReadWrite(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			list := NewTable()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						val, _ := list.Get(randomKey())
						if val != nil {
							count++
						}
					} else {
						newVal := make([]byte, 10)
						list.Put(randomKey(), newVal, 0)
					}
				}
			})
		})
	}
}
