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

package skl

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	//	"github.com/dgraph-io/badger/y"
)

func newValue(v int) unsafe.Pointer {
	p := new(int)
	*p = v
	return unsafe.Pointer(p)
}

func getValue(p unsafe.Pointer) int { return *(*int)(p) }

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	x := s.head.getNext(0)
	count := 0
	for x != nil {
		count++
		x = x.getNext(0)
	}
	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := NewSkiplist()
	val, _ := l.Get(key)
	require.True(t, val == nil) // Cannot use require.Nil for unsafe.Pointer nil.

	for _, less := range []bool{true, false} {
		for _, allowEqual := range []bool{true, false} {
			n, found := l.findNear(key, less, allowEqual)
			require.Nil(t, n)
			require.False(t, found)
		}
	}

	it := l.NewIterator()
	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	it.Seek(key)
	require.False(t, it.Valid())
}

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist()
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	l.Put([]byte("key1"), val1, 55)
	l.Put([]byte("key3"), val3, 56)
	l.Put([]byte("key2"), val2, 57)

	v, meta := l.Get([]byte("key"))
	require.True(t, v == nil)

	v, meta = l.Get([]byte("key1"))
	require.True(t, v != nil)
	require.EqualValues(t, 42, getValue(v))
	require.EqualValues(t, 55, meta)

	v, meta = l.Get([]byte("key2"))
	require.True(t, v != nil)
	require.EqualValues(t, 52, getValue(v))
	require.EqualValues(t, 57, meta)

	v, meta = l.Get([]byte("key3"))
	require.True(t, v != nil)
	require.EqualValues(t, 62, getValue(v))
	require.EqualValues(t, 56, meta)

	l.Put([]byte("key2"), val4, 0)
	v, _ = l.Get([]byte("key2"))
	require.True(t, v != nil)
	require.EqualValues(t, 72, getValue(v))
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkiplist()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
		}(i)
	}
	wg.Wait()
	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _ := l.Get([]byte(fmt.Sprintf("%05d", i)))
			require.True(t, v != nil)
			require.EqualValues(t, i, getValue(v))
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, n, length(l))
}

// TestOneKey will read while writing to one single key.
func TestOneKey(t *testing.T) {
	const n = 100
	key := []byte("thekey")
	l := NewSkiplist()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(key, newValue(i), 0)
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue bool
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _ := l.Get(key)
			if p == nil {
				return
			}
			sawValue = true
			v := getValue(p)
			require.True(t, 0 <= v && v < n)
		}()
	}
	wg.Wait()
	require.True(t, sawValue)
	require.EqualValues(t, 1, length(l))
}

func TestFindNear(t *testing.T) {
	l := NewSkiplist()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put([]byte(key), newValue(i), 0)
	}

	n, eq := l.findNear([]byte("00001"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, true)
	require.Nil(t, n)
	require.False(t, eq)

	n, eq = l.findNear([]byte("00005"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "00015", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key))
	require.True(t, eq)
	n, eq = l.findNear([]byte("00005"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05555"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key))
	require.True(t, eq)
	n, eq = l.findNear([]byte("05555"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05545", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05558"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key))
	require.False(t, eq)

	n, eq = l.findNear([]byte("09995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key))
	require.True(t, eq)
	n, eq = l.findNear([]byte("09995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "09985", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key))
	require.True(t, eq)

	n, eq = l.findNear([]byte("59995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), false, true)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key))
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key))
	require.False(t, eq)
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist()
	it := l.NewIterator()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := n - 1; i >= 0; i-- {
		l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		val, _ := it.Value()
		require.EqualValues(t, i, getValue(val))
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist()
	it := l.NewIterator()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := 0; i < n; i++ {
		l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
	}
	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		val, _ := it.Value()
		require.EqualValues(t, i, getValue(val))
		it.Prev()
	}
	require.False(t, it.Valid())
}

// TestIteratorSeek tests Seek and SeekForPrev.
func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := NewSkiplist()
	it := l.NewIterator()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Put([]byte(fmt.Sprintf("%05d", i*10+1000)), newValue(v), 0)
	}
	it.Seek([]byte(""))
	require.True(t, it.Valid())
	val, _ := it.Value()
	require.EqualValues(t, 1000, getValue(val))

	it.Seek([]byte("01000"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1000, getValue(val))

	it.Seek([]byte("01005"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1010, getValue(val))

	it.Seek([]byte("01010"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1010, getValue(val))

	it.Seek([]byte("99999"))
	require.False(t, it.Valid())

	// Try SeekForPrev.
	it.SeekForPrev([]byte(""))
	require.False(t, it.Valid())

	it.SeekForPrev([]byte("01000"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1000, getValue(val))

	it.SeekForPrev([]byte("01005"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1000, getValue(val))

	it.SeekForPrev([]byte("01010"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1010, getValue(val))

	it.SeekForPrev([]byte("99999"))
	require.True(t, it.Valid())
	val, _ = it.Value()
	require.EqualValues(t, 1990, getValue(val))
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			list := NewSkiplist()
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
						list.Put(randomKey(), value, 0)
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string]unsafe.Pointer)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey())]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey())] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}
