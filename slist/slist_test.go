package slist

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func newValue(v int) unsafe.Pointer {
	p := new(int)
	*p = v
	return unsafe.Pointer(p)
}

func getValue(p unsafe.Pointer) int { return *(*int)(p) }

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist()
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	require.True(t, nil == l.Put([]byte("key1"), val1, false))
	require.True(t, nil == l.Put([]byte("key3"), val3, false))
	require.True(t, nil == l.Put([]byte("key2"), val2, false))

	v := l.Get([]byte("key"))
	require.True(t, v == nil)

	v = l.Get([]byte("key1"))
	require.True(t, v != nil)
	require.EqualValues(t, 42, getValue(v))

	v = l.Get([]byte("key2"))
	require.True(t, v != nil)
	require.EqualValues(t, 52, getValue(v))

	v = l.Get([]byte("key3"))
	require.True(t, v != nil)
	require.EqualValues(t, 62, getValue(v))

	// Replace existing values. Set onlyIfAbsent=true.
	require.EqualValues(t, val2, l.Put([]byte("key2"), val4, true))

	v = l.Get([]byte("key2"))
	require.True(t, v != nil)
	require.EqualValues(t, 52, getValue(v))

	// Replace existing values. Set onlyIfAbsent=false.
	require.EqualValues(t, val2, l.Put([]byte("key2"), val4, false))

	v = l.Get([]byte("key2"))
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
			require.True(t, nil == l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), true))
		}(i)
	}
	wg.Wait()
	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get([]byte(fmt.Sprintf("%05d", i)))
			require.True(t, v != nil)
			require.EqualValues(t, i, getValue(v))
		}(i)
	}
	wg.Wait()
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
			l.Put(key, newValue(i), false)
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue bool
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := l.Get(key)
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
		require.True(t, nil == l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), true))
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, i, getValue(it.Value()))
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
		require.True(t, nil == l.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i), true))
	}
	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, i, getValue(it.Value()))
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
		require.True(t, nil == l.Put([]byte(fmt.Sprintf("%05d", i*10+1000)), newValue(v), true))
	}
	it.Seek([]byte(""))
	require.True(t, it.Valid())
	require.EqualValues(t, 1000, getValue(it.Value()))

	it.Seek([]byte("01000"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1000, getValue(it.Value()))

	it.Seek([]byte("01005"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1010, getValue(it.Value()))

	it.Seek([]byte("01010"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1010, getValue(it.Value()))

	it.Seek([]byte("99999"))
	require.False(t, it.Valid())

	// Try SeekForPrev.
	it.SeekForPrev([]byte(""))
	require.False(t, it.Valid())

	it.SeekForPrev([]byte("01000"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1000, getValue(it.Value()))

	it.SeekForPrev([]byte("01005"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1000, getValue(it.Value()))

	it.SeekForPrev([]byte("01010"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1010, getValue(it.Value()))

	it.SeekForPrev([]byte("99999"))
	require.True(t, it.Valid())
	require.EqualValues(t, 1990, getValue(it.Value()))
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func BenchmarkReadParallel(b *testing.B) {
	value := newValue(123)
	list := NewSkiplist()
	for i := 0; i < 100000; i++ {
		list.Put(randomKey(), value, true)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			list.Get(randomKey())
		}
	})
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
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						it := list.NewIterator()
						it.Seek(randomKey())
					} else {
						list.Put(randomKey(), value, true)
					}
				}
			})
		})
	}
}

// Test takes a long time because early parallel writes tend to lead to a lot of starts and
// variance in the running time.
func BenchmarkWriteParallel(b *testing.B) {
	value := newValue(123)
	list := NewSkiplist()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			list.Put(randomKey(), value, true)
		}
	})
}

// Alternate version to WriteParallel that fixes the number of writes to be decently large.
func BenchmarkWriteParallelAlt(b *testing.B) {
	value := newValue(123)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list := NewSkiplist()
		for i := 0; i < 10000; i++ {
			list.Put(randomKey(), value, true)
		}
	}
}
