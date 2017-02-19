package skl

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

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist()
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	//	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	require.True(t, nil == l.Put([]byte("key1"), val1, false))
	require.True(t, nil == l.Put([]byte("key3"), val3, false))
	require.True(t, nil == l.Put([]byte("key2"), val2, false))

	require.EqualValues(t, 3, length(l))
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
	require.EqualValues(t, 1, length(l))
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
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
