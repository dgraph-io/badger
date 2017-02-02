package skiplist

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgraph/x"
)

func TestSeek(t *testing.T) {
	list := NewSkiplist(10, 3, DefaultComparator)
	it := list.Iterator()

	require.False(t, it.Valid())

	list.Insert([]byte("def"))
	list.Insert([]byte("abc"))

	it.Seek([]byte("abc"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.Seek([]byte("a"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.Seek([]byte("d"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "def")

	it.Seek([]byte("fff"))
	require.False(t, it.Valid())
}

func TestSeekForPrev(t *testing.T) {
	list := NewSkiplist(10, 3, DefaultComparator)
	it := list.Iterator()

	require.False(t, it.Valid())

	list.Insert([]byte("def"))
	list.Insert([]byte("abc"))

	it.Seek([]byte("abc"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.SeekForPrev([]byte("a"))
	require.False(t, it.Valid())

	it.SeekForPrev([]byte("d"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.SeekForPrev([]byte("fff"))
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "def")
}

func TestNext(t *testing.T) {
	list := NewSkiplist(10, 3, DefaultComparator)
	it := list.Iterator()

	list.Insert([]byte("abc"))
	it.SeekToFirst()
	require.EqualValues(t, it.Key(), "abc")

	list.Insert([]byte("def"))
	list.Insert([]byte("cde"))
	it.Next()
	require.EqualValues(t, it.Key(), "cde")
	it.Next()
	require.EqualValues(t, it.Key(), "def")
}

func TestPrev(t *testing.T) {
	list := NewSkiplist(10, 3, DefaultComparator)
	it := list.Iterator()

	list.Insert([]byte("def"))
	it.SeekToLast()
	require.EqualValues(t, it.Key(), "def")

	list.Insert([]byte("abc"))
	list.Insert([]byte("cde"))
	it.Prev()
	require.EqualValues(t, it.Key(), "cde")
	it.Prev()
	require.EqualValues(t, it.Key(), "abc")
}

func TestReadWrite(t *testing.T) {
	list := NewSkiplist(10, 3, DefaultComparator)
	var wg sync.WaitGroup

	// Start writing first.
	for i := 0; i < 100; i++ {
		list.Insert([]byte(fmt.Sprintf("%05d", i)))
	}

	wg.Add(1)
	// Write in one goroutine.
	go func() {
		defer wg.Done()
		for i := 100; i < 10000; i++ {
			list.Insert([]byte(fmt.Sprintf("%05d", i)))
		}
	}()

	// Read from multiple goroutines. See if it will crash.
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			it := list.Iterator()
			var count int
			for it.SeekToFirst(); it.Valid(); it.Next() {
				count++
			}
			require.True(t, count >= 100)
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			it := list.Iterator()
			var count int
			for it.SeekToLast(); it.Valid(); it.Prev() {
				count++
			}
			require.True(t, count >= 100)
		}(i)
	}
	wg.Wait()
}

func randomKey() []byte {
	bs := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(bs, key)
	binary.LittleEndian.PutUint32(bs[4:], key2)
	return bs
}

func BenchmarkWrite(b *testing.B) {
	maxDepth := 10
	for branch := 2; branch <= 7; branch++ {
		b.Run(fmt.Sprintf("branch_%d", branch), func(b *testing.B) {
			list := NewSkiplist(maxDepth, branch, DefaultComparator)
			for i := 0; i < b.N; i++ {
				list.Insert(randomKey())
			}
		})
	}
}

func BenchmarkWriteParallel(b *testing.B) {
	maxDepth := 10
	for branch := 2; branch <= 7; branch++ {
		b.Run(fmt.Sprintf("branch_%d", branch), func(b *testing.B) {
			list := NewSkiplist(maxDepth, branch, DefaultComparator)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					list.InsertConcurrently(randomKey())
				}
			})
		})
	}
}

func BenchmarkRead(b *testing.B) {
	maxDepth := 10
	for branch := 2; branch <= 7; branch++ {
		b.Run(fmt.Sprintf("branch_%d", branch), func(b *testing.B) {
			list := NewSkiplist(maxDepth, branch, DefaultComparator)
			for i := 0; i < 100000; i++ {
				list.Insert(randomKey())
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it := list.Iterator()
				it.Seek(randomKey())
			}
		})
	}
}

func BenchmarkReadParallel(b *testing.B) {
	maxDepth := 10
	for branch := 2; branch <= 7; branch++ {
		b.Run(fmt.Sprintf("branch_%d", branch), func(b *testing.B) {
			list := NewSkiplist(maxDepth, branch, DefaultComparator)
			for i := 0; i < 100000; i++ {
				list.Insert(randomKey())
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					it := list.Iterator()
					it.Seek(randomKey())
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	maxDepth := 10
	branch := 3
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			list := NewSkiplist(maxDepth, branch, DefaultComparator)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						it := list.Iterator()
						it.Seek(randomKey())
					} else {
						list.InsertConcurrently(randomKey())
					}
				}
			})

		})
	}
}

func TestMain(m *testing.M) {
	x.Init()
	os.Exit(m.Run())
}
