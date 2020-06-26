package y

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockPool(t *testing.T) {
	const (
		size       = 1 << 10
		iterations = 1000
	)

	require := require.New(t)

	pool := BlockPool{
		BlockSize: size,
	}

	for i := 0; i < iterations; i++ {
		ret := pool.Get()
		require.Equal(size, len(ret))

		// Make sure we can write to every single byte of the slice.
		for idx := range ret {
			ret[idx] = 'a'
		}

		pool.Put(ret)
	}
}

func BenchmarkBlockPool(b *testing.B) {
	b.ReportAllocs()
	const size = 1 << 10

	pool := BlockPool{
		BlockSize: size,
	}

	for i := 0; i < b.N; i++ {
		ret := pool.Get()
		if len(ret) != size {
			b.Fail()
		}

		for idx := range ret {
			ret[idx] = 'a'
		}

		pool.Put(ret)
	}
}

func BenchmarkNoBlockPool(b *testing.B) {
	b.ReportAllocs()
	// Do not make that a const, we want to force the compiler to allocate on the heap
	var size = 1 << 10

	for i := 0; i < b.N; i++ {
		ret := make([]byte, size)
		if len(ret) != size {
			b.Fail()
		}

		for idx := range ret {
			ret[idx] = 'a'
		}
	}
}
