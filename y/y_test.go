package y

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/dustin/go-humanize"

	"github.com/stretchr/testify/require"
)

func TestReadNoalloc(t *testing.T) {
	n := NewNoAllocBuffer(5)
	var result []byte
	for i := 0; i < 4; i++ {
		res := []byte(fmt.Sprintf("%4d", i))
		result = append(result, res...)
		_, err := n.Write(res)
		require.NoError(t, err)
	}
	require.Equal(t, n.Bytes(), result)
}

func TestNoAllocHashBuffer(t *testing.T) {
	n := NewNoAllocHashBuffer(5)
	var result []byte
	hash := crc32.New(CastagnoliCrcTable)
	for i := 0; i < 4; i++ {
		res := []byte(fmt.Sprintf("%4d", i))
		result = append(result, res...)
		_, err := n.Write(res)
		require.NoError(t, err)

		_, err = hash.Write(res)
		require.NoError(t, err)
	}
	require.Equal(t, n.Bytes(), result)
	require.Equal(t, hash.Sum32(), n.Sum32())
	// Add another entry
	_, err := n.WriteByte(byte(32))
	require.NoError(t, err)

	hash.Reset()
	require.Equal(t, hash.Sum32(), n.Sum32())

}

func BenchmarkBuffer(b *testing.B) {
	count := int(1 * 1e6) // 1,000,000 * 10 == Total size 10,000,000 bytes
	pageSize := 1 << 20   // 1 MB page size
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	totalSz := humanize.Bytes(uint64(count * 10))
	fmt.Println("Page size:", humanize.Bytes(uint64(pageSize)))
	b.Run(fmt.Sprintf("write-%s", totalSz), func(b *testing.B) {
		buf := bytes.Buffer{}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				_, err := buf.Write(key(j))
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run(fmt.Sprintf("Noallocwrite-%s", totalSz), func(b *testing.B) {
		n := NewNoAllocBuffer(pageSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				_, err := n.Write(key(j))
				if err != nil {
					b.Fatal(err)
				}
			}
		}

	})
	b.Run(fmt.Sprintf("bytes-%s", totalSz), func(b *testing.B) {
		buf := bytes.Buffer{}
		for j := 0; j < count; j++ {
			_, err := buf.Write(key(j))
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if count*10 != len(buf.Bytes()) {
				b.Fail()
			}
		}
	})

	b.Run(fmt.Sprintf("Noallocbytes-%s", totalSz), func(b *testing.B) {
		n := NewNoAllocBuffer(pageSize)
		for j := 0; j < count; j++ {
			_, err := n.Write(key(j))
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if count*10 != len(n.Bytes()) {
				b.Fail()
			}
		}
	})

}
