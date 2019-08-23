package y

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkBuffer(b *testing.B) {
	var btw [1024]byte
	rand.Read(btw[:])

	pageSize := 4 * 1024

	b.Run("bytes-buffer", func(b *testing.B) {
		buf := new(bytes.Buffer)
		buf.Grow(pageSize)

		for i := 0; i < b.N; i++ {
			buf.Write(btw[:])
		}
	})

	b.Run("page-buffer", func(b *testing.B) {
		b.Run(fmt.Sprintf("page-size-%d", pageSize), func(b *testing.B) {
			pageBuffer := NewPageBuffer(pageSize)
			for i := 0; i < b.N; i++ {
				pageBuffer.Write(btw[:])
			}
		})
	})
}

func TestPageBuffer(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var bytesBuffer bytes.Buffer // This is just of verifying result.
	bytesBuffer.Grow(512)

	pageBuffer := NewPageBuffer(512)

	// Writer small []byte
	var smallBytes [256]byte
	rand.Read(smallBytes[:])
	var bigBytes [1024]byte
	rand.Read(bigBytes[:])

	_, err := pageBuffer.Write(smallBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")
	_, err = pageBuffer.Write(bigBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")

	// Write data to bytesBuffer also, just to match result.
	bytesBuffer.Write(smallBytes[:])
	bytesBuffer.Write(bigBytes[:])

	require.True(t, bytes.Equal(pageBuffer.Bytes(), bytesBuffer.Bytes()))

	// Test ReadAt full length.
	offset := int(rand.Int31n(int32(pageBuffer.Len())))
	read1 := pageBuffer.ReadAt(offset, -1)
	read2 := bytesBuffer.Bytes()[offset:]

	require.True(t, bytes.Equal(read1, read2))

	// Test ReadAt fixed length.
	offset = int(rand.Int31n(int32(pageBuffer.Len())))
	length := int(rand.Int31n(int32(pageBuffer.Len() - offset)))
	read1 = pageBuffer.ReadAt(offset, length)
	read2 = bytesBuffer.Bytes()[offset : offset+length]

	require.True(t, bytes.Equal(read1, read2))
}
