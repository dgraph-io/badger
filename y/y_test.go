package y

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkBuffer(b *testing.B) {
	var btw [1024]byte
	rand.Read(btw[:])

	pageSize := 1024

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
}

func TestBufferWrite(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [128]byte
	rand.Read(wb[:])

	pb := NewPageBuffer(32)
	bb := new(bytes.Buffer)

	end := 32
	for i := 0; i < 3; i++ {
		n, err := pb.Write(wb[:end])
		require.NoError(t, err, "unable to write bytes to buffer")
		require.Equal(t, n, end, "length of buffer and length written should be equal")

		// append to bb also for testing.
		bb.Write(wb[:end])

		require.True(t, bytes.Equal(pb.Bytes(), bb.Bytes()), "Both bytes should match")
		end = end * 2
	}
}

func TestPagebufferTruncate(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [1024]byte
	rand.Read(wb[:])

	b := NewPageBuffer(32)
	n, err := b.Write(wb[:])
	require.Equal(t, n, len(wb), "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")

	require.True(t, bytes.Equal(wb[:], b.Bytes()), "bytes written and read should be equal")

	// Truncate to 512.
	b.Truncate(512)
	require.True(t, bytes.Equal(b.Bytes(), wb[:512]))

	// Again write wb.
	n, err = b.Write(wb[:])
	require.Equal(t, n, len(wb), "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")

	// Truncate to 1000.
	b.Truncate(1000)
	require.True(t, bytes.Equal(b.Bytes(), append(wb[:512], wb[:]...)[:1000]))
}

func TestPagebufferReader(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [1024]byte
	rand.Read(wb[:])

	b := NewPageBuffer(32)
	n, err := b.Write(wb[:])
	require.Equal(t, n, len(wb), "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")
	// Also append some bytes so that last page is not full.
	n, err = b.Write(wb[:10])
	require.Equal(t, n, 10, "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")

	reader := b.NewReaderAt(0)
	// Read first 512 bytes.
	var rb [512]byte
	n, err = reader.Read(rb[:])
	require.NoError(t, err, "unable to read error")
	require.True(t, n == len(rb), "length read should be equal")
	// Match if read bytes are correct or not.
	rb2 := b.Bytes()[:512]
	require.True(t, bytes.Equal(rb[:], rb2))

	// Next read using reader.
	n, err = reader.Read(rb[:])
	require.NoError(t, err, "unable to read error")
	require.True(t, n == len(rb), "length read should be equal")
	// Read same number of bytes using ReaderAt.
	rb2 = b.Bytes()[512:1024]
	require.True(t, bytes.Equal(rb[:], rb2))

	// Next read using reader for reading last 10 bytes.
	n, err = reader.Read(rb[:10])
	require.NoError(t, err, "unable to read error")
	require.True(t, n == 10, "length read should be equal")
	// Read same number of bytes using ReaderAt.
	rb2 = b.Bytes()[1024 : 1024+10]
	require.True(t, bytes.Equal(rb[:10], rb2))

	// Check if EOF is returned at end or not.
	n, err = reader.Read(rb[:10])
	require.Equal(t, err, io.EOF, "EOF should be returned at end")
}

func TestPagebufferReader2(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [1024]byte
	rand.Read(wb[:])

	b := NewPageBuffer(32)
	n, err := b.Write(wb[:])
	require.Equal(t, n, len(wb), "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")
	// Also append some bytes so that last page is not full.
	n, err = b.Write(wb[:10])
	require.Equal(t, n, 10, "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")

	randOffset := int(rand.Int31n(int32(b.length)))
	randLength := int(rand.Int31n(int32(b.length - randOffset)))
	reader := b.NewReaderAt(randOffset)
	// Read randLength bytes.
	rb := make([]byte, randLength)
	n, err = reader.Read(rb[:])
	require.NoError(t, err, "unable to read error")
	require.True(t, n == len(rb), "length read should be equal")
	// Read same number of bytes using ReaderAt.
	rb2 := b.Bytes()[randOffset : randOffset+randLength]
	require.True(t, bytes.Equal(rb[:], rb2))
}

func TestPagebufferReader3(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [1000]byte
	rand.Read(wb[:])

	b := NewPageBuffer(32)
	n, err := b.Write(wb[:])
	require.Equal(t, n, len(wb), "length of buffer and length written should be equal")
	require.NoError(t, err, "unable to write bytes to buffer")

	reader := b.NewReaderAt(0)

	chunk := 10 // Read 10 bytes in loop
	readBuf := make([]byte, chunk)
	currentOffset := 0

	for i := 0; i < len(wb)/chunk; i++ {
		n, err = reader.Read(readBuf)
		require.NoError(t, err, "unable to read from reader")
		require.Equal(t, n, chunk, "length read should be equal to chunck")
		require.True(t, bytes.Equal(readBuf, wb[currentOffset:currentOffset+chunk]))

		rb := b.Bytes()[currentOffset : currentOffset+chunk]
		require.True(t, bytes.Equal(wb[currentOffset:currentOffset+chunk], rb))

		currentOffset += chunk
	}

	// Read EOF.
	n, err = reader.Read(readBuf)
	require.Equal(t, err, io.EOF, "should return EOF")
}
