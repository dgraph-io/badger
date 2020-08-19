package y

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCallocBuffer(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var bytesBuffer bytes.Buffer // This is just for verifying result.
	bytesBuffer.Grow(512)

	cBuffer := NewBuffer(512)

	// Writer small []byte
	var smallBytes [256]byte
	rand.Read(smallBytes[:])
	var bigBytes [1024]byte
	rand.Read(bigBytes[:])

	_, err := cBuffer.Write(smallBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")
	_, err = cBuffer.Write(bigBytes[:])
	require.NoError(t, err, "unable to write data to page buffer")

	// Write data to bytesBuffer also, just to match result.
	bytesBuffer.Write(smallBytes[:])
	bytesBuffer.Write(bigBytes[:])

	require.True(t, bytes.Equal(cBuffer.Bytes(), bytesBuffer.Bytes()))
}

func TestCallocBufferWrite(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var wb [128]byte
	rand.Read(wb[:])

	cb := NewBuffer(32)
	bb := new(bytes.Buffer)

	end := 32
	for i := 0; i < 3; i++ {
		n, err := cb.Write(wb[:end])
		require.NoError(t, err, "unable to write bytes to buffer")
		require.Equal(t, n, end, "length of buffer and length written should be equal")

		// append to bb also for testing.
		bb.Write(wb[:end])

		require.True(t, bytes.Equal(cb.Bytes(), bb.Bytes()), "Both bytes should match")
		end = end * 2
	}
}
