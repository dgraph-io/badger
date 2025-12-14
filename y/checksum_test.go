package y

import (
	"hash/crc32"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/stretchr/testify/require"
)

func TestCalculateChecksum_CRC32C(t *testing.T) {
	data := []byte("hello world")
	expected := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	got := CalculateChecksum(data, pb.Checksum_CRC32C)
	require.Equal(t, expected, got)

	// empty input
	expectedEmpty := uint64(crc32.Checksum([]byte{}, CastagnoliCrcTable))
	gotEmpty := CalculateChecksum([]byte{}, pb.Checksum_CRC32C)
	require.Equal(t, expectedEmpty, gotEmpty)
}

func TestCalculateChecksum_XXHash64(t *testing.T) {
	data := []byte("hello world")
	expected := xxhash.Sum64(data)
	got := CalculateChecksum(data, pb.Checksum_XXHash64)
	require.Equal(t, expected, got)
}

func TestVerifyChecksum_Success(t *testing.T) {
	data := []byte("hello world")
	c1 := &pb.Checksum{Algo: pb.Checksum_CRC32C, Sum: CalculateChecksum(data, pb.Checksum_CRC32C)}
	require.NoError(t, VerifyChecksum(data, c1))

	c2 := &pb.Checksum{Algo: pb.Checksum_XXHash64, Sum: CalculateChecksum(data, pb.Checksum_XXHash64)}
	require.NoError(t, VerifyChecksum(data, c2))
}

func TestVerifyChecksum_Mismatch(t *testing.T) {
	data := []byte("x")
	c := &pb.Checksum{Algo: pb.Checksum_CRC32C, Sum: 0}
	err := VerifyChecksum(data, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestCalculateChecksum_UnsupportedAlgoPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for unsupported algorithm")
		}
	}()

	_ = CalculateChecksum([]byte("x"), pb.Checksum_Algorithm(999))
}
