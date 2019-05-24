package y

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/dgraph-io/badger/pb"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
)

// ErrChecksumMismatch is returned at checksum mismatch.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// CalculateChecksum calculates checksum for data using ct checksum type.
func CalculateChecksum(data []byte, ct pb.ChecksumType) []byte {
	switch ct {
	case pb.ChecksumType_CRC32C:
		return calculateCRC32C(data)
	case pb.ChecksumType_XXHash:
		return calculateXXHash(data)
	default:
		panic("checksum type not supported")
	}
}

// VerifyChecksum verifies that checksum for data using ct should be equal to expected.
func VerifyChecksum(data, expected []byte, ct pb.ChecksumType) error {
	switch ct {
	case pb.ChecksumType_CRC32C:
		return verifyCRC32C(data, expected)
	case pb.ChecksumType_XXHash:
		return verifyXXHash(data, expected)
	default:
		panic("checksum type not supported")
	}
}

// calculateCRC32C calculates checksum for data using CRC32C.
func calculateCRC32C(data []byte) []byte {
	cs := crc32.Checksum(data, CastagnoliCrcTable)
	csByte := make([]byte, 4)
	binary.BigEndian.PutUint32(csByte, cs)
	return csByte
}

// calculateXXHash calculates checksum for data using xxHash.
func calculateXXHash(data []byte) []byte {
	cs := xxhash.Sum64(data)
	csByte := make([]byte, 8)
	binary.BigEndian.PutUint64(csByte, cs)
	return csByte
}

// verifyCRC32C verifies CRC32C checksum for data is equal to expected.
func verifyCRC32C(data, expected []byte) error {
	actual := calculateCRC32C(data)
	if !bytes.Equal(actual, expected) {
		return Wrapf(ErrChecksumMismatch, "actual: %s, expected: %s", actual, expected)
	}

	return nil
}

// verifyXXHash verifies xxHash checksum for data is equal to expected.
func verifyXXHash(data, expected []byte) error {
	actual := calculateXXHash(data)
	if !bytes.Equal(actual, expected) {
		return Wrapf(ErrChecksumMismatch, "actual: %s, expected: %s", actual, expected)
	}

	return nil
}
