package y

import (
	"hash/crc32"

	"github.com/dgraph-io/badger/pb"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
)

// ErrChecksumMismatch is returned at checksum mismatch.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// CalculateChecksum calculates checksum for data using ct checksum type.
func CalculateChecksum(data []byte, ct pb.Checksum_Algorithm) uint64 {
	switch ct {
	case pb.Checksum_CRC32C:
		return uint64(crc32.Checksum(data, CastagnoliCrcTable))
	case pb.Checksum_XXHash64:
		return xxhash.Sum64(data)
	default:
		panic("checksum type not supported")
	}
}

// VerifyChecksum validates the checksum for the data against the given expected checksum.
func VerifyChecksum(data []byte, expected pb.Checksum) error {
	actual := CalculateChecksum(data, expected.Algo)
	if actual != expected.Sum64 {
		return Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expected)
	}
	return nil
}
