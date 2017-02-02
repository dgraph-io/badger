package y

import (
	"encoding/binary"
)

type SequenceNumber uint64

const MaxSequenceNumber = (1 << 56) - 1

// ValueType encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
// The highest bit of the value type needs to be reserved to SST tables
// for them to do more flexible encoding.
type ValueType uint8

const (
	TypeDeletion = 1
	TypeValue    = iota

	// More next time.

	TypeMax = 0x7F
)

func PackSeqAndType(seq SequenceNumber, t ValueType) uint64 {
	AssertTruef(seq <= MaxSequenceNumber, "%d", seq)
	AssertTruef(t <= TypeMax, "%d", t)
	return uint64(seq<<8) | uint64(t)
}

func UnpackSeqAndType(packed uint64) (SequenceNumber, ValueType) {
	seq := packed >> 8
	t := packed & 0xFF
	AssertTruef(seq <= MaxSequenceNumber, "%d", seq)
	AssertTruef(t <= TypeMax, "%d", t)
	return SequenceNumber(seq), ValueType(t)
}

// GetLengthPrefixedSlice gets the length from prefix, then returns that
// number of bytes.
func GetLengthPrefixedSlice(data []byte) []byte {
	n, numRead := binary.Uvarint(data)
	AssertTruef(numRead > 0, "%d", numRead)
	data = data[numRead:]
	AssertTruef(len(data) >= int(n), "%d %d", len(data), n)
	return data[:n]
}
