package y

import (
	//	"bytes"
	"encoding/binary"
	//	"log"
)

const MaxSequenceNumber = (1 << 56) - 1

// ValueType encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
// The highest bit of the value type needs to be reserved to SST tables
// for them to do more flexible encoding.
type ValueType uint8

const (
	ValueTypeDeletion       = 0
	ValueTypeValue          = 1
	ValueTypeSingleDeletion = 0x7
	ValueTypeMax            = 0x7F
)

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueTypeForSeek = ValueTypeSingleDeletion
const ValueTypeForSeekForPrev = ValueTypeDeletion

func PackSeqAndType(seq uint64, t ValueType) uint64 {
	AssertTruef(seq <= MaxSequenceNumber, "%d", seq)
	AssertTruef(t <= ValueTypeMax, "%d", t)
	return uint64(seq<<8) | uint64(t)
}

func UnpackSeqAndType(packed uint64) (uint64, ValueType) {
	seq := packed >> 8
	t := packed & 0xFF
	AssertTruef(seq <= MaxSequenceNumber, "%d", seq)
	AssertTruef(t <= ValueTypeMax, "%d", t)
	return seq, ValueType(t)
}

// GetLengthPrefixedSlice gets the length from prefix, then returns a slice of
// those bytes. And it also returns the remainder slice.
func GetLengthPrefixedSlice(data []byte) ([]byte, []byte) {
	n, numRead := binary.Uvarint(data)
	AssertTruef(numRead > 0, "%d", numRead)
	data = data[numRead:]
	AssertTruef(len(data) >= int(n), "%d %d", len(data), n)
	return data[:n], data[n:]
}

// A helper class useful for DBImpl::Get()
type LookupKey struct {
	// We construct a char array of the form:
	//    klength  varint32               <-- start_
	//    userkey  char[klength]          <-- kstart_
	//    tag      uint64
	//                                    <-- end_
	// The array is a suitable MemTable key.
	// The suffix starting with "userkey" can be used as an InternalKey.
	start  []byte
	kstart int // Skip this many bytes for the varint encoding size of internal key.
}

// NewLookupKey is a helper class useful for DBImpl::Get().
func NewLookupKey(userKey []byte, seq uint64) *LookupKey {
	usize := len(userKey)
	needed := usize + 13 // Conservative estimate.
	s := new(LookupKey)
	buf := make([]byte, needed)
	s.kstart = binary.PutUvarint(buf, uint64(usize+8))

	// Copy user key.
	p := buf[s.kstart:]
	AssertTrue(len(userKey) == copy(p, userKey))

	tag := PackSeqAndType(seq, ValueTypeForSeek)
	binary.BigEndian.PutUint64(p[usize:], tag)
	s.start = buf[:s.kstart+usize+8]
	return s
}

// MemtableKey returns a key suitable for lookup in a MemTable.
func (s *LookupKey) MemtableKey() []byte { return s.start }

// InternalKey returns an internal key (suitable for passing to an internal iterator)
func (s *LookupKey) InternalKey() []byte { return s.start[s.kstart:] }

// UserKey returns the user key.
func (s *LookupKey) UserKey() []byte { return s.start[s.kstart : len(s.start)-8] }
