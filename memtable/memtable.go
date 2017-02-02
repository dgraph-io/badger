package memtable

import (
	"encoding/binary"
	//	"log"

	"github.com/dgraph-io/badger/skiplist"
	"github.com/dgraph-io/badger/y"
)

type Memtable struct {
	table *skiplist.Skiplist
	cmp   skiplist.Comparator // User key comparator.
}

type keyComparator struct {
	cmp skiplist.Comparator // For comparing user keys.
}

func (s keyComparator) Compare(a, b []byte) int {
	// Read the length prefix to get internal key length.
	// Grab "internal key length" many bytes.
	// Compare user keys. If the same, compare the extra bits which comprise
	// the sequence number and value type.
	k1 := y.GetLengthPrefixedSlice(a)
	k2 := y.GetLengthPrefixedSlice(b)
	y.AssertTrue(len(k1) >= 8)
	y.AssertTrue(len(k2) >= 8)

	// Compare user keys. Remove the last 8 bytes.
	u1 := k1[:len(k1)-8]
	u2 := k2[:len(k2)-8]
	r := s.cmp.Compare(u1, u2) // Compare user keys.
	if r != 0 {
		return r
	}
	// User keys are equal. Compare the extra stuff.
	// Decreasing sequence number, then decreasing value type.
	// In big endian, this is easy.
	e1 := binary.BigEndian.Uint64(k1[len(k1)-8:])
	e2 := binary.BigEndian.Uint64(k2[len(k2)-8:])
	if e1 > e2 {
		return -1
	} else if e1 < e2 {
		return 1
	}
	return 0
}

var DefaultKeyComparator = keyComparator{
	cmp: skiplist.DefaultComparator,
}

// NewMemtable creates a new memtable. Input is the user key comparator.
func NewMemtable(cmp skiplist.Comparator) *Memtable {
	return &Memtable{
		cmp: cmp,
		// For now, just use some default values. Can be exposed as options later.
		table: skiplist.NewSkiplist(12, 4, cmp),
	}
}

func (s *Memtable) Add(seqNum y.SequenceNumber, typ y.ValueType, key []byte,
	value []byte) {

	keySize := len(key)
	valSize := len(value)
	internalKeySize := keySize + 8

	// buf1, buf2 should go on stack.
	buf1 := make([]byte, 8)
	l1 := binary.PutUvarint(buf1, uint64(internalKeySize))

	buf2 := make([]byte, 8)
	l2 := binary.PutUvarint(buf2, uint64(valSize))

	out := make([]byte, l1+internalKeySize+l2+valSize)

	// Internal key size.
	y.AssertTrue(l1 == copy(out, buf1[:l1]))
	p := out[l1:]

	// User key.
	y.AssertTrue(keySize == copy(p, key))
	p = p[keySize:]

	// Sequence number and value type.
	binary.BigEndian.PutUint64(p, y.PackSeqAndType(seqNum, typ))
	p = p[8:]

	// Value size.
	y.AssertTrue(l2 == copy(p, buf2[:l2]))
	p = p[l2:]

	// Value.
	y.AssertTrue(valSize == copy(p, value))
	p = p[valSize:]

	y.AssertTrue(len(p) == 0)
	s.table.Insert(out)
}
