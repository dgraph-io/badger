package memtable

import (
	//	"bytes"
	"encoding/binary"
	//	"log"
	"os"

	"github.com/dgraph-io/badger/skiplist"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

type KeyComparator struct {
	userCmp skiplist.Comparator // For comparing user keys.
}

type Memtable struct {
	table *skiplist.Skiplist
	cmp   KeyComparator // User key comparator.
}

func (s KeyComparator) Compare(a, b []byte) int {
	// Read the length prefix to get internal key length.
	// Grab "internal key length" many bytes.
	// Compare user keys. If the same, compare the extra bits which comprise
	// the sequence number and value type.
	k1, _ := y.GetLengthPrefixedSlice(a)
	k2, _ := y.GetLengthPrefixedSlice(b)
	y.AssertTrue(len(k1) >= 8)
	y.AssertTrue(len(k2) >= 8)

	// Compare user keys. Remove the last 8 bytes.
	u1 := k1[:len(k1)-8]
	u2 := k2[:len(k2)-8]
	r := s.userCmp.Compare(u1, u2) // Compare user keys.
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

var DefaultKeyComparator = KeyComparator{
	userCmp: skiplist.DefaultComparator,
}

// NewMemtable creates a new memtable. Input is the user key comparator.
func NewMemtable(cmp KeyComparator) *Memtable {
	return &Memtable{
		cmp: cmp,
		// For now, just use some default values. Can be exposed as options later.
		table: skiplist.NewSkiplist(12, 4, cmp),
	}
}

func (s *Memtable) Add(seqNum uint64, typ y.ValueType, key []byte, value []byte) {
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

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
func encodeKey(key []byte) []byte {
	buf := make([]byte, 8)
	l := binary.PutUvarint(buf, uint64(len(key)))
	out := make([]byte, l+len(key)+1)
	// Last byte is zero to denote valSize=0.
	y.AssertTrue(l == copy(out, buf[:l]))
	y.AssertTrue(len(key) == copy(out[l:], key))
	return out
}

func (s *Memtable) WriteLevel0Table(f *os.File) error {
	iter := s.Iterator()
	b := table.TableBuilder{}
	b.Reset()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	f.Write(b.Finish())
	return nil
}

type Iterator struct {
	iter *skiplist.Iterator
}

func (s *Memtable) Iterator() *Iterator {
	return &Iterator{
		iter: s.table.Iterator(),
	}
}

func (s *Iterator) Valid() bool { return s.iter.Valid() }

func (s *Iterator) Seek(internalKey []byte) {
	s.iter.Seek(encodeKey(internalKey))
}

func (s *Iterator) SeekToFirst() { s.iter.SeekToFirst() }

func (s *Iterator) SeekToLast() { s.iter.SeekToLast() }

func (s *Iterator) Next() { s.iter.Next() }

func (s *Iterator) Prev() { s.iter.Prev() }

// key returns the internal key, which is user key + seqNum + valueType.
func (s *Iterator) Key() []byte {
	k, _ := y.GetLengthPrefixedSlice(s.iter.Key())
	return k
}

func (s *Iterator) Value() []byte {
	_, val := y.GetLengthPrefixedSlice(s.iter.Key())
	valSlice, _ := y.GetLengthPrefixedSlice(val)
	return valSlice
}

// Get looks up a key. Returns the value and whether we found the key in this table.
func (s *Memtable) Get(lkey *y.LookupKey) ([]byte, bool) {
	if s == nil {
		return nil, false // No hit.
	}
	it := s.Iterator()
	it.Seek(lkey.InternalKey())
	if !it.Valid() {
		return nil, false // No hit.
	}

	key := it.Key()
	keyLen := len(key)

	// Compare user keys.
	if s.cmp.userCmp.Compare(lkey.UserKey(), key[:keyLen-8]) != 0 {
		return nil, false // No hit.
	}

	// Check value type.
	tag := binary.BigEndian.Uint64(key[keyLen-8:])
	typ := tag & 0xFF
	switch typ {
	case y.ValueTypeValue:
		return it.Value(), true
	case y.ValueTypeDeletion:
		return nil, true // We have a hit, and we know this key is deleted.
	default:
		y.Fatalf("Unknown value type: %v", typ)
	}
	return nil, false // No hit.
}
