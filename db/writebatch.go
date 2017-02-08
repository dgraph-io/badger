package db

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/y"
)

// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeNoop
// varstring :=
//    len: varint
//    data: uint8[len]

type WriteBatch struct {
	rep []byte
	// TODO: contentFlags
}

// sequence, count.
const headerSize = 12

func NewWriteBatch(reserved int) *WriteBatch {
	if reserved < headerSize {
		reserved = headerSize
	}
	return &WriteBatch{
		rep: make([]byte, headerSize, reserved),
	}
}

func (s *WriteBatch) Count() int {
	return int(binary.BigEndian.Uint32(s.rep[8:headerSize]))
}

func (s *WriteBatch) SetCount(n int) {
	binary.BigEndian.PutUint32(s.rep[8:headerSize], uint32(n))
}

func (s *WriteBatch) Sequence() uint64 {
	return binary.BigEndian.Uint64(s.rep[:8])
}

func (s *WriteBatch) SetSequence(seq uint64) {
	binary.BigEndian.PutUint64(s.rep[:8], seq)
}

func (s *WriteBatch) Put(key []byte, val []byte) {
	s.SetCount(s.Count() + 1)

	s.rep = append(s.rep, byte(y.ValueTypeValue))
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], uint64(len(key)))
	s.rep = append(s.rep, tmp[:n]...)
	s.rep = append(s.rep, key...)

	n = binary.PutUvarint(tmp[:], uint64(len(val)))
	s.rep = append(s.rep, tmp[:n]...)
	s.rep = append(s.rep, val...)
}

func (s *WriteBatch) Delete(key []byte) {
	s.SetCount(s.Count() + 1)

	s.rep = append(s.rep, byte(y.ValueTypeDeletion))
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], uint64(len(key)))
	s.rep = append(s.rep, tmp[:n]...)
	s.rep = append(s.rep, key...)
}

func (s *WriteBatch) Iterate(h WriteBatchHandler) error {
	input := s.rep
	if len(input) < headerSize {
		return y.Errorf("Malformed WriteBatch, too small")
	}
	input = input[headerSize:]
	var found int
	for len(input) > 0 {
		found++
		tag := input[0]
		input = input[1:]
		switch tag {
		case y.ValueTypeValue:
			var key, val []byte
			key, input = y.GetLengthPrefixedSlice(input)
			val, input = y.GetLengthPrefixedSlice(input)
			h.Put(key, val)
		case y.ValueTypeDeletion:
			var key []byte
			key, input = y.GetLengthPrefixedSlice(input)
			h.Delete(key)
		default:
			return y.Errorf("Unknown WriteBatch tag: %d", int(tag))
		}
	}
	if found != s.Count() {
		return y.Errorf("WriteBatch has wrong count: %d %d", found, s.Count())
	}
	return nil
}

func (s *WriteBatch) InsertInto(mem *memtable.Memtable) error {
	inserter := &MemtableInserter{
		seq: s.Sequence(),
		mem: mem,
	}
	return s.Iterate(inserter)
}

func (s *WriteBatch) SetContents(contents []byte) {
	y.AssertTrue(len(contents) >= headerSize)
	s.rep = contents
}

// Append adds input w to this WriteBatch.
func (s *WriteBatch) Append(w *WriteBatch) {
	s.SetCount(s.Count() + w.Count())
	s.rep = append(s.rep, w.rep[headerSize:]...)
}

// WriteBatch's Iterate will communicate with this interface.
type WriteBatchHandler interface {
	Put(key []byte, val []byte)
	Delete(key []byte)
}

// MemtableInserter is a WriteBatchHandler. Applies WriteBatch to memtable.
type MemtableInserter struct {
	seq uint64
	mem *memtable.Memtable
}

func (s *MemtableInserter) Put(key []byte, val []byte) {
	s.mem.Add(s.seq, y.ValueTypeValue, key, val)
	s.seq++
}

func (s *MemtableInserter) Delete(key []byte) {
	s.mem.Add(s.seq, y.ValueTypeDeletion, key, y.EmptySlice)
	s.seq++
}
