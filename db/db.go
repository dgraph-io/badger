package db

import (
	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/y"
)

type Version struct {
	vset *VersionSet
	next *Version
	prev *Version
}

func NewVersion(vset *VersionSet) *Version {
	v := &Version{vset: vset}
	v.next = v
	v.prev = v
	return v
}

// Version.Get should wrap over table.

type VersionSet struct {
	lastSeq      uint64
	dummyVersion *Version // Head of circular doubly-linked list of versions
	current      *Version // dummyVersions.prev
}

func (s *VersionSet) setLastSequence(seq uint64) {
	y.AssertTrue(seq >= s.lastSeq)
	s.lastSeq = seq
}

// NewVersionSet creates new VersionSet.
func NewVersionSet() *VersionSet {
	vset := new(VersionSet)
	vset.dummyVersion = NewVersion(vset)
	vset.AppendVersion(NewVersion(vset))
	return vset
}

// AppendVersion appends v to the end of our linked list.
func (s *VersionSet) AppendVersion(v *Version) {
	s.current = v
	v.prev = s.dummyVersion.prev
	v.next = s.dummyVersion
	v.prev.next = v
	v.next.prev = v
}

type DB struct {
	imm      *memtable.Memtable // Immutable, memtable being flushed.
	mem      *memtable.Memtable
	versions *VersionSet
}

func NewDB() *DB {
	// VersionEdit strongly tied to table files. Omit for now.
	db := &DB{
		versions: NewVersionSet(),
		mem:      memtable.NewMemtable(memtable.DefaultKeyComparator),
	}
	// TODO: Add TableCache here.
	return db
}

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	// TODO: Allow snapshotting. Set snapshot here.
	snapshot := s.versions.lastSeq
	lkey := y.NewLookupKey(key, snapshot)
	if v, hit := s.mem.Get(lkey); hit {
		return v
	}
	if v, hit := s.imm.Get(lkey); hit {
		return v
	}
	// TODO: Call s.current.Get. It should use disk here.
	return nil
}

// Write applies a WriteBatch.
func (s *DB) Write(wb *WriteBatch) error {
	// Parallelize this later.
	wb.SetSequence(s.versions.lastSeq + 1)
	s.versions.lastSeq += uint64(wb.Count())
	if err := wb.InsertInto(s.mem); err != nil {
		return err
	}
	return nil
}

// Put puts a key-val pair.
func (s *DB) Put(key []byte, val []byte) error {
	wb := NewWriteBatch(0)
	wb.Put(key, val)
	return s.Write(wb)
}

// Delete deletes a key.
func (s *DB) Delete(key []byte) error {
	wb := NewWriteBatch(0)
	wb.Delete(key)
	return s.Write(wb)
}

func (s *DB) MakeRoomForWrite() {
	// TODO: Check memory usage. Swap mem_ and imm_.
}
