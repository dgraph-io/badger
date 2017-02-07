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
	imm      *memtable.Memtable
	mem      *memtable.Memtable
	versions *VersionSet
}

func NewDB() *DB {
	// VersionEdit strongly tied to table files. Omit for now.
	db := &DB{
		versions: NewVersionSet(),
	}
	// TODO: Add TableCache here.
	return db
}

// Get looks for key and returns value. If not found, return nil.
func (s *DB) Get(key []byte) []byte {
	lkey := y.NewLookupKey(key, s.versions.lastSeq)
	if v := s.mem.Get(lkey); v != nil {
		return v
	}
	if v := s.imm.Get(lkey); v != nil {
		return v
	}
	// TODO: Call s.current.Get. It should use disk here.
	return nil
}
