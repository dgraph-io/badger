package db

import (
	"os"
	"sync"

	"github.com/dgraph-io/badger/memtable"
	"github.com/dgraph-io/badger/y"
)

type DBOptions struct {
	WriteBufferSize int
}

var DefaultDBOptions = &DBOptions{
	WriteBufferSize: 1 << 10,
}

type DB struct {
	imm       *memtable.Memtable // Immutable, memtable being flushed.
	mem       *memtable.Memtable
	versions  *VersionSet
	immWg     sync.WaitGroup // Nonempty when flushing immutable memtable.
	dbOptions DBOptions
}

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

func NewDB(opt *DBOptions) *DB {
	// VersionEdit strongly tied to table files. Omit for now.
	db := &DB{
		versions:  NewVersionSet(),
		mem:       memtable.NewMemtable(memtable.DefaultKeyComparator),
		dbOptions: *opt, // Make a copy.
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
	if err := s.makeRoomForWrite(); err != nil {
		return err
	}
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

func (s *DB) makeRoomForWrite() error {
	if s.mem.MemUsage() < s.dbOptions.WriteBufferSize {
		// Nothing to do. We have enough space.
		return nil
	}
	s.immWg.Wait() // Make sure we finish flushing immutable memtable.
	s.imm = s.mem
	s.mem = memtable.NewMemtable(memtable.DefaultKeyComparator)
	s.compactMemtable() // This is for imm.
	return nil
}

func (s *DB) compactMemtable() {
	y.AssertTrue(s.imm != nil)
	s.immWg.Add(1)
	go func() {
		defer s.immWg.Done()
		f, err := os.Open("/tmp/l0") // Fix later.
		y.Check(err)
		defer f.Close()
		y.Check(s.imm.WriteLevel0Table(f))
	}()
}
