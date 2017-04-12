package badger

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

type tableHandler struct {
	ref int32 // For file garbage collection.

	// The following are initialized once and const.
	smallest, biggest []byte       // Smallest and largest keys.
	fd                *os.File     // Owns fd.
	table             *table.Table // Does not own fd.
	id                uint64
}

// tableIterator is a thin wrapper around table.TableIterator.
// For example, it does reference counting.
type tableIterator struct {
	table *tableHandler
	it    y.Iterator // From the actual table.
}

func (s *tableIterator) Next()                      { s.it.Next() }
func (s *tableIterator) SeekToFirst()               { s.it.SeekToFirst() }
func (s *tableIterator) Seek(key []byte)            { s.it.Seek(key) }
func (s *tableIterator) KeyValue() ([]byte, []byte) { return s.it.KeyValue() }
func (s *tableIterator) Valid() bool                { return s.it.Valid() }
func (s *tableIterator) Name() string               { return "TableHandlerIterator" }

func (s *tableIterator) Close() {
	s.it.Close()
	s.table.decrRef() // Important.
}

func (s *tableHandler) newIterator() y.Iterator {
	s.incrRef() // Important.
	return &tableIterator{
		table: s,
		it:    s.table.NewIterator(),
	}
}

func (s *tableHandler) incrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *tableHandler) decrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef == 0 {
		// We can safely delete this file, because for all the current files, we always have
		// at least one reference pointing to them.
		filename := s.fd.Name()
		y.Check(s.fd.Close())
		os.Remove(filename)
	}
}

func (s *tableHandler) size() int64 { return s.table.Size() }

// newTableHandler returns a new table given file. Please remember to decrRef.
func newTableHandler(id uint64, f *os.File) (*tableHandler, error) {
	t, err := table.OpenTable(f)
	if err != nil {
		return nil, err
	}
	out := &tableHandler{
		id:    id,
		fd:    f,
		table: t,
		ref:   1, // Caller is given one reference.
	}

	it := t.NewIterator()
	it.SeekToFirst()
	y.AssertTrue(it.Valid())
	out.smallest, _ = it.KeyValue()

	// TODO: We shouldn't need to create another iterator.
	it2 := t.NewIterator() // For now, safer to use a different iterator.
	it2.SeekToLast()
	y.AssertTrue(it2.Valid())
	out.biggest, _ = it2.KeyValue()

	// Make sure we did populate smallest and biggest.
	y.AssertTrue(len(out.smallest) > 0) // We do not allow empty keys...
	y.AssertTrue(len(out.biggest) > 0)
	// It is possible that smallest=biggest. In that case, table has only one element.
	return out, nil
}

// tempFile returns a unique filename and the uint64.
func tempFile(dir string) (uint64, *os.File) {
	var id uint64
	for {
		id = rand.Uint64()
		name := fmt.Sprintf("table_%016x", id)
		filename := filepath.Join(dir, name)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			// File does not exist.
			fd, err := os.Create(filename)
			y.Check(err)
			return id, fd
		}
	}
}
