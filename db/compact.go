package db

import (
	"bytes"
	//	"fmt"
	"io/ioutil"
	"os"
	"sort"
	//	"sync"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

const (
	baseSize  = 1 << 20
	maxLevels = 10
)

var (
	levels []*levelWrapper
)

type tableWrapper struct {
	smallest, biggest []byte // Smallest and largest keys.
	fd                *os.File
	table             *table.Table
	markedForDelete   bool
}

type levelWrapper struct {
	level   int
	maxSize int
	tables  []*tableWrapper
}

func init() {
	levels = make([]*levelWrapper, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i] = &levelWrapper{
			level: i,
		}
		if i == 0 {
			levels[i].maxSize = baseSize
		} else {
			levels[i].maxSize = levels[i-1].maxSize * 10
		}
	}
}

// overlappingTables returns a slice of s.tables that overlap with closed interval [begin, end].
func (s *levelWrapper) overlappingTables(begin, end []byte) []*tableWrapper {
	y.AssertTrue(s.level > 0)
	var out []*tableWrapper
	// Not that many files. Just do simple linear search. TODO: Consider binary search.
	for _, t := range s.tables {
		if bytes.Compare(begin, t.biggest) <= 0 && bytes.Compare(end, t.smallest) >= 0 {
			out = append(out, t)
		}
	}
	return out
}

// sortTable sorts tables for the level. ASSUME write lock.
func (s *levelWrapper) sortTables() {
	y.AssertTrue(s.level >= 1)
	sort.Slice(s.tables, func(i, j int) bool {
		return bytes.Compare(s.tables[i].smallest, s.tables[j].smallest) < 0
	})
}

// Will not be needed if we move ConcatIterator, MergingIterator to this package.
func getTables(tables []*tableWrapper) []*table.Table {
	var out []*table.Table
	for _, t := range tables {
		out = append(out, t.table)
	}
	return out
}

func newTableWrapper(f *os.File, t *table.Table) *tableWrapper {
	out := &tableWrapper{
		fd:    f,
		table: t,
	}
	it := t.NewIterator()
	it.SeekToFirst()
	y.AssertTrue(it.Valid())
	// KV interface is kind of awkward...
	it.KV(func(k, v []byte) {
		out.smallest = k
	})
	it.SeekToLast()
	y.AssertTrue(it.Valid())
	it.KV(func(k, v []byte) {
		out.biggest = k
	})
	// Make sure we did populate smallest and biggest.
	y.AssertTrue(len(out.smallest) > 0) // We do not allow empty keys...
	y.AssertTrue(len(out.biggest) > 0)
	return out
}

func doCopy(src []byte) []byte {
	out := make([]byte, len(src))
	y.AssertTrue(len(src) == copy(out, src))
	return out
}

// compact merges t:=s.tbl[idx] to the next level.
// While merging, t remains available for reading.
// No one should be allowed to merge into this level as it might write into t.
func (s *levelWrapper) compact(idx int) error {
	y.AssertTrue(s.level+1 < len(levels)) // Make sure s is not the last level.
	y.AssertTrue(idx >= 0 && idx < len(s.tables))

	// t is table to be merged to next level.
	t := s.tables[idx]
	nl := levels[s.level+1] // Next level.
	nl.sortTables()         // TODO: Not many items, but can avoid sorting.

	tables := nl.overlappingTables(t.smallest, t.biggest)
	if len(tables) == 0 {
		// No overlapping tables with next level. Just move this file to the next level.
		nl.tables = append(nl.tables, t)
		s.tables = append(s.tables[:idx], s.tables[idx+1:]...)
		return nil
	}

	for _, t := range tables {
		t.markedForDelete = true
	}
	it1 := table.NewConcatIterator([]*table.Table{t.table})
	it2 := table.NewConcatIterator(getTables(tables))
	it := table.NewMergingIterator(it1, it2)
	// Currently, when the iterator is constructed, we automatically SeekToFirst.
	// We may not want to do that.
	var newTables []*tableWrapper
	var builder table.TableBuilder
	builder.Reset()

	var lastKey []byte

	finishTable := func() error {
		fd, err := ioutil.TempFile("", "badger")
		if err != nil {
			return err
		}
		fd.Write(builder.Finish())
		builder.Reset()
		table, err := table.OpenTable(fd)
		if err != nil {
			return nil
		}
		newTables = append(newTables, newTableWrapper(fd, table))
		return nil
	}

	for ; it.Valid(); it.Next() {
		if builder.FinalSize() > nl.maxSize {
			if err := finishTable(); err != nil {
				return err
			}
		}
		kSlice, vSlice := it.KeyValue()
		// We need to make copies of these as table might use them as "last".
		key := doCopy(kSlice)
		val := doCopy(vSlice)
		//		fmt.Printf("key=%s val=%s lastKey=%s\n", string(key), string(val), string(lastKey))
		if bytes.Equal(key, lastKey) {
			// Ignore duplicate keys. The first iterator takes precedence.
			continue
		}
		if err := builder.Add(key, val); err != nil {
			return err
		}
		lastKey = key
	}
	if builder.Empty() {
		return nil
	}
	if err := finishTable(); err != nil {
		return err
	}
	// newTables is now populated. Add them to level "nl" and delete old tables. Need to lock...
	nlTables := nl.tables[:0]
	for _, t := range nl.tables {
		if !t.markedForDelete {
			nlTables = append(nlTables, t)
		}
	}
	nlTables = append(nlTables, newTables...)
	nl.tables = nlTables
	s.tables = append(s.tables[:idx], s.tables[idx+1:]...) // Delete table on this level.
	return nil
}
