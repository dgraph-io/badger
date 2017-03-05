package db

import (
	"bytes"
	"os"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// TableController corresponds to a file or table on disk.
type TableController struct {
	smallest, biggest []byte // Smallest and largest keys.
	fd                *os.File
	size              uint64 // Size of file.
	tbl               *table.Table
}

type LevelController struct {
	sync.RWMutex // For now, when merging a table from level L to L+1, we will lock level L+1.

	level int
	size  uint64
	tbl   []*TableController // For level>=1, keep this sorted and non-overlapping.
	next  *LevelController
}

// overlappingInputs returns a slice of s.tbm for tables that overlap with key range.
// Note that this is a closed interval [begin, end].
func (s *LevelController) overlappingInputs(begin, end []byte) []*TableController {
	y.AssertTrue(s.level > 0)
	var out []*TableController
	// Not that many files. Just do simple linear search. TODO: Consider binary search.
	for _, t := range s.tbl {
		if bytes.Compare(begin, t.biggest) <= 0 && bytes.Compare(end, t.smallest) >= 0 {
			out = append(out, t)
		}
	}
	return out
}

// sortTable sorts tables for the level. ASSUME write lock.
func (s *LevelController) sortTables() {
	y.AssertTrue(s.level >= 1)
	sort.Slice(s.tbl, func(i, j int) bool {
		return bytes.Compare(s.tbl[i].smallest, s.tbl[j].smallest) < 0
	})
}

// compact merges t:=s.tbl[idx] to the next level.
// While merging, t remains available for reading.
// No one should be allowed to merge into this level as it might write into t.
func (s *LevelController) compact(idx int) error {
	y.AssertTrue(s.next != nil)
	y.AssertTrue(idx >= 0 && idx < len(s.tbl))

	// t is table to be merged to next level.
	t := s.tbl[idx]

	//	s.next.Lock()
	//	defer s.next.Unlock()
	s.next.sortTables()

	inputs := s.next.overlappingInputs(t.smallest, t.biggest)
	if len(inputs) == 0 {
		// Just move file to next level. TODO: What to lock? Need to CAS?
		s.next.tbl = append(s.next.tbl, t) // Add first then delete.
		s.tbl = append(s.tbl[:idx], s.tbl[idx+1:]...)
		return
	}
	// There is at least one overlapping table.
	// If we exceed 
	it := t.tbl.NewIterator()
	it.SeekToFirst()
	if err := it.Error() != nil; err != nil {
		return err
	}
	for ; it.Valid(); it.Next() {
		if 
	}
	//	for _, input := range inputs {

	//	}
}
