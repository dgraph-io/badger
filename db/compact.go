package db

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

const (
	baseSize          = 1 << 20
	maxLevels         = 10
	numCompactWorkers = 3
	maxTableSize      = 50 << 20
)

type tableHandler struct {
	// The following are initialized once and const.
	smallest, biggest []byte       // Smallest and largest keys.
	fd                *os.File     // Owns fd.
	table             *table.Table // Does not own fd.
}

type levelHandler struct {
	// Guards tables, totalSize.
	sync.RWMutex

	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
	tables    []*tableHandler
	totalSize int64

	// The following are initialized once and const.
	level        int
	maxTotalSize int64
}

type levelsController struct {
	// Guards beingCompacted.
	sync.Mutex

	levels         []*levelHandler // Never changing for now. Initialized at start.
	beingCompacted []bool
}

var (
	lvlsController levelsController
)

func init() {
	lvlsController.init()
}

// doCopy creates a copy of []byte. Needed because table package uses []byte a lot.
func doCopy(src []byte) []byte {
	out := make([]byte, len(src))
	y.AssertTrue(len(src) == copy(out, src))
	return out
}

// Will not be needed if we move ConcatIterator, MergingIterator to this package.
func getTables(tables []*tableHandler) []*table.Table {
	var out []*table.Table
	for _, t := range tables {
		out = append(out, t.table)
	}
	return out
}

func newTableHandler(f *os.File) (*tableHandler, error) {
	t, err := table.OpenTable(f)
	if err != nil {
		return nil, err
	}
	out := &tableHandler{
		fd:    f,
		table: t,
	}
	it := t.NewIterator()
	it.SeekToFirst()
	y.AssertTrue(it.Valid())
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
	return out, nil
}

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

func (s *levelHandler) deleteTable(idx int) {
	s.Lock()
	defer s.Unlock()
	s.tables = append(s.tables[:idx], s.tables[idx+1:]...)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
func (s *levelHandler) replaceTables(left, right int, newTables []*tableHandler) {
	s.Lock()
	defer s.Unlock()
	// To be safe, just make a copy. TODO: Be more careful and avoid copying.
	numDeleted := right - left
	numAdded := len(newTables)
	tables := make([]*tableHandler, len(s.tables)-numDeleted+numAdded)
	y.AssertTrue(left == copy(tables, s.tables[:left]))
	t := tables[left:]
	y.AssertTrue(numAdded == copy(t, newTables))
	t = t[numAdded:]
	y.AssertTrue(len(s.tables[right:]) == copy(t, s.tables[right:]))
	s.tables = tables
}

func (s *levelHandler) pickCompactTable() int {
	y.AssertTrue(len(s.tables) > 0) // We expect at least one table to pick.
	s.RLock()
	defer s.RUnlock()
	var idx int
	mx := s.tables[0].table.Size()
	for i := 1; i < len(s.tables); i++ {
		size := s.tables[i].table.Size()
		if size > mx {
			mx = size
			idx = i
		}
	}
	return idx
}

func (s *levelHandler) getTable(idx int) *tableHandler {
	s.RLock()
	defer s.RUnlock()
	y.AssertTrue(0 <= idx && idx < len(s.tables))
	return s.tables[idx]
}

// overlappingTables returns the tables that intersect with key range.
// s.tables[left] to s.tables[right-1] overlap with the closed key-range [begin, end].
func (s *levelHandler) overlappingTables(begin, end []byte) (int, int) {
	s.RLock()
	defer s.RUnlock()

	y.AssertTrue(s.level > 0)
	// Binary search.
	left := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].biggest, begin) >= 0
	})
	right := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].smallest, end) > 0
	})
	return left, right
}

func (s *levelsController) init() {
	s.levels = make([]*levelHandler, maxLevels)
	for i := 0; i < maxLevels; i++ {
		s.levels[i] = &levelHandler{
			level: i,
		}
		if i == 0 {
			// For level 0, as long as there is a table there, we want to compact it away.
			// Otherwise, if there are too many tables on level 0, a query will be very expensive.
			s.levels[i].maxTotalSize = -1
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = baseSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * 10
		}
	}
	for i := 0; i < numCompactWorkers; i++ {
		go s.compact()
	}
}

func (s *levelsController) compact() {
	timeChan := time.Tick(time.Second)
	for {
		select {
		// Can add a done channel or other stuff.
		case <-timeChan:
			s.tryCompact()
		}
	}
}

// pickCompactLevel determines which level to compact. Return -1 if not found.
func (s *levelsController) pickCompactLevel() int {
	s.Lock() // For access to beingCompacted.
	defer s.Unlock()
	for i := 0; i+1 < maxLevels; i++ {
		// Lower levels take priority. Most important is level 0. It should only have one table.
		// See if we want to compact i to i+1.
		if s.beingCompacted[i] || s.beingCompacted[i+1] {
			continue
		}
		if s.levels[i].getTotalSize() < s.levels[i].maxTotalSize {
			continue
		}
		// Mark these two levels while locking s.
		s.beingCompacted[i] = true
		s.beingCompacted[i+1] = true
		return i
	}
	// Didn't find anything.
	return -1
}

func (s *levelsController) tryCompact() {
	l := s.pickCompactLevel()
	if l < 0 {
		// Level is negative. Nothing to compact.
		return
	}

	if err := s.doCompact(l); err != nil {
		log.Printf("tryCompact encountered an error: %+v", err)
		// Don't return yet. We need to unmark beingCompacted.
	}

	s.Lock()
	defer s.Unlock()
	s.beingCompacted[l] = false
	s.beingCompacted[l+1] = false
}

// doCompact compacts level l.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < maxLevels) // Sanity check.
	thisLevel := s.levels[l]
	nextLevel := s.levels[l+1]
	tableIdx := thisLevel.pickCompactTable()
	t := thisLevel.getTable(tableIdx) // Want to compact away t.
	left, right := nextLevel.overlappingTables(t.smallest, t.biggest)
	// Merge t with tables[left:right]. Excludes tables[right].
	if left >= right {
		// No overlap with the next level. Just move the file down to the next level.
		y.AssertTrue(left == right)
		nextLevel.replaceTables(left, right, []*tableHandler{t}) // Function will acquire level lock.
		y.AssertTrue(thisLevel.tables[tableIdx] == t)            // We do not expect any change here.
		thisLevel.deleteTable(tableIdx)                          // Function will acquire level lock.
		return nil
	}

	it1 := table.NewConcatIterator([]*table.Table{t.table})
	it2 := table.NewConcatIterator(getTables(nextLevel.tables[left:right]))
	it := table.NewMergingIterator(it1, it2)
	// Currently, when the iterator is constructed, we automatically SeekToFirst.
	// We may not want to do that.
	var newTables []*tableHandler
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
		newTable, err := newTableHandler(fd)
		newTables = append(newTables, newTable)
		return nil
	}

	for ; it.Valid(); it.Next() {
		if builder.FinalSize() > maxTableSize {
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
	y.AssertTrue(!builder.Empty())
	if err := finishTable(); err != nil {
		return err
	}

	nextLevel.replaceTables(left, right, newTables)
	y.AssertTrue(thisLevel.tables[tableIdx] == t) // We do not expect any change here.
	thisLevel.deleteTable(tableIdx)               // Function will acquire level lock.
	return nil
}
