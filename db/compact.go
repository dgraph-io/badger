package db

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

type CompactOptions struct {
	LevelOneSize      int64
	MaxLevels         int
	NumCompactWorkers int
	MaxTableSize      int64
}

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

	beingCompacted []bool

	// The following are initialized once and const.
	levels []*levelHandler
	opt    CompactOptions
}

var (
	lvlsController levelsController
)

func DefaultCompactOptions() *CompactOptions {
	return &CompactOptions{
		LevelOneSize:      1 << 20,
		MaxLevels:         10,
		NumCompactWorkers: 3,
		MaxTableSize:      50 << 20,
	}
}

func InitCompact(opt *CompactOptions) {
	lvlsController.init(opt)
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

func (s *tableHandler) size() int64 { return s.table.Size() }

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

func (s *levelHandler) deleteTable(idx int) {
	s.Lock()
	defer s.Unlock()
	t := s.tables[idx]
	s.totalSize -= t.size()
	s.tables = append(s.tables[:idx], s.tables[idx+1:]...)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
func (s *levelHandler) replaceTables(left, right int, newTables []*tableHandler) {
	s.Lock()
	defer s.Unlock()

	// Update totalSize first.
	for _, tbl := range newTables {
		s.totalSize += tbl.size()
	}
	for i := left; i < right; i++ {
		s.totalSize -= s.tables[i].size()
	}

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
	mx := s.tables[0].size()
	for i := 1; i < len(s.tables); i++ {
		size := s.tables[i].size()
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

func (s *levelsController) init(opt *CompactOptions) {
	if opt == nil {
		s.opt = *DefaultCompactOptions()
	} else {
		s.opt = *opt
	}
	s.levels = make([]*levelHandler, s.opt.MaxLevels)
	s.beingCompacted = make([]bool, s.opt.MaxLevels)
	for i := 0; i < s.opt.MaxLevels; i++ {
		s.levels[i] = &levelHandler{
			level: i,
		}
		if i == 0 {
			// For level 0, as long as there is a table there, we want to compact it away.
			// Otherwise, if there are too many tables on level 0, a query will be very expensive.
			s.levels[i].maxTotalSize = 0
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = s.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * 10
		}
	}
	for i := 0; i < s.opt.NumCompactWorkers; i++ {
		go s.compact(i)
	}
}

func (s *levelsController) compact(workerID int) {
	timeChan := time.Tick(100 * time.Millisecond)
	for {
		select {
		// Can add a done channel or other stuff.
		case <-timeChan:
			s.tryCompact(workerID)
		}
	}
}

// pickCompactLevel determines which level to compact. Return -1 if not found.
func (s *levelsController) pickCompactLevel() int {
	s.Lock() // For access to beingCompacted.
	defer s.Unlock()

	// Some temporary logging here.
	for i := 0; i < s.opt.MaxLevels; i++ {
		var busy int
		if s.beingCompacted[i] {
			busy = 1
		}
		fmt.Printf("(i=%d, size=%d, busy=%d, numTables=%d) ", i, s.levels[i].getTotalSize(), busy, len(s.levels[i].tables))
	}
	fmt.Printf("\n")

	for i := 0; i+1 < s.opt.MaxLevels; i++ {
		// Lower levels take priority. Most important is level 0. It should only have one table.
		// See if we want to compact i to i+1.
		if s.beingCompacted[i] || s.beingCompacted[i+1] {
			continue
		}
		if s.levels[i].getTotalSize() <= s.levels[i].maxTotalSize {
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

func (s *levelsController) tryCompact(workerID int) {
	l := s.pickCompactLevel()
	if l < 0 {
		// Level is negative. Nothing to compact.
		fmt.Printf("tryCompact(%d) nop\n", workerID)
		return
	}

	fmt.Printf("tryCompact(%d): Merging level %d to %d\n", workerID, l, l+1)
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
	y.AssertTrue(l+1 < s.opt.MaxLevels) // Sanity check.
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
		fmt.Printf("Merge: Move table from level %d to %d\n", l, l+1)
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
		if int64(builder.FinalSize()) > s.opt.MaxTableSize {
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
	fmt.Printf("Merge: Replace table %d to %d with %d new tables\n", left, right, len(newTables))
	return nil
}
