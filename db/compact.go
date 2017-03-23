/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package db

import (
	"bytes"
	"fmt"
	"io"
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
	NumLevelZeroTables int   // Maximum number of Level 0 tables before we start compacting.
	LevelOneSize       int64 // Maximum total size for Level 1.
	MaxLevels          int   // Maximum number of levels of compaction. May be made variable later.
	NumCompactWorkers  int   // Number of goroutines ddoing compaction.
	MaxTableSize       int64 // Each table (or file) is at most this size.
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
	opt          CompactOptions
}

type levelsController struct {
	// Guards beingCompacted.
	sync.Mutex

	beingCompacted []bool

	// The following are initialized once and const.
	levels []*levelHandler
	opt    CompactOptions
}

func DefaultCompactOptions() CompactOptions {
	return CompactOptions{
		NumLevelZeroTables: 3,
		LevelOneSize:       5 << 20,
		MaxLevels:          10,
		NumCompactWorkers:  3,
		MaxTableSize:       50 << 20,
	}
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
	out.smallest, _ = it.KeyValue()

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
	fmt.Printf("Deleting table: level=%d idx=%d\n", s.level, idx)
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
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// For level 0, return the oldest table.
		return 0
	}

	// For other levels, pick the largest table.
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

// getFirstTable returns the first table. If level is empty, we return nil.
func (s *levelHandler) getFirstTable() *tableHandler {
	s.RLock()
	defer s.RUnlock()
	if len(s.tables) == 0 {
		return nil
	}
	return s.tables[0]
}

func (s *levelHandler) getTable(idx int) *tableHandler {
	s.RLock()
	defer s.RUnlock()
	y.AssertTruef(0 <= idx && idx < len(s.tables), "level=%d idx=%d len=%d",
		s.level, idx, len(s.tables))
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

func newLevelsController(opt CompactOptions) *levelsController {
	s := &levelsController{
		opt:            opt,
		levels:         make([]*levelHandler, opt.MaxLevels),
		beingCompacted: make([]bool, opt.MaxLevels),
	}
	for i := 0; i < s.opt.MaxLevels; i++ {
		s.levels[i] = &levelHandler{
			level: i,
			opt:   s.opt,
		}
		if i == 0 {
			// Do nothing.
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
	return s
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

	///////// Some temporary logging here.
	for i := 0; i < s.opt.MaxLevels; i++ {
		var busy int
		if s.beingCompacted[i] {
			busy = 1
		}
		fmt.Printf("(i=%d, size=%d, busy=%d, numTables=%d) ", i, s.levels[i].getTotalSize(), busy, len(s.levels[i].tables))
	}
	fmt.Printf("\n")
	///////// End of temporary logging.

	for i := 0; i+1 < s.opt.MaxLevels; i++ {
		// Lower levels take priority. Most important is level 0. It should only have one table.
		// See if we want to compact i to i+1.
		if s.beingCompacted[i] || s.beingCompacted[i+1] {
			continue
		}
		if i == 0 && len(s.levels[0].tables) <= s.opt.NumLevelZeroTables {
			continue
		}
		//		if i == 0 {
		//			fmt.Printf("~~~level0 looks full: %d\n", len(s.levels[0].tables))
		//		}
		if i > 0 && s.levels[i].getTotalSize() <= s.levels[i].maxTotalSize {
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
	// We expect beingCompacted to be marked for the chosen two levels, in pickCompactLevel.
	if l < 0 {
		// Level is negative. Nothing to compact.
		fmt.Printf("tryCompact(worker=%d) nop\n", workerID)
		return
	}

	fmt.Printf("tryCompact(worker=%d): Merging level %d to %d\n", workerID, l, l+1)
	if err := s.doCompact(l); err != nil {
		log.Printf("tryCompact encountered an error: %+v", err)
		// Don't return yet. We need to unmark beingCompacted.
	}

	s.Lock()
	defer s.Unlock()
	s.beingCompacted[l] = false
	s.beingCompacted[l+1] = false
}

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < s.opt.MaxLevels) // Sanity check.
	thisLevel := s.levels[l]
	nextLevel := s.levels[l+1]
	tableIdx := thisLevel.pickCompactTable()
	t := thisLevel.getTable(tableIdx) // Want to compact away t.
	// In case you worry that levelHandler.tables[tableIdx] is no longer the same as before,
	// note that the code is delicate. These two levels are already marked for compaction, so
	// nobody should be able to mutate levelHandler.tables.
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

	it := y.NewMergeIterator([]y.Iterator{
		t.table.NewIterator(),
		table.NewConcatIterator(getTables(nextLevel.tables[left:right])),
	})
	// Currently, when the iterator is constructed, we automatically SeekToFirst.
	// We may not want to do that.
	var newTables []*tableHandler
	var builder table.TableBuilder
	builder.Reset()

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

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if int64(builder.FinalSize()) > s.opt.MaxTableSize {
			if err := finishTable(); err != nil {
				return err
			}
		}
		kSlice, vSlice := it.KeyValue()
		// Safer to make copies as table uses []byte and might overwrite.
		key := make([]byte, len(kSlice))
		val := make([]byte, len(vSlice))
		y.AssertTrue(len(kSlice) == copy(key, kSlice))
		y.AssertTrue(len(vSlice) == copy(val, vSlice))
		if err := builder.Add(key, val); err != nil {
			return err
		}
	}
	if !builder.Empty() {
		//		fmt.Printf("EndTable: largestKey=%s size=%d\n", string(lastKey), builder.FinalSize())
		if err := finishTable(); err != nil {
			return err
		}
	}

	nextLevel.replaceTables(left, right, newTables)
	y.AssertTrue(thisLevel.tables[tableIdx] == t) // We do not expect any change here.
	thisLevel.deleteTable(tableIdx)               // Function will acquire level lock.
	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.
	// Do a assert as a sanity check.
	y.AssertTrue(l != 0 || tableIdx == 0)
	fmt.Printf("Level %d: Replace table [%d, %d) with %d new tables\n", l+1, left, right, len(newTables))
	return nil
}

func (s *levelsController) addLevel0Table(t *tableHandler) {
	for !s.levels[0].tryAddLevel0Table(t) {
		fmt.Printf("Stalled on level 0\n")
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *levelHandler) tryAddLevel0Table(t *tableHandler) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	if len(s.tables) > s.opt.NumLevelZeroTables {
		return false
	}
	s.tables = append(s.tables, t)
	s.totalSize += t.size()
	return true
}

func (s *levelHandler) check() {
	if s.level == 0 {
		return
	}
	s.RLock()
	defer s.RUnlock()
	numTables := len(s.tables)
	for j := 1; j < numTables; j++ {
		y.AssertTruef(j < len(s.tables), "Level %d, j=%d numTables=%d", s.level, j, numTables)
		y.AssertTruef(bytes.Compare(s.tables[j-1].biggest, s.tables[j].smallest) < 0,
			"%s vs %s: level=%d j=%d numTables=%d",
			string(s.tables[j-1].biggest), string(s.tables[j].smallest), s.level, j, numTables)
		y.AssertTruef(bytes.Compare(s.tables[j].smallest, s.tables[j].biggest) <= 0,
			"%s vs %s: level=%d j=%d numTables=%d",
			string(s.tables[j].smallest), string(s.tables[j].biggest), s.level, j, numTables)
	}
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key []byte) []byte {
	// No need to lock anything as we just iterate over the currently immutable levelHandlers.
	for _, h := range s.levels {
		if v := h.get(key); v != nil {
			return v
		}
	}
	return nil
}

// getHelper acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getHelper(key []byte) []*tableHandler {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		out := make([]*tableHandler, len(s.tables))
		y.AssertTrue(len(s.tables) == copy(out, s.tables))
		return out
	}
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].biggest, key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil
	}
	return []*tableHandler{s.tables[idx]}
}

// get returns value for a given key. If not found, return nil.
func (s *levelHandler) get(key []byte) []byte {
	tables := s.getHelper(key)
	for _, th := range tables {
		it := th.table.NewIterator()
		it.Seek(key)
		if !it.Valid() {
			continue
		}
		itKey, itVal := it.KeyValue()
		if bytes.Equal(key, itKey) {
			return itVal
		}
	}
	return nil
}

// While iterating over a level, tables may be modified or deleted.
// To handle that, we keep a table iterator. When this table iterator is done, we lock the level
// and do another seek to find the next table.
type levelIterator struct {
	lh   *levelHandler
	iter *table.TableIterator
}

func (s *levelHandler) NewIterator() y.Iterator {
	if s.level > 0 {
		return &levelIterator{lh: s}
	}
	// For level 0, we will return a merge iterator across all tables. The newer table at the end
	// of levelHandler.tables should take precedence.
	// It's ok that new tables are added while we are iterating, because the overall MergeIterator
	// will include the current memtable(s) which will include the table being added.
	// It's ok that tables are being removed while we are iterating. They are being compacted to
	// the next level and will just appear twice, but will be consistent.
	s.RLock()
	defer s.RUnlock()
	iters := make([]y.Iterator, 0, len(s.tables))
	for i := len(s.tables) - 1; i >= 0; i-- {
		iters = append(iters, s.tables[i].table.NewIterator())
	}
	return y.NewMergeIterator(iters)
}

func (s *levelIterator) Valid() bool {
	return s.iter != nil && s.iter.Valid()
}

// findTable finds the first table such that table.biggest > or >= key.
func (s *levelHandler) findTable(key []byte, allowEqual bool) *tableHandler {
	s.RLock()
	defer s.RUnlock()

	var idx int
	if allowEqual {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return bytes.Compare(s.tables[i].biggest, key) >= 0
		})
	} else {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return bytes.Compare(s.tables[i].biggest, key) > 0
		})
	}
	if idx == len(s.tables) {
		return nil
	}
	return s.tables[idx]
}

func (s *levelIterator) KeyValue() ([]byte, []byte) {
	y.AssertTrue(s.iter.Valid())
	return s.iter.KeyValue()
}

func (s *levelIterator) SeekToFirst() {
	s.iter = nil
	tbl := s.lh.getFirstTable()
	if tbl == nil {
		// Leave iterator as invalid.
		return
	}
	s.iter = tbl.table.NewIterator()
	s.iter.SeekToFirst()
}

// Seek seeks to element with key >= given key.
func (s *levelIterator) Seek(key []byte) {
	s.iter = nil                    // Reset.
	th := s.lh.findTable(key, true) // allowEqual=true.
	if th == nil {
		return
	}
	// Note that while doing this table seek, we do not lock the level.
	s.iter = th.table.NewIterator()
	s.iter.Seek(key)
}

func (s *levelIterator) Next() {
	y.AssertTrue(s.Valid())
	currentKey, _ := s.KeyValue()
	s.iter.Next()
	if s.iter.Valid() {
		return // We can remain in the current table.
	}
	y.AssertTrue(s.iter.Error() == io.EOF) // Relax later.
	// Need to do a re-seek table. But this is not the usual seek.
	// allowEqual=false so that new table contains an element that is strictly > current key.
	// We do not like to do hacks like append the key with something like "0".
	th := s.lh.findTable(currentKey, false)
	if th == nil {
		return
	}
	s.iter = th.table.NewIterator()
	s.iter.Seek(currentKey) // We expect key to be found as table.biggest > key.
	y.AssertTrue(s.Valid())
	key, _ := s.iter.KeyValue()
	y.AssertTrue(key != nil)
	if bytes.Equal(key, currentKey) {
		// Currently, tableIterator is not able to seek to key strictly > given key.
		// Hence, we have to do an extra comparison.
		s.iter.Next()
		y.AssertTrue(s.Valid())
	}
}

// newIterators returns a list of level iterators.
// We could have returned one single MergeIterator but that has to be merged with memtable
// iterators as well. Let's do the merging only once, at the DB object level.
func (s *levelsController) newIterators() []y.Iterator {
	var out []y.Iterator
	for _, l := range s.levels {
		out = append(out, l.NewIterator())
	}
	return out
}
