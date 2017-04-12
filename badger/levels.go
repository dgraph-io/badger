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

package badger

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

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
	opt          Options
}

type levelsController struct {
	// Guards beingCompacted.
	sync.Mutex

	beingCompacted []bool

	// The following are initialized once and const.
	levels []*levelHandler
	opt    Options
}

var (
	// This is for getting timings between stalls.
	lastUnstalled time.Time
)

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

// deleteTables remove tables idx0, ..., idx1-1.
func (s *levelHandler) deleteTables(idx0, idx1 int) {
	s.Lock()
	defer s.Unlock()
	for i := idx0; i < idx1; i++ {
		s.totalSize -= s.tables[i].size()
		s.tables[i].decrRef()
	}
	s.tables = append(s.tables[:idx0], s.tables[idx1:]...)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
func (s *levelHandler) replaceTables(left, right int, newTables []*tableHandler) {
	s.Lock()
	defer s.Unlock()

	// Update totalSize first.
	for _, tbl := range newTables {
		s.totalSize += tbl.size()
		tbl.incrRef()
	}
	for i := left; i < right; i++ {
		s.totalSize -= s.tables[i].size()
		s.tables[i].decrRef()
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

// pickCompactTables returns a range of tables to be compacted away.
func (s *levelHandler) pickCompactTables() (int, int) {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// For now, for level 0, we return all the tables.
		// Note that during compaction, s.tables might grow longer. This is fine. The indices into
		// s.tables remain valid because these new tables are appended to the back of s.tables.
		return 0, len(s.tables)
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
	return idx, idx + 1
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

func newLevelHandler(opt Options, level int) *levelHandler {
	return &levelHandler{
		level: level,
		opt:   opt,
	}
}

func newLevelsController(opt Options) *levelsController {
	y.AssertTrue(opt.NumLevelZeroTablesStall > opt.NumLevelZeroTables)
	s := &levelsController{
		opt:            opt,
		levels:         make([]*levelHandler, opt.MaxLevels),
		beingCompacted: make([]bool, opt.MaxLevels),
	}
	for i := 0; i < s.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(opt, i)
		if i == 0 {
			// Do nothing.
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = s.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(opt.LevelSizeMultiplier)
		}
	}
	for i := 0; i < s.opt.NumCompactWorkers; i++ {
		go s.runWorker(i)
	}
	return s
}

func (s *levelsController) runWorker(workerID int) {
	if s.opt.DoNotCompact {
		fmt.Println("NOT running any compactions due to DB options.")
		return
	}

	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	timeChan := time.Tick(10 * time.Millisecond)
	for {
		select {
		// Can add a done channel or other stuff.
		case <-timeChan:
			s.tryCompact(workerID)
		}
	}
}

// Requires read lock over levelsController.
func (s *levelsController) debugPrint() {
	s.Lock()
	defer s.Unlock()
	for i := 0; i < s.opt.MaxLevels; i++ {
		var busy int
		if s.beingCompacted[i] {
			busy = 1
		}
		fmt.Printf("(i=%d, size=%d, busy=%d, numTables=%d) ", i, s.levels[i].getTotalSize(), busy, len(s.levels[i].tables))
	}
	fmt.Printf("\n")
}

// pickCompactLevel determines which level to compact. Return -1 if not found.
func (s *levelsController) pickCompactLevel() int {
	s.Lock() // For access to beingCompacted.
	defer s.Unlock()

	// Going from higher levels to lower levels offers a small gain.
	// It probably has to do with level 1 being smaller when level 0 has to merge with the whole of
	// level 1.
	for i := s.opt.MaxLevels - 2; i >= 0; i-- {
		// Lower levels take priority. Most important is level 0. It should only have one table.
		// See if we want to compact i to i+1.
		if s.beingCompacted[i] || s.beingCompacted[i+1] {
			continue
		}
		if (i == 0 && len(s.levels[0].tables) > s.opt.NumLevelZeroTables) ||
			(i > 0 && s.levels[i].getTotalSize() > s.levels[i].maxTotalSize) {
			s.beingCompacted[i], s.beingCompacted[i+1] = true, true
			return i
		}
	}
	// Didn't find anything that is really bad.
	// Let's do level 0 if it is not empty. Let's do a level that is close to its maxTotalSize.
	if s.levels[0].getTotalSize() > 0 && !s.beingCompacted[0] && !s.beingCompacted[1] {
		s.beingCompacted[0], s.beingCompacted[1] = true, true
		return 0
	}
	// Doing work preemptively seems to make us slower.
	// If you want to try that, add a weaker condition here such as >0.75*totalSize.
	return -1
}

func (s *levelsController) tryCompact(workerID int) {
	l := s.pickCompactLevel()
	// We expect pickCompactLevel to read and update beingCompacted.
	if l < 0 {
		return
	}
	y.Check(s.doCompact(l)) // May relax check later.
	s.Lock()
	defer s.Unlock()
	s.beingCompacted[l] = false
	s.beingCompacted[l+1] = false
}

func (s *levelHandler) keyRange(idx0, idx1 int) ([]byte, []byte) {
	y.AssertTrue(idx0 < idx1)
	s.RLock() // For access to s.tables.
	defer s.RUnlock()
	smallest := s.tables[idx0].smallest
	biggest := s.tables[idx0].biggest
	for i := idx0 + 1; i < idx1; i++ {
		if bytes.Compare(s.tables[i].smallest, smallest) < 0 {
			smallest = s.tables[i].smallest
		}
		if bytes.Compare(s.tables[i].biggest, biggest) > 0 {
			biggest = s.tables[i].biggest
		}
	}
	return smallest, biggest
}

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < s.opt.MaxLevels) // Sanity check.
	timeStart := time.Now()
	thisLevel := s.levels[l]
	nextLevel := s.levels[l+1]
	srcIdx0, srcIdx1 := thisLevel.pickCompactTables()
	smallest, biggest := thisLevel.keyRange(srcIdx0, srcIdx1)
	// In case you worry that levelHandler.tables[tableIdx] is no longer the same as before,
	// note that the code is delicate. These two levels are already marked for compaction, so
	// nobody should be able to mutate levelHandler.tables.
	left, right := nextLevel.overlappingTables(smallest, biggest)
	// Merge thisLevel's selected tables with nextLevel.tables[left:right]. Excludes tables[right].
	y.AssertTrue(left <= right)
	if (thisLevel.level >= 1) && left == right {
		// No overlap with the next level. Just move the file(s) down to the next level.
		// Function will acquire level lock.
		nextLevel.replaceTables(left, right, thisLevel.tables[srcIdx0:srcIdx1])
		thisLevel.deleteTables(srcIdx0, srcIdx1) // Function will acquire level lock.
		if s.opt.Verbose {
			fmt.Printf("Merge: Move table [%d,%d) from level %d to %d\n", srcIdx0, srcIdx1, l, l+1)
		}
		return nil
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	var iters []y.Iterator
	if l == 0 {
		iters = appendIteratorsReversed(iters, thisLevel.tables[srcIdx0:srcIdx1])
	} else {
		y.AssertTrue(srcIdx0+1 == srcIdx1) // For now, for level >=1, we expect exactly one table.
		iters = []y.Iterator{thisLevel.tables[srcIdx0].newIterator()}
	}
	iters = append(iters, newConcatIterator(nextLevel.tables[left:right]))
	it := y.NewMergeIterator(iters)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.SeekToFirst()

	var newTables [200]*tableHandler
	var wg sync.WaitGroup
	var i int
	for ; it.Valid(); i++ {
		y.AssertTruef(i < len(newTables), "Rewriting too many tables: %d %d", i, len(newTables))
		builder := table.NewTableBuilder()
		for ; it.Valid(); it.Next() {
			if int64(builder.FinalSize()) > s.opt.MaxTableSize {
				break
			}
			key, val := it.KeyValue()
			if err := builder.Add(key, val); err != nil {
				builder.Close()
				return err
			}
		}
		if builder.Empty() {
			builder.Close()
			continue
		}

		wg.Add(1)
		go func(idx int, builder *table.TableBuilder) {
			defer builder.Close()
			defer wg.Done()

			fileID, fd := tempFile(s.opt.Dir)
			fd.Write(builder.Finish())
			var err error
			newTables[idx], err = newTableHandler(fileID, fd)
			// decrRef is added below.
			y.Check(err)
		}(i, builder)
	}
	wg.Wait()

	newTablesSlice := newTables[:i]
	defer func() {
		for _, t := range newTablesSlice {
			t.decrRef() // replaceTables will increment reference.
		}
	}()

	if s.opt.Verbose {
		fmt.Printf("LOG Compact %d->%d: Del [%d,%d), Del [%d,%d), Add [%d,%d), took %v\n",
			l, l+1, srcIdx0, srcIdx1, left, right, left, left+len(newTablesSlice), time.Since(timeStart))
	}
	nextLevel.replaceTables(left, right, newTablesSlice)
	thisLevel.deleteTables(srcIdx0, srcIdx1) // Function will acquire level lock.
	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.
	return nil
}

func (s *levelsController) addLevel0Table(t *tableHandler) {
	for !s.levels[0].tryAddLevel0Table(t, s.opt.Verbose) {
		// Stall. Make sure all levels are healthy before we unstall.
		var timeStart time.Time
		if s.opt.Verbose {
			fmt.Printf("STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: %v\n",
				time.Since(lastUnstalled))
			s.debugPrint()
			timeStart = time.Now()
		}
		// Before we unstall, we need to make sure that level 0 and 1 are healthy. Otherwise, we
		// will very quickly fill up level 0 again and if the compaction strategy favors level 0,
		// then level 1 is going to super full.
		for {
			if s.levels[0].getTotalSize() > 0 && s.levels[1].getTotalSize() > s.levels[1].maxTotalSize {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if s.opt.Verbose {
			fmt.Printf("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: %v\n",
				time.Since(timeStart))
			s.debugPrint()
			lastUnstalled = time.Now()
		}
	}
}

// tryAddLevel0Table returns true if ok and no stalling.
func (s *levelHandler) tryAddLevel0Table(t *tableHandler, verbose bool) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	if len(s.tables) > s.opt.NumLevelZeroTablesStall {
		return false
	}

	s.tables = append(s.tables, t)
	t.incrRef()
	s.totalSize += t.size()

	if verbose {
		fmt.Printf("Num level 0 tables increased from %d to %d\n", len(s.tables)-1, len(s.tables))
	}
	return true
}

// Check does some sanity check on one level of data or in-memory index.
func (s *levelHandler) Check() {
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

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getTableForKey(key []byte) []*tableHandler {
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
	tables := s.getTableForKey(key)
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

func appendIteratorsReversed(out []y.Iterator, th []*tableHandler) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].newIterator())
	}
	return out
}

func newConcatIterator(th []*tableHandler) y.Iterator {
	iters := make([]y.Iterator, 0, len(th))
	for _, t := range th {
		// This will increment the reference of the table handler.
		iters = append(iters, t.newIterator())
	}
	return y.NewConcatIterator(iters)
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelHandler) appendIterators(iters []y.Iterator) []y.Iterator {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		return appendIteratorsReversed(iters, s.tables)
	}
	return append(iters, newConcatIterator(s.tables))
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(iters []y.Iterator) []y.Iterator {
	for _, level := range s.levels {
		iters = level.appendIterators(iters)
	}
	return iters
}

// Check does some sanity check on our levels data or in-memory index for all levels.
func (s *levelsController) Check() {
	for _, l := range s.levels {
		l.Check()
	}
}
