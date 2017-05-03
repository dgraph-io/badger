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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
	tables    []*table.Table
	totalSize int64

	// The following are initialized once and const.
	level        int
	maxTotalSize int64
	kv           *KV
}

type levelsController struct {
	// Guards beingCompacted.
	sync.Mutex

	beingCompacted []bool

	// The following are initialized once and const.
	levels []*levelHandler
	clog   compactLog
	kv     *KV

	// Atomic.
	maxFileID    uint64 // Next ID to be used.
	maxCompactID uint64

	// For ending compactions.
	compactWorkersDone chan struct{}
	compactWorkersWg   sync.WaitGroup
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

// initTables replaces s.tables with given tables. This is done during loading.
func (s *levelHandler) initTables(tables []*table.Table) {
	s.Lock()
	defer s.Unlock()
	s.tables = tables
	s.totalSize = 0
	for _, t := range tables {
		s.totalSize += t.Size()
	}
	if s.level == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(s.tables, func(i, j int) bool {
			return s.tables[i].ID() < s.tables[j].ID()
		})
	} else {
		sort.Slice(s.tables, func(i, j int) bool {
			return bytes.Compare(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
		})
	}
}

// deleteTables remove tables idx0, ..., idx1-1.
func (s *levelHandler) deleteTables(toDel []*table.Table) {
	s.Lock()
	defer s.Unlock()
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}
	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table.Table
	for _, t := range s.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.totalSize -= t.Size()
		t.DecrRef()
	}
	s.tables = newTables
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
func (s *levelHandler) replaceTables(newTables []*table.Table) {
	s.Lock()
	defer s.Unlock()
	// Need to re-search the range of tables in this level to be replaced as
	// other goroutines might be changing it as well.
	y.AssertTrue(len(newTables) > 0)

	// Increase totalSize first.
	for _, tbl := range newTables {
		s.totalSize += tbl.Size()
		tbl.IncrRef()
	}

	left, right := overlappingTables(
		newTables[0].Smallest(), newTables[len(newTables)-1].Biggest(),
		s.tables)
	// Update totalSize and reference counts.
	for i := left; i < right; i++ {
		s.totalSize -= s.tables[i].Size()
		s.tables[i].DecrRef()
	}

	// To be safe, just make a copy. TODO: Be more careful and avoid copying.
	numDeleted := right - left
	numAdded := len(newTables)
	tables := make([]*table.Table, len(s.tables)-numDeleted+numAdded)
	y.AssertTrue(left == copy(tables, s.tables[:left]))
	t := tables[left:]
	y.AssertTrue(numAdded == copy(t, newTables))
	t = t[numAdded:]
	y.AssertTrue(len(s.tables[right:]) == copy(t, s.tables[right:]))
	s.tables = tables
}

// pickCompactTables returns a range of tables to be compacted away.
func (s *levelHandler) pickCompactTables() (int, int) {
	s.RLock() // Not really necessary.
	defer s.RUnlock()

	if s.level == 0 {
		// For now, for level 0, we return all the tables.
		// Note that during compaction, s.tables might grow longer. This is fine. The indices into
		// s.tables remain valid because these new tables are appended to the back of s.tables.
		return 0, len(s.tables)
	}

	// For other levels, pick the largest table.
	var idx int
	mx := s.tables[0].Size()
	for i := 1; i < len(s.tables); i++ {
		size := s.tables[i].Size()
		if size > mx {
			mx = size
			idx = i
		}
	}
	return idx, idx + 1
}

func newLevelHandler(kv *KV, level int) *levelHandler {
	return &levelHandler{
		level: level,
		kv:    kv,
	}
}

func newLevelsController(kv *KV) *levelsController {
	y.AssertTrue(kv.opt.NumLevelZeroTablesStall > kv.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:             kv,
		levels:         make([]*levelHandler, kv.opt.MaxLevels),
		beingCompacted: make([]bool, kv.opt.MaxLevels),
	}

	for i := 0; i < kv.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(kv, i)
		if i == 0 {
			// Do nothing.
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = kv.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(kv.opt.LevelSizeMultiplier)
		}
	}

	// Replay compact log. Check against files in directory.
	clogName := filepath.Join(kv.opt.Dir, "clog")
	_, err := os.Stat(clogName)
	if err == nil {
		y.Printf("Replaying compact log: %s\n", clogName)
		compactLogReplay(clogName, kv.opt.Dir, getIDMap(kv.opt.Dir))
		y.Check(os.Remove(clogName)) // Everything is ok. Clear compact log.
	}

	// Some files may be deleted. Let's reload.
	tables := make([][]*table.Table, kv.opt.MaxLevels)
	for fileID := range getIDMap(kv.opt.Dir) {
		fd, err := y.OpenSyncedFile(table.NewFilename(fileID, kv.opt.Dir), true)
		y.Check(err)
		t, err := table.OpenTable(fd, kv.opt.MapTablesTo)
		y.Check(err)

		// Check metadata for level information.
		tableMeta := t.Metadata()
		y.AssertTrue(len(tableMeta) == 2)
		level := int(binary.BigEndian.Uint16(tableMeta))
		y.AssertTrue(level < kv.opt.MaxLevels)
		tables[level] = append(tables[level], t)

		if fileID > s.maxFileID {
			s.maxFileID = fileID
		}
	}
	s.maxFileID++
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}
	//	s.debugPrintMore()
	s.validate() // Make sure key ranges do not overlap etc.

	// Create new compact log.
	s.clog.init(clogName)
	return s
}

func (s *levelsController) startCompact() {
	n := s.kv.opt.MaxLevels / 2
	s.compactWorkersDone = make(chan struct{}, n)
	s.compactWorkersWg.Add(n)
	for i := 0; i < n; i++ {
		go s.runWorker(i)
	}
}

func (s *levelsController) runWorker(workerID int) {
	defer s.compactWorkersWg.Done() // Indicate worker is done.
	if s.kv.opt.DoNotCompact {
		fmt.Println("NOT running any compactions due to DB options.")
		return
	}

	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	timeChan := time.Tick(10 * time.Millisecond)
	var done bool
	for !done {
		select {
		// Can add a done channel or other stuff.
		case <-timeChan:
			s.tryCompact(workerID)
		case <-s.compactWorkersDone:
			done = true
		}
	}
}

// pickCompactLevel determines which level to compact. Return -1 if not found.
func (s *levelsController) pickCompactLevel() int {
	s.Lock() // For access to beingCompacted.
	defer s.Unlock()

	// Going from higher levels to lower levels offers a small gain.
	// It probably has to do with level 1 being smaller when level 0 has to merge with the whole of
	// level 1.
	for i := s.kv.opt.MaxLevels - 2; i >= 0; i-- {
		// Lower levels take priority. Most important is level 0. It should only have one table.
		// See if we want to compact i to i+1.
		if s.beingCompacted[i] || s.beingCompacted[i+1] {
			continue
		}
		if (i == 0 && s.levels[0].numTables() > s.kv.opt.NumLevelZeroTables) ||
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

// compactBuildTables merge topTables and botTables to form a list of new tables.
func (s *levelsController) compactBuildTables(
	l int, topTables, botTables []*table.Table, c *compaction) ([]*table.Table, func()) {
	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	var iters []y.Iterator
	if l == 0 {
		iters = appendIteratorsReversed(iters, topTables, false)
	} else {
		y.AssertTrue(len(topTables) == 1)
		iters = []y.Iterator{topTables[0].NewIterator(false)}
	}

	// Load to RAM for tables to be deleted.
	//	for _, tbl := range topTables {
	//		tbl.LoadToRAM()
	//	}
	//	for _, tbl := range botTables {
	//		tbl.LoadToRAM()
	//	}

	iters = append(iters, table.NewConcatIterator(botTables, false))
	it := y.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	newTables := make([]*table.Table, len(c.toInsert))
	var wg sync.WaitGroup
	var i int
	newIDMin, newIDMax := c.toInsert[0], c.toInsert[len(c.toInsert)-1]
	newID := newIDMin
	for ; it.Valid(); i++ {
		y.AssertTruef(i < len(newTables), "Rewriting too many tables: %d %d", i, len(newTables))
		timeStart := time.Now()
		builder := table.NewTableBuilder()
		for ; it.Valid(); it.Next() {
			if builder.ReachedCapacity(s.kv.opt.MaxTableSize) {
				break
			}
			y.Check(builder.Add(it.Key(), it.Value()))
		}
		if builder.Empty() {
			builder.Close()
			continue
		}
		fmt.Printf("LOG Compact. Iteration to generate one table took: %v\n", time.Since(timeStart))

		wg.Add(1)
		y.AssertTruef(newID <= newIDMax, "%d %d", newID, newIDMax)
		go func(idx int, fileID uint64, builder *table.TableBuilder) {
			defer builder.Close()
			defer wg.Done()
			fd, err := y.OpenSyncedFile(table.NewFilename(fileID, s.kv.opt.Dir), true)
			y.Check(err)
			// Encode the level number as table metadata.
			var levelNum [2]byte
			binary.BigEndian.PutUint16(levelNum[:], uint16(l+1))

			fd.Write(builder.Finish(levelNum[:]))
			newTables[idx], err = table.OpenTable(fd, s.kv.opt.MapTablesTo)
			// decrRef is added below.
			y.Check(err)
		}(i, newID, builder)
		newID++
	}
	wg.Wait()

	out := newTables[:i]
	return out, func() {
		for _, t := range out {
			t.DecrRef() // replaceTables will increment reference.
		}
	}
}

type compactDef struct {
	top []*table.Table
	bot []*table.Table
}

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < s.kv.opt.MaxLevels) // Sanity check.
	thisLevel := s.levels[l]
	nextLevel := s.levels[l+1]

	//	srcIdx0, srcIdx1 := thisLevel.pickCompactTables()
	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	var cds []compactDef
	if l == 0 {
		thisLevel.RLock() // For accessing tables. We may be adding new table into level 0.
		top := make([]*table.Table, len(thisLevel.tables))
		y.AssertTrue(len(thisLevel.tables) == copy(top, thisLevel.tables))
		thisLevel.RUnlock()

		smallest, biggest := keyRange(top)
		//		nextLevel.RLock() // Shouldn't be necessary.
		left, right := overlappingTables(smallest, biggest, nextLevel.tables)
		//		nextLevel.RUnlock()
		bot := make([]*table.Table, right-left)
		copy(bot, nextLevel.tables[left:right])
		cds = append(cds, compactDef{
			top: top,
			bot: bot,
		})
	} else {
		// Sort tables by size, descending.
		// TODO: This does not seem to speed up much. Consider ordering by
		// tables that require the least amount of work, i.e., min overlap with
		// the next level.
		tbls := make([]*table.Table, len(thisLevel.tables))
		copy(tbls, thisLevel.tables)
		sort.Slice(tbls, func(i, j int) bool {
			return tbls[i].Size() > tbls[j].Size()
		})
		type intPair struct {
			left, right int
		}
		var ranges []intPair
		for _, t := range tbls {
			left, right := overlappingTables(t.Smallest(), t.Biggest(), nextLevel.tables)
			var hasOverlap bool
			for _, r := range ranges {
				if !(left >= r.right || right <= r.left) {
					hasOverlap = true
					break
				}
			}
			if !hasOverlap {
				bot := make([]*table.Table, right-left)
				copy(bot, nextLevel.tables[left:right])
				cds = append(cds, compactDef{
					top: []*table.Table{t},
					bot: bot,
				})
				ranges = append(ranges, intPair{left, right})
				// TODO: Number of tables to be compacted together. We have tried different values.
				if len(cds) >= 2 {
					break
				}
			}
		}
	}

	var wg sync.WaitGroup
	for _, cd := range cds {
		wg.Add(1)
		go func(cd compactDef) {
			defer wg.Done()
			timeStart := time.Now()
			var readSize int64
			for _, tbl := range cd.top {
				readSize += tbl.Size()
			}
			for _, tbl := range cd.bot {
				readSize += tbl.Size()
			}

			if thisLevel.level >= 1 && len(cd.bot) == 0 {
				y.AssertTrue(len(cd.top) == 1)
				tbl := cd.top[0]
				nextLevel.replaceTables(cd.top)
				thisLevel.deleteTables(cd.top)
				updateLevel(tbl, l+1)
				if s.kv.opt.Verbose {
					fmt.Printf("LOG Compact-Move %d->%d smallest:%s biggest:%s took %v\n",
						l, l+1, string(tbl.Smallest()), string(tbl.Biggest()), time.Since(timeStart))
				}
				return
			}

			c := s.buildCompaction(&cd)
			//			if s.kv.opt.Verbose {
			//				y.Printf("Compact start: %v\n", c)
			//			}
			s.clog.add(c)
			newTables, decr := s.compactBuildTables(l, cd.top, cd.bot, c)
			defer decr()

			nextLevel.replaceTables(newTables)
			thisLevel.deleteTables(cd.top) // Function will acquire level lock.
			// Note: For level 0, while doCompact is running, it is possible that new tables are added.
			// However, the tables are added only to the end, so it is ok to just delete the first table.

			// Write to compact log.
			c.done = 1
			s.clog.add(c)

			if s.kv.opt.Verbose {
				fmt.Printf("LOG Compact %d->%d, del %d tables, add %d tables, took %v\n",
					l, l+1, len(cd.top)+len(cd.bot), len(newTables), time.Since(timeStart))
			}
		}(cd)
	}
	wg.Wait()
	//	s.validate()
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) {
	for !s.levels[0].tryAddLevel0Table(t, s.kv.opt.Verbose) {
		// Stall. Make sure all levels are healthy before we unstall.
		var timeStart time.Time
		if s.kv.opt.Verbose {
			fmt.Printf("STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: %v\n",
				time.Since(lastUnstalled))
			s.debugPrint()
			timeStart = time.Now()
		}
		// Before we unstall, we need to make sure that level 0 and 1 are healthy. Otherwise, we
		// will very quickly fill up level 0 again and if the compaction strategy favors level 0,
		// then level 1 is going to super full.
		for {
			if s.levels[0].getTotalSize() == 0 && s.levels[1].getTotalSize() < s.levels[1].maxTotalSize {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if s.kv.opt.Verbose {
			fmt.Printf("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: %v\n",
				time.Since(timeStart))
			s.debugPrint()
			lastUnstalled = time.Now()
		}
	}
}

// tryAddLevel0Table returns true if ok and no stalling.
func (s *levelHandler) tryAddLevel0Table(t *table.Table, verbose bool) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	if len(s.tables) > s.kv.opt.NumLevelZeroTablesStall {
		return false
	}

	s.tables = append(s.tables, t)
	t.IncrRef()
	s.totalSize += t.Size()

	if verbose {
		fmt.Printf("Num level 0 tables increased from %d to %d\n", len(s.tables)-1, len(s.tables))
	}
	return true
}

func (s *levelHandler) numTables() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.tables)
}

func (s *levelsController) close() {
	if s.kv.opt.Verbose {
		y.Printf("Sending close signal to compact workers\n")
	}
	n := s.kv.opt.MaxLevels / 2
	for i := 0; i < n; i++ {
		s.compactWorkersDone <- struct{}{}
	}
	// Wait for all compactions to be done. We want to be in a stable state.
	// Also, closing tables while merge iterators have references will also lead to crash.
	s.compactWorkersWg.Wait()
	if s.kv.opt.Verbose {
		y.Printf("Compaction is all done\n")
	}
	for _, l := range s.levels {
		l.close()
	}
	s.clog.close()
}

func (s *levelHandler) close() {
	s.RLock()
	defer s.RUnlock()
	for _, t := range s.tables {
		t.Close()
	}
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(ctx context.Context, key []byte) y.ValueStruct {
	// No need to lock anything as we just iterate over the currently immutable levelHandlers.
	for _, h := range s.levels {
		vs := h.get(ctx, key)
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		y.Trace(ctx, "Found key")
		return vs
	}
	return y.ValueStruct{}
}

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getTableForKey(key []byte) []*table.Table {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		// CAUTION: Reverse the tables.
		out := make([]*table.Table, 0, len(s.tables))
		for i := len(s.tables) - 1; i >= 0; i-- {
			out = append(out, s.tables[i])
		}
		return out
	}
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].Biggest(), key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil
	}
	return []*table.Table{s.tables[idx]}
}

// get returns value for a given key. If not found, return nil.
func (s *levelHandler) get(ctx context.Context, key []byte) y.ValueStruct {
	y.Trace(ctx, "get key at level %d", s.level)
	tables := s.getTableForKey(key)
	for _, th := range tables {
		if th.DoesNotHave(key) {
			continue
		}
		it := th.NewIterator(false)
		defer it.Close()
		it.Seek(key)
		if !it.Valid() {
			continue
		}
		if bytes.Equal(key, it.Key()) {
			return it.Value()
		}

	}
	return y.ValueStruct{}
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(reversed))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelHandler) appendIterators(iters []y.Iterator, reversed bool) []y.Iterator {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		return appendIteratorsReversed(iters, s.tables, reversed)
	}
	return append(iters, table.NewConcatIterator(s.tables, reversed))
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, reversed bool) []y.Iterator {
	for _, level := range s.levels {
		iters = level.appendIterators(iters, reversed)
	}
	return iters
}
