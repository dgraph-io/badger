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
	compactDone chan struct{}
	compactJobs sync.WaitGroup
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
func (s *levelHandler) deleteTables(idx0, idx1 int) {
	s.Lock()
	defer s.Unlock()
	for i := idx0; i < idx1; i++ {
		s.totalSize -= s.tables[i].Size()
		s.tables[i].DecrRef()
	}
	s.tables = append(s.tables[:idx0], s.tables[idx1:]...)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
func (s *levelHandler) replaceTables(left, right int, newTables []*table.Table) {
	s.Lock()
	defer s.Unlock()

	// Update totalSize first.
	for _, tbl := range newTables {
		s.totalSize += tbl.Size()
		tbl.IncrRef()
	}
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

// getFirstTable returns the first table. If level is empty, we return nil.
func (s *levelHandler) getFirstTable() *table.Table {
	s.RLock()
	defer s.RUnlock()
	if len(s.tables) == 0 {
		return nil
	}
	return s.tables[0]
}

func (s *levelHandler) getTable(idx int) *table.Table {
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
		return bytes.Compare(s.tables[i].Biggest(), begin) >= 0
	})
	right := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].Smallest(), end) > 0
	})
	return left, right
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
		fd, err := y.OpenSyncedFile(table.NewFilename(fileID, kv.opt.Dir))
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
	s.compactDone = make(chan struct{}, n)
	s.compactJobs.Add(n)
	for i := 0; i < n; i++ {
		go s.runWorker(i)
	}
}

func (s *levelsController) runWorker(workerID int) {
	defer s.compactJobs.Done() // Indicate worker is done.
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
		case <-s.compactDone:
			done = true
		}
	}
}

// debugPrint shows the general state.
func (s *levelsController) debugPrint() {
	s.Lock()
	defer s.Unlock()
	for i := 0; i < s.kv.opt.MaxLevels; i++ {
		var busy int
		if s.beingCompacted[i] {
			busy = 1
		}
		fmt.Printf("(i=%d, size=%d, busy=%d, numTables=%d) ", i, s.levels[i].getTotalSize(), busy, len(s.levels[i].tables))
	}
	fmt.Printf("\n")
}

// debugPrintMore shows key ranges of each level.
func (s *levelsController) debugPrintMore() {
	s.Lock()
	defer s.Unlock()
	for i := 0; i < s.kv.opt.MaxLevels; i++ {
		s.levels[i].debugPrintMore()
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
		if (i == 0 && len(s.levels[0].tables) > s.kv.opt.NumLevelZeroTables) ||
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
	smallest := s.tables[idx0].Smallest()
	biggest := s.tables[idx0].Biggest()
	for i := idx0 + 1; i < idx1; i++ {
		if bytes.Compare(s.tables[i].Smallest(), smallest) < 0 {
			smallest = s.tables[i].Smallest()
		}
		if bytes.Compare(s.tables[i].Biggest(), biggest) > 0 {
			biggest = s.tables[i].Biggest()
		}
	}
	return smallest, biggest
}

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < s.kv.opt.MaxLevels) // Sanity check.
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
		y.AssertTruef(srcIdx0+1 == srcIdx1, "Expect to move only one table")
		tbl := thisLevel.tables[srcIdx0]
		nextLevel.replaceTables(left, right, thisLevel.tables[srcIdx0:srcIdx1])
		thisLevel.deleteTables(srcIdx0, srcIdx1) // Function will acquire level lock.
		updateLevel(tbl, l+1)
		if s.kv.opt.Verbose {
			fmt.Printf("LOG Compact-Move %d->%d smallest:%s biggest:%s took %v\n",
				l, l+1, string(tbl.Smallest()), string(tbl.Biggest()), time.Since(timeStart))
		}
		return nil
	}

	var c compaction
	var newIDMin, newIDMax uint64
	{
		// Prepare compaction object.
		c.compactID = s.reserveCompactID()
		var estSize int64
		for _, t := range thisLevel.tables[srcIdx0:srcIdx1] {
			c.toDelete = append(c.toDelete, t.ID())
			estSize += t.Size()
		}
		for _, t := range nextLevel.tables[left:right] {
			c.toDelete = append(c.toDelete, t.ID())
			estSize += t.Size()
		}
		estNumTables := 1 + (estSize+s.kv.opt.MaxTableSize-1)/s.kv.opt.MaxTableSize
		newIDMin, newIDMax = s.reserveFileIDs(int(estNumTables))
		// TODO: Consider storing just two numbers for toInsert.
		for i := newIDMin; i < newIDMax; i++ {
			c.toInsert = append(c.toInsert, uint64(i))
		}
	}
	s.clog.add(&c)
	if s.kv.opt.Verbose {
		y.Printf("Compact start: %v\n", c)
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	var iters []y.Iterator
	if l == 0 {
		iters = appendIteratorsReversed(iters, thisLevel.tables[srcIdx0:srcIdx1])
	} else {
		y.AssertTrue(srcIdx0+1 == srcIdx1) // For now, for level >=1, we expect exactly one table.
		iters = []y.Iterator{thisLevel.tables[srcIdx0].NewIterator()}
	}
	iters = append(iters, newConcatIterator(nextLevel.tables[left:right]))
	it := y.NewMergeIterator(iters)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.SeekToFirst()

	var newTables [200]*table.Table
	var wg sync.WaitGroup
	var i int
	newID := newIDMin
	iterTime := time.Now()
	for ; it.Valid(); i++ {
		y.AssertTruef(i < len(newTables), "Rewriting too many tables: %d %d", i, len(newTables))
		builder := table.NewTableBuilder()
		for ; it.Valid(); it.Next() {
			if int64(builder.FinalSize()) > s.kv.opt.MaxTableSize {
				break
			}
			key := it.Key()
			val, meta := it.Value()
			if err := builder.Add(key, val, meta); err != nil {
				builder.Close()
				return err
			}
		}
		if builder.Empty() {
			builder.Close()
			continue
		}
		fmt.Printf("LOG Compact. Iteration to generate one table took: %v\n", time.Since(iterTime))
		iterTime = time.Now()

		wg.Add(1)
		y.AssertTruef(newID < newIDMax, "%d %d", newID, newIDMax)
		go func(idx int, fileID uint64, builder *table.TableBuilder) {
			defer builder.Close()
			defer wg.Done()
			fd, err := y.OpenSyncedFile(table.NewFilename(fileID, s.kv.opt.Dir))
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

	newTablesSlice := newTables[:i]
	defer func() {
		for _, t := range newTablesSlice {
			t.DecrRef() // replaceTables will increment reference.
		}
	}()

	if s.kv.opt.Verbose {
		fmt.Printf("LOG Compact %d{Del [%d,%d)} => %d{Del [%d,%d), Add [%d,%d)}, took %v\n",
			l, srcIdx0, srcIdx1, l+1, left, right, left, left+len(newTablesSlice), time.Since(timeStart))
	}
	nextLevel.replaceTables(left, right, newTablesSlice)
	thisLevel.deleteTables(srcIdx0, srcIdx1) // Function will acquire level lock.
	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	// Write to compact log.
	c.done = 1
	s.clog.add(&c)
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

// Check does some sanity check on one level of data or in-memory index.
func (s *levelHandler) validate() {
	if s.level == 0 {
		return
	}
	s.RLock()
	defer s.RUnlock()
	numTables := len(s.tables)
	for j := 1; j < numTables; j++ {
		y.AssertTruef(j < len(s.tables), "Level %d, j=%d numTables=%d", s.level, j, numTables)
		y.AssertTruef(bytes.Compare(s.tables[j-1].Biggest(), s.tables[j].Smallest()) < 0,
			"Inter: %s vs %s: level=%d j=%d numTables=%d",
			string(s.tables[j-1].Biggest()), string(s.tables[j].Smallest()), s.level, j, numTables)
		y.AssertTruef(bytes.Compare(s.tables[j].Smallest(), s.tables[j].Biggest()) <= 0,
			"Intra: %s vs %s: level=%d j=%d numTables=%d",
			string(s.tables[j].Smallest()), string(s.tables[j].Biggest()), s.level, j, numTables)
	}
}

func (s *levelHandler) debugPrintMore() {
	s.RLock()
	defer s.RUnlock()
	y.Printf("Level %d:", s.level)
	for _, t := range s.tables {
		y.Printf(" [%s, %s]", t.Smallest(), t.Biggest())
	}
	y.Printf("\n")
}

func (s *levelsController) close() {
	if s.kv.opt.Verbose {
		y.Printf("Sending close signal to compact workers\n")
	}
	n := s.kv.opt.MaxLevels / 2
	for i := 0; i < n; i++ {
		s.compactDone <- struct{}{}
	}
	// Wait for all compactions to be done. We want to be in a stable state.
	// Also, closing tables while merge iterators have references will also lead to crash.
	s.compactJobs.Wait()
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

func (s *levelsController) getSummary() *Summary {
	out := &Summary{
		fileIDs: make(map[uint64]bool),
	}
	for _, l := range s.levels {
		l.getSummary(out)
	}
	return out
}

func (s *levelHandler) getSummary(summary *Summary) {
	s.RLock()
	defer s.RUnlock()
	for _, t := range s.tables {
		summary.fileIDs[t.ID()] = true
	}
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(ctx context.Context, key []byte) ([]byte, byte) {
	// No need to lock anything as we just iterate over the currently immutable levelHandlers.
	for _, h := range s.levels {
		v, meta := h.get(ctx, key)
		if v == nil && meta == 0 {
			continue
		}
		y.Trace(ctx, "Found key")
		return v, meta
	}
	return nil, 0
}

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getTableForKey(key []byte) []*table.Table {
	s.RLock()
	defer s.RUnlock()
	if s.level == 0 {
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		out := make([]*table.Table, len(s.tables))
		y.AssertTrue(len(s.tables) == copy(out, s.tables))
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
func (s *levelHandler) get(ctx context.Context, key []byte) ([]byte, byte) {
	y.Trace(ctx, "get key at level %d", s.level)
	tables := s.getTableForKey(key)
	for _, th := range tables {
		it := th.NewIterator()
		defer it.Close()
		it.Seek(key)
		if !it.Valid() {
			continue
		}
		if bytes.Equal(key, it.Key()) {
			return it.Value()
		}

	}
	return nil, 0
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator())
	}
	return out
}

func newConcatIterator(th []*table.Table) y.Iterator {
	iters := make([]y.Iterator, 0, len(th))
	for _, t := range th {
		// This will increment the reference of the table handler.
		iters = append(iters, t.NewIterator())
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
func (s *levelsController) validate() {
	for _, l := range s.levels {
		l.validate()
	}
}
