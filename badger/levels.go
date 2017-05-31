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
	"github.com/pkg/errors"
)

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

func newLevelsController(kv *KV) (*levelsController, error) {
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

		if err := os.Remove(clogName); err != nil { // Everything is ok. Clear compact log.
			return nil, errors.Wrapf(err, "Removing compaction log: %q", clogName)
		}
	}

	// Some files may be deleted. Let's reload.
	tables := make([][]*table.Table, kv.opt.MaxLevels)
	for fileID := range getIDMap(kv.opt.Dir) {
		fname := table.NewFilename(fileID, kv.opt.Dir)
		fd, err := y.OpenSyncedFile(fname, true)
		if err != nil {
			return nil, errors.Wrapf(err, "Opening file: %q", fname)
		}

		t, err := table.OpenTable(fd, kv.opt.MapTablesTo)
		if err != nil {
			return nil, errors.Wrapf(err, "Opening table: %v", fd)
		}

		// Check metadata for level information.
		tableMeta := t.Metadata()
		y.AssertTruef(len(tableMeta) == 2, "len(tableMeta). Expected=2. Actual=%d", len(tableMeta))

		level := int(binary.BigEndian.Uint16(tableMeta))
		y.AssertTruef(level < kv.opt.MaxLevels, "max(level). Expected=%d. Actual=%d",
			kv.opt.MaxLevels, level)
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

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		return nil, errors.Wrap(err, "Level validation")
	}

	// Create new compact log.
	if err := s.clog.init(clogName); err != nil {
		return nil, errors.Wrap(err, "Compaction Log")
	}
	return s, nil
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
// TODO: Implement this here
// https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
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
	l int, cd compactDef, c *compaction) ([]*table.Table, func() error) {

	topTables := cd.top
	botTables := cd.bot

	// Create iterators across all the tables involved first.
	var iters []y.Iterator
	if l == 0 {
		iters = appendIteratorsReversed(iters, topTables, false)
	} else {
		y.AssertTrue(len(topTables) == 1)
		iters = []y.Iterator{topTables[0].NewIterator(false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	iters = append(iters, table.NewConcatIterator(botTables, false))
	it := y.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	// Start generating new tables.
	newTables := make([]*table.Table, len(c.toInsert))
	che := make(chan error, len(c.toInsert))
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
		y.Printf("LOG Compact. Iteration to generate one table took: %v\n", time.Since(timeStart))

		y.AssertTruef(newID <= newIDMax, "%d %d", newID, newIDMax)
		go func(idx int, fileID uint64, builder *table.TableBuilder) {
			defer builder.Close()

			fd, err := y.OpenSyncedFile(table.NewFilename(fileID, s.kv.opt.Dir), true)
			if err != nil {
				che <- errors.Wrapf(err, "While opening new table: %d", fileID)
				return
			}

			// Encode the level number as table metadata.
			var levelNum [2]byte
			binary.BigEndian.PutUint16(levelNum[:], uint16(l+1))
			_, err = fd.Write(builder.Finish(levelNum[:]))
			if err != nil {
				che <- errors.Wrapf(err, "Unable to write to file: %d", fileID)
				return
			}

			newTables[idx], err = table.OpenTable(fd, s.kv.opt.MapTablesTo)
			// decrRef is added below.
			che <- errors.Wrapf(err, "Unable to open table: %q", fd.Name())

		}(i, newID, builder)
		newID++
	}

	// Wait for all table builders to finish.
	for x := 0; x < i; x++ {
		if err := <-che; err != nil {
			return nil, func() error { return errors.Wrapf(err, "While running compaction for: %+v", c) }
		}
	}

	out := newTables[:i]
	return out, func() error {
		for _, t := range out {
			// replaceTables will increment reference.
			if err := t.DecrRef(); err != nil {
				return err
			}
		}
		return nil
	}
}

type compactDef struct {
	thisLevel *levelHandler
	nextLevel *levelHandler

	top []*table.Table
	bot []*table.Table
}

func (cd *compactDef) RLock() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) RUnlock() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

var errNoTableFound = errors.New("No table found")

func (cd *compactDef) fillTablesL0() error {
	cd.thisLevel.AssertRLock()
	cd.nextLevel.AssertRLock()

	cd.top = make([]*table.Table, len(cd.thisLevel.tables))
	copy(cd.top, cd.thisLevel.tables)
	if len(cd.top) == 0 {
		return errNoTableFound
	}

	smallest, biggest := keyRange(cd.top)

	left, right := cd.nextLevel.overlappingTables(smallest, biggest)
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])
	return nil
}

func (cd *compactDef) fillTables() error {
	cd.thisLevel.AssertRLock()
	cd.nextLevel.AssertRLock()

	tbls := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tbls, cd.thisLevel.tables)
	if len(tbls) == 0 {
		return errNoTableFound
	}

	// Find the biggest table, and compact that first.
	// TODO: Try other table picking strategies.
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].Size() > tbls[j].Size()
	})

	t := tbls[0]
	cd.top = []*table.Table{t}

	left, right := cd.nextLevel.overlappingTables(t.Smallest(), t.Biggest())
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])
	return nil
}

func (s *levelsController) runCompactDef(l int, cd compactDef) {
	timeStart := time.Now()
	var readSize int64
	for _, tbl := range cd.top {
		readSize += tbl.Size()
	}
	for _, tbl := range cd.bot {
		readSize += tbl.Size()
	}

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	if thisLevel.level >= 1 && len(cd.bot) == 0 {
		y.AssertTrue(len(cd.top) == 1)

		nextLevel.replaceTables(cd.top)
		thisLevel.deleteTables(cd.top)

		tbl := cd.top[0]
		tbl.UpdateLevel(l + 1)
		y.Printf("LOG Compact-Move %d->%d smallest:%s biggest:%s took %v\n",
			l, l+1, string(tbl.Smallest()), string(tbl.Biggest()), time.Since(timeStart))
		return
	}

	c := s.buildCompactionLogEntry(&cd)
	//			if s.kv.opt.Verbose {
	//				y.Printf("Compact start: %v\n", c)
	//			}
	s.clog.add(c)
	newTables, decr := s.compactBuildTables(l, cd, c)
	if newTables == nil {
		err := decr()
		// This compaction couldn't be done successfully.
		y.Printf("LOG Compact FAILED with error: %+v: %+v %+v", err, cd, c)
		return
	}
	defer decr()

	nextLevel.replaceTables(newTables)
	thisLevel.deleteTables(cd.top) // Function will acquire level lock.

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	// Write to compact log.
	c.done = 1
	s.clog.add(c)

	y.Printf("LOG Compact %d->%d, del %d tables, add %d tables, took %v\n",
		l, l+1, len(cd.top)+len(cd.bot), len(newTables), time.Since(timeStart))
}

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(l int) error {
	y.AssertTrue(l+1 < s.kv.opt.MaxLevels) // Sanity check.

	cd := compactDef{
		thisLevel: s.levels[l],
		nextLevel: s.levels[l+1],
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	cd.RLock()
	if l == 0 {
		if err := cd.fillTablesL0(); err != nil {
			cd.RUnlock()
			return err
		}
	} else {
		if err := cd.fillTables(); err != nil {
			cd.RUnlock()
			return err
		}
	}
	cd.RUnlock()

	s.runCompactDef(l, cd)
	//	s.validate()
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) {
	for !s.levels[0].tryAddLevel0Table(t) {
		// Stall. Make sure all levels are healthy before we unstall.
		var timeStart time.Time
		if s.kv.opt.Verbose {
			y.Printf("STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: %v\n",
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
			y.Printf("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: %v\n",
				time.Since(timeStart))
			s.debugPrint()
			lastUnstalled = time.Now()
		}
	}
}

func (s *levelsController) close() error {
	y.Printf("Sending close signal to compact workers\n")
	n := s.kv.opt.MaxLevels / 2
	for i := 0; i < n; i++ {
		s.compactWorkersDone <- struct{}{}
	}
	// Wait for all compactions to be done. We want to be in a stable state.
	// Also, closing tables while merge iterators have references will also lead to crash.
	s.compactWorkersWg.Wait()
	y.Printf("Compaction is all done\n")
	for _, l := range s.levels {
		if err := l.close(); err != nil {
			return errors.Wrap(err, "levelsController.Close")
		}
	}
	return s.clog.close()
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key []byte) (y.ValueStruct, error) {
	// No need to lock anything as we just iterate over the currently immutable levelHandlers.
	for _, h := range s.levels {
		vs, err := h.get(key)
		if err != nil {
			return y.ValueStruct{}, errors.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		return vs, nil
	}
	return y.ValueStruct{}, nil
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
func (s *levelsController) appendIterators(
	iters []y.Iterator, reversed bool) []y.Iterator {
	for _, level := range s.levels {
		iters = level.appendIterators(iters, reversed)
	}
	return iters
}
