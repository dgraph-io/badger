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
	"sort"
	"sync"
	"unsafe"

	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
)

type levelHandler struct {
	// Guards tables, totalSize.
	sync.RWMutex

	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
	tables         []*table.Table
	totalSize      int64
	totalStaleSize int64
	meta           *z.Buffer // maybe use z.Buffer, keep smallest and pointer to table (64 bits), suing sliceiterate
	// update above when tables are added or removed, will have to rewrite

	// The following are initialized once and const.
	level    int
	strLevel string
	db       *DB
}

func (s *levelHandler) isLastLevel() bool {
	return s.level == s.db.opt.MaxLevels-1
}

func (s *levelHandler) getTotalStaleSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalStaleSize
}

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
	s.totalStaleSize = 0
	for _, t := range tables {
		s.addSize(t)
	}

	if s.level == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(s.tables, func(i, j int) bool {
			return s.tables[i].ID() < s.tables[j].ID()
		})
	} else {
		// Sort tables by keys.
		sort.Slice(s.tables, func(i, j int) bool {
			return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
		})
	}

	for _, t := range s.tables {
		s.appendTable(t)
	}
}

// deleteTables remove tables idx0, ..., idx1-1.
func (s *levelHandler) deleteTables(toDel []*table.Table) error {
	s.Lock() // s.Unlock() below

	s.meta.Reset()
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
			s.appendTable(t)
			continue
		}
		s.subtractSize(t)
	}
	s.tables = newTables

	s.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (s *levelHandler) replaceTables(toDel, toAdd []*table.Table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	s.Lock() // We s.Unlock() below.

	s.meta.Reset()
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}
	var newTables []*table.Table
	for _, t := range s.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.subtractSize(t)
	}

	// Increase totalSize first.
	for _, t := range toAdd {
		s.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	// Assign tables.
	s.tables = newTables
	sort.Slice(s.tables, func(i, j int) bool {
		return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
	})
	for _, t := range s.tables {
		s.appendTable(t)
	}
	s.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel)
}

// addTable adds toAdd table to levelHandler. Normally when we add tables to levelHandler, we sort
// tables based on table.Smallest. This is required for correctness of the system. But in case of
// stream writer this can be avoided. We can just add tables to levelHandler's table list
// and after all addTable calls, we can sort table list(check sortTable method).
// NOTE: levelHandler.sortTables() should be called after call addTable calls are done.
func (s *levelHandler) addTable(t *table.Table) {
	s.Lock()
	defer s.Unlock()

	s.addSize(t) // Increase totalSize first.
	t.IncrRef()
	s.tables = append(s.tables, t)
	s.appendTable(t)
}

// sortTables sorts tables of levelHandler based on table.Smallest.
// Normally it should be called after all addTable calls.
func (s *levelHandler) sortTables() {
	s.RLock()
	defer s.RUnlock()

	sort.Slice(s.tables, func(i, j int) bool {
		return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
	})
	// Given T1, T2 lie on same level,
	// if smallest of T1 < smallest of T2, then biggest of T1 < biggest of T2.
	s.meta.SortSlice(func(ls, rs []byte) bool {
		lhs := tableEntry(ls)
		rhs := tableEntry(rs)
		return less(lhs, rhs)
	})
}

func decrRefs(tables []*table.Table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelHandler(db *DB, level int) *levelHandler {
	return &levelHandler{
		level:    level,
		strLevel: fmt.Sprintf("l%d", level),
		db:       db,
		meta:     z.NewBuffer(1<<20, "Level Handler"),
	}
}

type tableEntry []byte

// type tableEntry struct {
// 	ptr   uint64  // pointer to table
// 	small []byte  // smallest/biggest key of the table
//  big   []byte
// }

func tableEntrySize(small, big []byte) int {
	return 8 + 4 + 4 + len(small) + len(big) // ptr + smallSz + bigSz + len(small) + len(big)
}

func marshalTableEntry(dst []byte, ptr *table.Table, small, big []byte) {
	binary.BigEndian.PutUint64(dst[0:8], uint64(uintptr(unsafe.Pointer(ptr))))
	binary.BigEndian.PutUint32(dst[8:12], uint32(len(small)))
	binary.BigEndian.PutUint32(dst[12:16], uint32(len(big)))

	n := copy(dst[16:], small)
	m := copy(dst[16+n:], big)
	y.AssertTrue(len(dst) == 16+n+m)
}

func (me tableEntry) Size() int {
	return len(me)
}

func (me tableEntry) Ptr() uint64 {
	return binary.BigEndian.Uint64(me[0:8])
}

func (me tableEntry) Smallest() []byte {
	sz := binary.BigEndian.Uint32(me[8:12])
	return me[16 : 16+sz]
}

func (me tableEntry) Biggest() []byte {
	sz1 := binary.BigEndian.Uint32(me[8:12])
	sz2 := binary.BigEndian.Uint32(me[12:16])
	return me[16+sz1 : 16+sz1+sz2]
}

func less(lhs, rhs tableEntry) bool {
	return y.CompareKeys(lhs.Smallest(), rhs.Smallest()) < 0
}

// tryAddLevel0Table returns true if ok and no stalling.
func (s *levelHandler) tryAddLevel0Table(t *table.Table) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	// Stall (by returning false) if we are above the specified stall setting for L0.
	if len(s.tables) >= s.db.opt.NumLevelZeroTablesStall {
		return false
	}
	s.tables = append(s.tables, t)
	s.appendTable(t)
	t.IncrRef()
	s.addSize(t)
	return true
}

// Note: Caller must take the lock.
func (s *levelHandler) appendTable(t *table.Table) {
	dst := s.meta.SliceAllocate(tableEntrySize(t.Smallest(), t.Biggest()))
	marshalTableEntry(dst, t, t.Smallest(), t.Biggest())
}

// This should be called while holding the lock on the level.
func (s *levelHandler) addSize(t *table.Table) {
	s.totalSize += t.Size()
	s.totalStaleSize += int64(t.StaleDataSize())
}

// This should be called while holding the lock on the level.
func (s *levelHandler) subtractSize(t *table.Table) {
	s.totalSize -= t.Size()
	s.totalStaleSize -= int64(t.StaleDataSize())
}
func (s *levelHandler) numTables() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.tables)
}

func (s *levelHandler) close() error {
	s.RLock()
	defer s.RUnlock()
	var err error
	for _, t := range s.tables {
		if closeErr := t.Close(-1); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	s.meta.Release()
	return y.Wrap(err, "levelHandler.close")
}

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
func (s *levelHandler) getTableForKey(key []byte) ([]*table.Table, func() error) {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		// CAUTION: Reverse the tables.
		out := make([]*table.Table, 0, len(s.tables))
		for i := len(s.tables) - 1; i >= 0; i-- {
			out = append(out, s.tables[i])
			s.tables[i].IncrRef()
		}
		return out, func() error {
			for _, t := range out {
				if err := t.DecrRef(); err != nil {
					return err
				}
			}
			return nil
		}
	}
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(s.tables[i].Biggest(), key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil, func() error { return nil }
	}
	tbl := s.tables[idx]
	tbl.IncrRef()
	return []*table.Table{tbl}, tbl.DecrRef
}

// get returns value for a given key or the key after that. If not found, return nil.
func (s *levelHandler) get(key []byte) (y.ValueStruct, error) {
	tables, decr := s.getTableForKey(key)
	keyNoTs := y.ParseKey(key)

	hash := y.Hash(keyNoTs)
	var maxVs y.ValueStruct
	for _, th := range tables {
		if th.DoesNotHave(hash) {
			y.NumLSMBloomHitsAdd(s.db.opt.MetricsEnabled, s.strLevel, 1)
			continue
		}

		it := th.NewIterator(0)
		defer it.Close()

		y.NumLSMGetsAdd(s.db.opt.MetricsEnabled, s.strLevel, 1)
		it.Seek(key)
		if !it.Valid() {
			continue
		}
		if y.SameKey(key, it.Key()) {
			if version := y.ParseTs(it.Key()); maxVs.Version < version {
				maxVs = it.ValueCopy()
				maxVs.Version = version
			}
		}
	}
	return maxVs, decr()
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelHandler) appendIterators(iters []y.Iterator, opt *IteratorOptions) []y.Iterator {
	s.RLock()
	defer s.RUnlock()

	var topt int
	if opt.Reverse {
		topt = table.REVERSED
	}
	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		// Level 0 tables are not in key sorted order, so we need to consider them one by one.
		var out []*table.Table
		for _, t := range s.tables {
			if opt.pickTable(t) {
				out = append(out, t)
			}
		}
		return appendIteratorsReversed(iters, out, topt)
	}

	tables := opt.pickTables(s)
	if len(tables) == 0 {
		return iters
	}
	return append(iters, table.NewConcatIterator(tables, topt))
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (s *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(kr.left, s.tables[i].Biggest()) <= 0
	})
	right := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(kr.right, s.tables[i].Smallest()) < 0
	})
	return left, right
}
