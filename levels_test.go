/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"io/ioutil"
	"testing"

	"github.com/dgraph-io/badger/v2/options"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/table"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

// createAndOpen creates a table with the given data and adds it to the given level.
func createAndOpen(db *DB, td []keyValVersion, level int) {
	opts := table.Options{
		BlockSize:          db.opt.BlockSize,
		BloomFalsePositive: db.opt.BloomFalsePositive,
		LoadingMode:        options.LoadToRAM,
		ChkMode:            options.NoVerification,
	}
	b := table.NewTableBuilder(opts)

	// Add all keys and versions to the table.
	for _, item := range td {
		key := y.KeyWithTs([]byte(item.key), uint64(item.version))
		val := y.ValueStruct{Value: []byte(item.val)}
		b.Add(key, val, 0)
	}
	fd, err := y.CreateSyncedFile(table.NewFilename(db.lc.reserveFileID(), db.opt.Dir), true)
	if err != nil {
		panic(err)
	}

	if _, err = fd.Write(b.Finish()); err != nil {
		panic(err)
	}
	tab, err := table.OpenTable(fd, opts)
	if err != nil {
		panic(err)
	}
	if err := db.manifest.addChanges([]*pb.ManifestChange{
		newCreateChange(tab.ID(), level, 0, tab.CompressionType()),
	}); err != nil {
		panic(err)
	}
	// Add table to the given level.
	db.lc.levels[level].tables = append(db.lc.levels[level].tables, tab)
}

type keyValVersion struct {
	key     string
	val     string
	version int
}

func TestCheckOverlap(t *testing.T) {
	t.Run("overlap", func(t *testing.T) {
		// This test consists of one table on level 0 and one on level 1.
		// There is an overlap amongst the tables but there is no overlap
		// with rest of the levels.
		t.Run("same keys", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				l0 := []keyValVersion{{"foo", "bar", 3}}
				l1 := []keyValVersion{{"foo", "bar", 2}}
				createAndOpen(db, l0, 0)
				createAndOpen(db, l1, 1)

				// Level 0 should overlap with level 0 tables.
				require.True(t, db.lc.checkOverlap(db.lc.levels[0].tables, 0))
				// Level 1 should overlap with level 0 tables (they have the same keys).
				require.True(t, db.lc.checkOverlap(db.lc.levels[0].tables, 1))
				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 2))
				require.False(t, db.lc.checkOverlap(db.lc.levels[1].tables, 2))
				require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 3))
				require.False(t, db.lc.checkOverlap(db.lc.levels[1].tables, 3))

			})
		})
		t.Run("overlapping keys", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				l0 := []keyValVersion{{"a", "x", 1}, {"b", "x", 1}, {"foo", "bar", 3}}
				l1 := []keyValVersion{{"foo", "bar", 2}}
				createAndOpen(db, l0, 0)
				createAndOpen(db, l1, 1)

				// Level 0 should overlap with level 0 tables.
				require.True(t, db.lc.checkOverlap(db.lc.levels[0].tables, 0))
				require.True(t, db.lc.checkOverlap(db.lc.levels[1].tables, 1))
				// Level 1 should overlap with level 0 tables, "foo" key is common.
				require.True(t, db.lc.checkOverlap(db.lc.levels[0].tables, 1))
				// Level 2 and 3 should not overlap with level 0 tables.
				require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 2))
				require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 3))
			})
		})
	})
	t.Run("non-overlapping", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{{"a", "x", 1}, {"b", "x", 1}, {"c", "bar", 3}}
			l1 := []keyValVersion{{"foo", "bar", 2}}
			createAndOpen(db, l0, 0)
			createAndOpen(db, l1, 1)

			// Level 1 should not overlap with level 0 tables
			require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 1))
			// Level 2 and 3 should not overlap with level 0 tables.
			require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 2))
			require.False(t, db.lc.checkOverlap(db.lc.levels[0].tables, 3))
		})
	})
}

func getAllAndCheck(t *testing.T, db *DB, expected []keyValVersion) {
	db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.AllVersions = true
		opt.InternalAccess = true
		it := txn.NewIterator(opt)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			require.Less(t, i, len(expected), "DB has more number of key than expected")
			item := it.Item()
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			// fmt.Printf("k: %s v: %d val: %s\n", item.key, item.Version(), v)
			expect := expected[i]
			require.Equal(t, expect.key, string(item.Key()), "expected key: %s actual key: %s",
				expect.key, item.Key())
			require.Equal(t, expect.val, string(v), "key: %s expected value: %s actual %s",
				item.key, expect.val, v)
			require.Equal(t, expect.version, int(item.Version()), "key: %s expected version: %d ",
				"actual %d", item.key, expect.version, item.Version())
			i++
		}
		require.Equal(t, len(expected), i, "keys examined should be equal to keys expected")
		return nil
	})

}

func TestCompaction(t *testing.T) {
	t.Run("level 0 to level 1", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l0 := []keyValVersion{{"foo", "bar", 3}, {"fooz", "baz", 1}}
		l01 := []keyValVersion{{"foo", "bar", 2}}
		l1 := []keyValVersion{{"foo", "bar", 1}}
		// Level 0 has table l0 and l01.
		createAndOpen(db, l0, 0)
		createAndOpen(db, l01, 0)
		// Level 1 has table l1.
		createAndOpen(db, l1, 1)

		// Set a high discard timestamp so that all the keys are below the discard timestamp.
		db.SetDiscardTs(10)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 3}, {"foo", "bar", 2}, {"foo", "bar", 1}, {"fooz", "baz", 1},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// foo version 2 should be dropped after compaction.
		getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 3}, {"fooz", "baz", 1}})
	})

	t.Run("level 0 to level 1 with lower overlap", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l0 := []keyValVersion{{"foo", "bar", 3}, {"fooz", "baz", 1}}
		l01 := []keyValVersion{{"foo", "bar", 2}}
		l1 := []keyValVersion{{"foo", "bar", 1}}
		l2 := []keyValVersion{{"foo", "bar", 0}}
		// Level 0 has table l0 and l01.
		createAndOpen(db, l0, 0)
		createAndOpen(db, l01, 0)
		// Level 1 has table l1.
		createAndOpen(db, l1, 1)
		// Level 2 has table l2.
		createAndOpen(db, l2, 2)

		// Set a high discard timestamp so that all the keys are below the discard timestamp.
		db.SetDiscardTs(10)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 3}, {"foo", "bar", 2}, {"foo", "bar", 1},
			{"foo", "bar", 0}, {"fooz", "baz", 1},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// foo version 2 and version 1 should be dropped after compaction.
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 3}, {"foo", "bar", 0}, {"fooz", "baz", 1},
		})
	})

	t.Run("level 1 to level 2", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l1 := []keyValVersion{{"foo", "bar", 3}, {"fooz", "baz", 1}}
		l2 := []keyValVersion{{"foo", "bar", 2}}
		createAndOpen(db, l1, 1)
		createAndOpen(db, l2, 2)

		// Set a high discard timestamp so that all the keys are below the discard timestamp.
		db.SetDiscardTs(10)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 3}, {"foo", "bar", 2}, {"fooz", "baz", 1},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[1],
			nextLevel: db.lc.levels[2],
			top:       db.lc.levels[1].tables,
			bot:       db.lc.levels[2].tables,
		}
		require.NoError(t, db.lc.runCompactDef(1, cdef))
		// foo version 2 should be dropped after compaction.
		getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 3}, {"fooz", "baz", 1}})
	})
}

func TestHeadKeyCleanup(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Disable compactions and keep single version of each key.
	opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
	db, err := OpenManaged(opt)
	require.NoError(t, err)

	l0 := []keyValVersion{
		{string(head), "foo", 5}, {string(head), "bar", 4}, {string(head), "baz", 3},
	}
	l1 := []keyValVersion{{string(head), "fooz", 2}, {string(head), "foozbaz", 1}}
	// Level 0 has table l0 and l01.
	createAndOpen(db, l0, 0)
	// Level 1 has table l1.
	createAndOpen(db, l1, 1)

	// Set a high discard timestamp so that all the keys are below the discard timestamp.
	db.SetDiscardTs(10)

	getAllAndCheck(t, db, []keyValVersion{
		{string(head), "foo", 5}, {string(head), "bar", 4}, {string(head), "baz", 3},
		{string(head), "fooz", 2}, {string(head), "foozbaz", 1},
	})
	cdef := compactDef{
		thisLevel: db.lc.levels[0],
		nextLevel: db.lc.levels[1],
		top:       db.lc.levels[0].tables,
		bot:       db.lc.levels[1].tables,
	}
	require.NoError(t, db.lc.runCompactDef(0, cdef))
	// foo version 2 should be dropped after compaction.
	getAllAndCheck(t, db, []keyValVersion{{string(head), "foo", 5}})
}

func TestDiscardTs(t *testing.T) {
	t.Run("all keys above discardTs", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l0 := []keyValVersion{{"foo", "bar", 4}, {"fooz", "baz", 3}}
		l01 := []keyValVersion{{"foo", "bar", 3}}
		l1 := []keyValVersion{{"foo", "bar", 2}}
		// Level 0 has table l0 and l01.
		createAndOpen(db, l0, 0)
		createAndOpen(db, l01, 0)
		// Level 1 has table l1.
		createAndOpen(db, l1, 1)

		// Set dicardTs to 1. All the keys are above discardTs.
		db.SetDiscardTs(1)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"foo", "bar", 2}, {"fooz", "baz", 3},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// No keys should be dropped.
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"foo", "bar", 2}, {"fooz", "baz", 3},
		})
	})
	t.Run("some keys above discardTs", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l0 := []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"foo", "bar", 2}, {"fooz", "baz", 2},
		}
		l1 := []keyValVersion{{"foo", "bbb", 1}}
		createAndOpen(db, l0, 0)
		createAndOpen(db, l1, 1)

		// Set dicardTs to 3. foo2 and foo1 should be dropped.
		db.SetDiscardTs(3)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"foo", "bar", 2},
			{"foo", "bbb", 1}, {"fooz", "baz", 2},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// foo1 and foo2 should be dropped.
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"fooz", "baz", 2},
		})
	})
	t.Run("all keys below discardTs", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		// Disable compactions and keep single version of each key.
		opt := DefaultOptions(dir).WithNumCompactors(0).WithNumVersionsToKeep(1)
		db, err := OpenManaged(opt)
		require.NoError(t, err)

		l0 := []keyValVersion{{"foo", "bar", 4}, {"fooz", "baz", 3}}
		l01 := []keyValVersion{{"foo", "bar", 3}}
		l1 := []keyValVersion{{"foo", "bar", 2}}
		// Level 0 has table l0 and l01.
		createAndOpen(db, l0, 0)
		createAndOpen(db, l01, 0)
		// Level 1 has table l1.
		createAndOpen(db, l1, 1)

		// Set dicardTs to 10. All the keys are below discardTs.
		db.SetDiscardTs(10)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bar", 4}, {"foo", "bar", 3}, {"foo", "bar", 2}, {"fooz", "baz", 3},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// Only one version of every key should be left.
		getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 4}, {"fooz", "baz", 3}})
	})
}
