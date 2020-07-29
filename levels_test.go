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
	"math"
	"testing"
	"time"

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
		val := y.ValueStruct{Value: []byte(item.val), Meta: item.meta}
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
	meta    byte
}

func TestCheckOverlap(t *testing.T) {
	t.Run("overlap", func(t *testing.T) {
		// This test consists of one table on level 0 and one on level 1.
		// There is an overlap amongst the tables but there is no overlap
		// with rest of the levels.
		t.Run("same keys", func(t *testing.T) {
			runBadgerTest(t, nil, func(t *testing.T, db *DB) {
				l0 := []keyValVersion{{"foo", "bar", 3, 0}}
				l1 := []keyValVersion{{"foo", "bar", 2, 0}}
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
				l0 := []keyValVersion{{"a", "x", 1, 0}, {"b", "x", 1, 0}, {"foo", "bar", 3, 0}}
				l1 := []keyValVersion{{"foo", "bar", 2, 0}}
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
			l0 := []keyValVersion{{"a", "x", 1, 0}, {"b", "x", 1, 0}, {"c", "bar", 3, 0}}
			l1 := []keyValVersion{{"foo", "bar", 2, 0}}
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
			item := it.Item()
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			// fmt.Printf("k: %s v: %d val: %s\n", item.key, item.Version(), v)
			require.Less(t, i, len(expected), "DB has more number of key than expected")
			expect := expected[i]
			require.Equal(t, expect.key, string(item.Key()), "expected key: %s actual key: %s",
				expect.key, item.Key())
			require.Equal(t, expect.val, string(v), "key: %s expected value: %s actual %s",
				item.key, expect.val, v)
			require.Equal(t, expect.version, int(item.Version()),
				"key: %s expected version: %d actual %d", item.key, expect.version, item.Version())
			require.Equal(t, expect.meta, item.meta,
				"key: %s expected meta: %d meta %d", item.key, expect.meta, item.meta)
			i++
		}
		require.Equal(t, len(expected), i, "keys examined should be equal to keys expected")
		return nil
	})

}

func TestCompaction(t *testing.T) {
	// Disable compactions and keep single version of each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(1)
	opt.managedTxns = true
	t.Run("level 0 to level 1", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}}
			l01 := []keyValVersion{{"foo", "bar", 2, 0}}
			l1 := []keyValVersion{{"foo", "bar", 1, 0}}
			// Level 0 has table l0 and l01.
			createAndOpen(db, l0, 0)
			createAndOpen(db, l01, 0)
			// Level 1 has table l1.
			createAndOpen(db, l1, 1)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0}, {"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0}, {"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[0],
				nextLevel: db.lc.levels[1],
				top:       db.lc.levels[0].tables,
				bot:       db.lc.levels[1].tables,
			}
			require.NoError(t, db.lc.runCompactDef(0, cdef))
			// foo version 2 should be dropped after compaction.
			getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}})
		})
	})

	t.Run("level 0 to level 1 with lower overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}}
			l01 := []keyValVersion{{"foo", "bar", 2, 0}}
			l1 := []keyValVersion{{"foo", "bar", 1, 0}}
			l2 := []keyValVersion{{"foo", "bar", 0, 0}}
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
				{"foo", "bar", 3, 0}, {"foo", "bar", 2, 0}, {"foo", "bar", 1, 0},
				{"foo", "bar", 0, 0}, {"fooz", "baz", 1, 0},
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
				{"foo", "bar", 3, 0}, {"foo", "bar", 0, 0}, {"fooz", "baz", 1, 0},
			})
		})
	})

	t.Run("level 1 to level 2", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}}
			l2 := []keyValVersion{{"foo", "bar", 2, 0}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0}, {"foo", "bar", 2, 0}, {"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
			}
			require.NoError(t, db.lc.runCompactDef(1, cdef))
			// foo version 2 should be dropped after compaction.
			getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, 0}})
		})
	})

	t.Run("level 1 to level 2 with delete", func(t *testing.T) {
		t.Run("with overlap", func(t *testing.T) {
			runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
				l1 := []keyValVersion{{"foo", "bar", 3, bitDelete}, {"fooz", "baz", 1, bitDelete}}
				l2 := []keyValVersion{{"foo", "bar", 2, 0}}
				l3 := []keyValVersion{{"foo", "bar", 1, 0}}
				createAndOpen(db, l1, 1)
				createAndOpen(db, l2, 2)
				createAndOpen(db, l3, 3)

				// Set a high discard timestamp so that all the keys are below the discard timestamp.
				db.SetDiscardTs(10)

				getAllAndCheck(t, db, []keyValVersion{
					{"foo", "bar", 3, 1},
					{"foo", "bar", 2, 0},
					{"foo", "bar", 1, 0},
					{"fooz", "baz", 1, 1},
				})
				cdef := compactDef{
					thisLevel: db.lc.levels[1],
					nextLevel: db.lc.levels[2],
					top:       db.lc.levels[1].tables,
					bot:       db.lc.levels[2].tables,
				}
				require.NoError(t, db.lc.runCompactDef(1, cdef))
				// foo bar version 2 should be dropped after compaction. fooz
				// baz version 1 will remain because overlap exists, which is
				// expected because `hasOverlap` is only checked once at the
				// beginning of `compactBuildTables` method.
				// everything from level 1 is now in level 2.
				getAllAndCheck(t, db, []keyValVersion{
					{"foo", "bar", 3, bitDelete},
					{"foo", "bar", 1, 0},
					{"fooz", "baz", 1, 1},
				})

				cdef = compactDef{
					thisLevel: db.lc.levels[2],
					nextLevel: db.lc.levels[3],
					top:       db.lc.levels[2].tables,
					bot:       db.lc.levels[3].tables,
				}
				require.NoError(t, db.lc.runCompactDef(2, cdef))
				// everything should be removed now
				getAllAndCheck(t, db, []keyValVersion{})
			})
		})
		t.Run("with bottom overlap", func(t *testing.T) {
			runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
				l1 := []keyValVersion{{"foo", "bar", 3, bitDelete}}
				l2 := []keyValVersion{{"foo", "bar", 2, 0}, {"fooz", "baz", 2, bitDelete}}
				l3 := []keyValVersion{{"fooz", "baz", 1, 0}}
				createAndOpen(db, l1, 1)
				createAndOpen(db, l2, 2)
				createAndOpen(db, l3, 3)

				// Set a high discard timestamp so that all the keys are below the discard timestamp.
				db.SetDiscardTs(10)

				getAllAndCheck(t, db, []keyValVersion{
					{"foo", "bar", 3, bitDelete},
					{"foo", "bar", 2, 0},
					{"fooz", "baz", 2, bitDelete},
					{"fooz", "baz", 1, 0},
				})
				cdef := compactDef{
					thisLevel: db.lc.levels[1],
					nextLevel: db.lc.levels[2],
					top:       db.lc.levels[1].tables,
					bot:       db.lc.levels[2].tables,
				}
				require.NoError(t, db.lc.runCompactDef(1, cdef))
				// the top table at L1 doesn't overlap L3, but the bottom table at L2
				// does, delete keys should not be removed.
				getAllAndCheck(t, db, []keyValVersion{
					{"foo", "bar", 3, bitDelete},
					{"fooz", "baz", 2, bitDelete},
					{"fooz", "baz", 1, 0},
				})
			})
		})
		t.Run("without overlap", func(t *testing.T) {
			runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
				l1 := []keyValVersion{{"foo", "bar", 3, bitDelete}, {"fooz", "baz", 1, bitDelete}}
				l2 := []keyValVersion{{"fooo", "barr", 2, 0}}
				createAndOpen(db, l1, 1)
				createAndOpen(db, l2, 2)

				// Set a high discard timestamp so that all the keys are below the discard timestamp.
				db.SetDiscardTs(10)

				getAllAndCheck(t, db, []keyValVersion{
					{"foo", "bar", 3, 1}, {"fooo", "barr", 2, 0}, {"fooz", "baz", 1, 1},
				})
				cdef := compactDef{
					thisLevel: db.lc.levels[1],
					nextLevel: db.lc.levels[2],
					top:       db.lc.levels[1].tables,
					bot:       db.lc.levels[2].tables,
				}
				require.NoError(t, db.lc.runCompactDef(1, cdef))
				// foo version 2 should be dropped after compaction.
				getAllAndCheck(t, db, []keyValVersion{{"fooo", "barr", 2, 0}})
			})
		})
	})
}

func TestCompactionTwoVersions(t *testing.T) {
	// Disable compactions and keep two versions of each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(2)
	opt.managedTxns = true
	t.Run("with overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, bitDelete}}
			l2 := []keyValVersion{{"foo", "bar", 2, 0}}
			l3 := []keyValVersion{{"foo", "bar", 1, 0}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)
			createAndOpen(db, l3, 3)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 1},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
			}
			require.NoError(t, db.lc.runCompactDef(1, cdef))
			// Nothing should be dropped after compaction because number of
			// versions to keep is 2.
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 1},
			})

			cdef = compactDef{
				thisLevel: db.lc.levels[2],
				nextLevel: db.lc.levels[3],
				top:       db.lc.levels[2].tables,
				bot:       db.lc.levels[3].tables,
			}
			require.NoError(t, db.lc.runCompactDef(2, cdef))
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
			})
		})
	})
}

func TestCompactionAllVersions(t *testing.T) {
	// Disable compactions and keep all versions of the each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(math.MaxInt32)
	opt.managedTxns = true
	t.Run("without overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar", 3, 0}, {"fooz", "baz", 1, bitDelete}}
			l2 := []keyValVersion{{"foo", "bar", 2, 0}}
			l3 := []keyValVersion{{"foo", "bar", 1, 0}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)
			createAndOpen(db, l3, 3)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 1},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
			}
			require.NoError(t, db.lc.runCompactDef(1, cdef))
			// Nothing should be dropped after compaction because all versions
			// should be kept.
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
				{"fooz", "baz", 1, 1},
			})

			cdef = compactDef{
				thisLevel: db.lc.levels[2],
				nextLevel: db.lc.levels[3],
				top:       db.lc.levels[2].tables,
				bot:       db.lc.levels[3].tables,
			}
			require.NoError(t, db.lc.runCompactDef(2, cdef))
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0},
				{"foo", "bar", 1, 0},
			})
		})
	})
	t.Run("without overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar", 3, bitDelete}, {"fooz", "baz", 1, bitDelete}}
			l2 := []keyValVersion{{"fooo", "barr", 2, 0}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, 1}, {"fooo", "barr", 2, 0}, {"fooz", "baz", 1, 1},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
			}
			require.NoError(t, db.lc.runCompactDef(1, cdef))
			// foo version 2 should be dropped after compaction.
			getAllAndCheck(t, db, []keyValVersion{{"fooo", "barr", 2, 0}})
		})
	})
}

func TestHeadKeyCleanup(t *testing.T) {
	// Disable compactions and keep single version of each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(1)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l0 := []keyValVersion{
			{string(head), "foo", 5, 0}, {string(head), "bar", 4, 0}, {string(head), "baz", 3, 0},
		}
		l1 := []keyValVersion{{string(head), "fooz", 2, 0}, {string(head), "foozbaz", 1, 0}}
		// Level 0 has table l0 and l01.
		createAndOpen(db, l0, 0)
		// Level 1 has table l1.
		createAndOpen(db, l1, 1)

		// Set a high discard timestamp so that all the keys are below the discard timestamp.
		db.SetDiscardTs(10)

		getAllAndCheck(t, db, []keyValVersion{
			{string(head), "foo", 5, 0}, {string(head), "bar", 4, 0}, {string(head), "baz", 3, 0},
			{string(head), "fooz", 2, 0}, {string(head), "foozbaz", 1, 0},
		})
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))
		// foo version 2 should be dropped after compaction.
		getAllAndCheck(t, db, []keyValVersion{{string(head), "foo", 5, 0}})
	})
}

func TestDiscardTs(t *testing.T) {
	// Disable compactions and keep single version of each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(1)
	opt.managedTxns = true

	t.Run("all keys above discardTs", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{{"foo", "bar", 4, 0}, {"fooz", "baz", 3, 0}}
			l01 := []keyValVersion{{"foo", "bar", 3, 0}}
			l1 := []keyValVersion{{"foo", "bar", 2, 0}}
			// Level 0 has table l0 and l01.
			createAndOpen(db, l0, 0)
			createAndOpen(db, l01, 0)
			// Level 1 has table l1.
			createAndOpen(db, l1, 1)

			// Set dicardTs to 1. All the keys are above discardTs.
			db.SetDiscardTs(1)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
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
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
			})
		})
	})
	t.Run("some keys above discardTs", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0}, {"fooz", "baz", 2, 0},
			}
			l1 := []keyValVersion{{"foo", "bbb", 1, 0}}
			createAndOpen(db, l0, 0)
			createAndOpen(db, l1, 1)

			// Set dicardTs to 3. foo2 and foo1 should be dropped.
			db.SetDiscardTs(3)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0}, {"foo", "bar", 2, 0},
				{"foo", "bbb", 1, 0}, {"fooz", "baz", 2, 0},
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
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0}, {"fooz", "baz", 2, 0},
			})
		})
	})
	t.Run("all keys below discardTs", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l0 := []keyValVersion{{"foo", "bar", 4, 0}, {"fooz", "baz", 3, 0}}
			l01 := []keyValVersion{{"foo", "bar", 3, 0}}
			l1 := []keyValVersion{{"foo", "bar", 2, 0}}
			// Level 0 has table l0 and l01.
			createAndOpen(db, l0, 0)
			createAndOpen(db, l01, 0)
			// Level 1 has table l1.
			createAndOpen(db, l1, 1)

			// Set dicardTs to 10. All the keys are below discardTs.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 4, 0}, {"foo", "bar", 3, 0},
				{"foo", "bar", 2, 0}, {"fooz", "baz", 3, 0},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[0],
				nextLevel: db.lc.levels[1],
				top:       db.lc.levels[0].tables,
				bot:       db.lc.levels[1].tables,
			}
			require.NoError(t, db.lc.runCompactDef(0, cdef))
			// Only one version of every key should be left.
			getAllAndCheck(t, db, []keyValVersion{{"foo", "bar", 4, 0}, {"fooz", "baz", 3, 0}})
		})
	})
}

// This is a test to ensure that the first entry with DiscardEarlierversion bit < DiscardTs
// is kept around (when numversionstokeep is infinite).
func TestDiscardFirstVersion(t *testing.T) {
	opt := DefaultOptions("")
	opt.NumCompactors = 0
	opt.NumVersionsToKeep = math.MaxInt32
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l0 := []keyValVersion{{"foo", "bar", 1, 0}}
		l01 := []keyValVersion{{"foo", "bar", 2, bitDiscardEarlierVersions}}
		l02 := []keyValVersion{{"foo", "bar", 3, 0}}
		l03 := []keyValVersion{{"foo", "bar", 4, 0}}
		l04 := []keyValVersion{{"foo", "bar", 9, 0}}
		l05 := []keyValVersion{{"foo", "bar", 10, bitDiscardEarlierVersions}}

		// Level 0 has all the tables.
		createAndOpen(db, l0, 0)
		createAndOpen(db, l01, 0)
		createAndOpen(db, l02, 0)
		createAndOpen(db, l03, 0)
		createAndOpen(db, l04, 0)
		createAndOpen(db, l05, 0)

		// Discard Time stamp is set to 7.
		db.SetDiscardTs(7)

		// Compact L0 to L1
		cdef := compactDef{
			thisLevel: db.lc.levels[0],
			nextLevel: db.lc.levels[1],
			top:       db.lc.levels[0].tables,
			bot:       db.lc.levels[1].tables,
		}
		require.NoError(t, db.lc.runCompactDef(0, cdef))

		// - Version 10, 9 lie above version 7 so they should be there.
		// - Version 4, 3, 2 lie below the discardTs but they don't have the
		//   "bitDiscardEarlierVersions" versions set so they should not be removed because number
		//    of versions to keep is set to infinite.
		// - Version 1 is below DiscardTS and below the first "bitDiscardEarlierVersions"
		//   marker so IT WILL BE REMOVED.
		ExpectedKeys := []keyValVersion{
			{"foo", "bar", 10, bitDiscardEarlierVersions},
			{"foo", "bar", 9, 0},
			{"foo", "bar", 4, 0},
			{"foo", "bar", 3, 0},
			{"foo", "bar", 2, bitDiscardEarlierVersions}}

		getAllAndCheck(t, db, ExpectedKeys)
	})
}

// This test ensures we don't stall when L1's size is greater than opt.LevelOneSize.
// We should stall only when L0 tables more than the opt.NumLevelZeroTableStall.
func TestL1Stall(t *testing.T) {
	opt := DefaultOptions("")
	// Disable all compactions.
	opt.NumCompactors = 0
	// Number of level zero tables.
	opt.NumLevelZeroTables = 3
	// Addition of new tables will stall if there are 4 or more L0 tables.
	opt.NumLevelZeroTablesStall = 4
	// Level 1 size is 10 bytes.
	opt.LevelOneSize = 10

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Level 0 has 4 tables.
		db.lc.levels[0].Lock()
		db.lc.levels[0].tables = []*table.Table{createEmptyTable(db), createEmptyTable(db),
			createEmptyTable(db), createEmptyTable(db)}
		db.lc.levels[0].Unlock()

		timeout := time.After(5 * time.Second)
		done := make(chan bool)

		// This is important. Set level 1 size more than the opt.LevelOneSize (we've set it to 10).
		db.lc.levels[1].totalSize = 100
		go func() {
			tab := createEmptyTable(db)
			require.NoError(t, db.lc.addLevel0Table(tab))
			tab.DecrRef()
			done <- true
		}()
		time.Sleep(time.Second)

		db.lc.levels[0].Lock()
		// Drop two tables from Level 0 so that addLevel0Table can make progress. Earlier table
		// count was 4 which is equal to L0 stall count.
		toDrop := db.lc.levels[0].tables[:2]
		decrRefs(toDrop)
		db.lc.levels[0].tables = db.lc.levels[0].tables[2:]
		db.lc.levels[0].Unlock()

		select {
		case <-timeout:
			t.Fatal("Test didn't finish in time")
		case <-done:
		}
	})
}

func createEmptyTable(db *DB) *table.Table {
	opts := table.Options{
		BloomFalsePositive: db.opt.BloomFalsePositive,
		LoadingMode:        options.LoadToRAM,
		ChkMode:            options.NoVerification,
	}
	b := table.NewTableBuilder(opts)
	// Add one key so that we can open this table.
	b.Add(y.KeyWithTs([]byte("foo"), 1), y.ValueStruct{}, 0)

	// Open table in memory to avoid adding changes to manifest file.
	tab, err := table.OpenInMemoryTable(b.Finish(), db.lc.reserveFileID(), &opts)
	if err != nil {
		panic(err)
	}

	return tab
}

func TestL0Stall(t *testing.T) {
	test := func(t *testing.T, opt *Options) {
		runBadgerTest(t, opt, func(t *testing.T, db *DB) {
			db.lc.levels[0].Lock()
			// Add NumLevelZeroTableStall+1 number of tables to level 0. This would fill up level
			// zero and all new additions are expected to stall if L0 is in memory.
			for i := 0; i < opt.NumLevelZeroTablesStall+1; i++ {
				db.lc.levels[0].tables = append(db.lc.levels[0].tables, createEmptyTable(db))
			}
			db.lc.levels[0].Unlock()

			timeout := time.After(5 * time.Second)
			done := make(chan bool)

			go func() {
				tab := createEmptyTable(db)
				require.NoError(t, db.lc.addLevel0Table(tab))
				tab.DecrRef()
				done <- true
			}()
			// Let it stall for a second.
			time.Sleep(time.Second)

			select {
			case <-timeout:
				if opt.KeepL0InMemory {
					t.Log("Timeout triggered")
					// Mark this test as successful since L0 is in memory and the
					// addition of new table to L0 is supposed to stall.

					// Remove tables from level 0 so that the stalled
					// compaction can make progress. This does not have any
					// effect on the test. This is done so that the goroutine
					// stuck on addLevel0Table can make progress and end.
					db.lc.levels[0].Lock()
					db.lc.levels[0].tables = nil
					db.lc.levels[0].Unlock()
					<-done
				} else {
					t.Fatal("Test didn't finish in time")
				}
			case <-done:
				// The test completed before 5 second timeout. Mark it as successful.
			}
		})
	}

	opt := DefaultOptions("")
	// Disable all compactions.
	opt.NumCompactors = 0
	// Number of level zero tables.
	opt.NumLevelZeroTables = 3
	// Addition of new tables will stall if there are 4 or more L0 tables.
	opt.NumLevelZeroTablesStall = 4

	t.Run("with KeepL0InMemory", func(t *testing.T) {
		opt.KeepL0InMemory = true
		test(t, &opt)
	})
	t.Run("with L0 on disk", func(t *testing.T) {
		opt.KeepL0InMemory = false
		test(t, &opt)
	})
}

// Regression test for https://github.com/dgraph-io/dgraph/issues/5573
func TestDropPrefixMoveBug(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// l1 is used to verify that drop prefix actually drops move keys from all the levels.
		l1 := []keyValVersion{{string(append(badgerMove, "F"...)), "", 0, 0}}
		createAndOpen(db, l1, 1)

		// Mutiple levels can have the exact same move key with version.
		l2 := []keyValVersion{{string(append(badgerMove, "F"...)), "", 0, 0}, {"A", "", 0, 0}}
		l21 := []keyValVersion{{"B", "", 0, 0}, {"C", "", 0, 0}}
		l22 := []keyValVersion{{"F", "", 0, 0}, {"G", "", 0, 0}}

		// Level 2 has all the tables.
		createAndOpen(db, l2, 2)
		createAndOpen(db, l21, 2)
		createAndOpen(db, l22, 2)

		require.NoError(t, db.lc.validate())
		require.NoError(t, db.DropPrefix([]byte("F")))

		db.View(func(txn *Txn) error {
			iopt := DefaultIteratorOptions
			iopt.AllVersions = true

			it := txn.NewIterator(iopt)
			defer it.Close()

			specialKey := []byte("F")
			droppedPrefixes := [][]byte{specialKey, append(badgerMove, specialKey...)}
			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().Key()
				// Ensure we don't have any "F" or "!badger!move!F" left
				require.False(t, hasAnyPrefixes(key, droppedPrefixes))
			}
			return nil
		})
		require.NoError(t, db.lc.validate())
	})
}
