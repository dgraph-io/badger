/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func TestTruncateVlogWithClose(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%d%10d", i, i))
	}
	data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		require.NoError(t, err)
		return m
	}

	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	opt.SyncWrites = true
	opt.Truncate = true
	opt.ValueThreshold = 1 // Force all reads from value log.

	db, err := Open(opt)
	require.NoError(t, err)

	err = db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry(key(0), data(4055)))
	})
	require.NoError(t, err)

	// Close the DB.
	require.NoError(t, db.Close())
	require.NoError(t, os.Truncate(path.Join(dir, "000000.vlog"), 4096))

	// Reopen and write some new data.
	db, err = Open(opt)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		err := db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry(key(i), data(10)))
		})
		require.NoError(t, err)
	}
	// Read it back to ensure that we can read it now.
	for i := 0; i < 32; i++ {
		err := db.View(func(txn *Txn) error {
			item, err := txn.Get(key(i))
			require.NoError(t, err)
			val := getItemValue(t, item)
			require.Equal(t, 10, len(val))
			return nil
		})
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// Reopen and read the data again.
	db, err = Open(opt)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		err := db.View(func(txn *Txn) error {
			item, err := txn.Get(key(i))
			require.NoError(t, err)
			val := getItemValue(t, item)
			require.Equal(t, 10, len(val))
			return nil
		})
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
}

var manual = flag.Bool("manual", false, "Set when manually running some tests.")

// Badger dir to be used for performing db.Open benchmark.
var benchDir = flag.String("benchdir", "", "Set when running db.Open benchmark")

// The following 3 TruncateVlogNoClose tests should be run one after another.
// None of these close the DB, simulating a crash. They should be run with a
// script, which truncates the value log to 4096, lining up with the end of the
// first entry in the txn. At <4096, it would cause the entry to be truncated
// immediately, at >4096, same thing.
func TestTruncateVlogNoClose(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	fmt.Println("running")
	dir := "p"
	opts := getTestOptions(dir)
	opts.SyncWrites = true
	opts.Truncate = true

	kv, err := Open(opts)
	require.NoError(t, err)
	key := func(i int) string {
		return fmt.Sprintf("%d%10d", i, i)
	}
	data := fmt.Sprintf("%4055d", 1)
	err = kv.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte(key(0)), []byte(data)))
	})
	require.NoError(t, err)
}
func TestTruncateVlogNoClose2(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	dir := "p"
	opts := getTestOptions(dir)
	opts.SyncWrites = true
	opts.Truncate = true

	kv, err := Open(opts)
	require.NoError(t, err)
	key := func(i int) string {
		return fmt.Sprintf("%d%10d", i, i)
	}
	data := fmt.Sprintf("%10d", 1)
	for i := 32; i < 64; i++ {
		err := kv.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte(key(i)), []byte(data)))
		})
		require.NoError(t, err)
	}
	for i := 32; i < 64; i++ {
		require.NoError(t, kv.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(key(i)))
			require.NoError(t, err)
			val := getItemValue(t, item)
			require.NotNil(t, val)
			require.True(t, len(val) > 0)
			return nil
		}))
	}
}
func TestTruncateVlogNoClose3(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	fmt.Print("Running")
	dir := "p"
	opts := getTestOptions(dir)
	opts.SyncWrites = true
	opts.Truncate = true

	kv, err := Open(opts)
	require.NoError(t, err)
	key := func(i int) string {
		return fmt.Sprintf("%d%10d", i, i)
	}
	for i := 32; i < 64; i++ {
		require.NoError(t, kv.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(key(i)))
			require.NoError(t, err)
			val := getItemValue(t, item)
			require.NotNil(t, val)
			require.True(t, len(val) > 0)
			return nil
		}))
	}
}

func TestBigKeyValuePairs(t *testing.T) {
	// This test takes too much memory. So, run separately.
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}

	// Passing an empty directory since it will be filled by runBadgerTest.
	opts := DefaultOptions("").
		WithMaxTableSize(1 << 20).
		WithValueLogMaxEntries(64)
	runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
		bigK := make([]byte, 65001)
		bigV := make([]byte, db.opt.ValueLogFileSize+1)
		small := make([]byte, 65000)

		txn := db.NewTransaction(true)
		require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.SetEntry(NewEntry(bigK, small)))
		require.Regexp(t, regexp.MustCompile("Value.*exceeded"),
			txn.SetEntry(NewEntry(small, bigV)))

		require.NoError(t, txn.SetEntry(NewEntry(small, small)))
		require.Regexp(t, regexp.MustCompile("Key.*exceeded"), txn.SetEntry(NewEntry(bigK, bigV)))

		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get(small)
			require.Equal(t, ErrKeyNotFound, err)
			return nil
		}))

		// Now run a longer test, which involves value log GC.
		data := fmt.Sprintf("%100d", 1)
		key := func(i int) string {
			return fmt.Sprintf("%65000d", i)
		}

		saveByKey := func(key string, value []byte) error {
			return db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte(key), value))
			})
		}

		getByKey := func(key string) error {
			return db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(key))
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					if len(val) == 0 {
						log.Fatalf("key not found %q", len(key))
					}
					return nil
				})
			})
		}

		for i := 0; i < 32; i++ {
			if i < 30 {
				require.NoError(t, saveByKey(key(i), []byte(data)))
			} else {
				require.NoError(t, saveByKey(key(i), []byte(fmt.Sprintf("%100d", i))))
			}
		}

		for j := 0; j < 5; j++ {
			for i := 0; i < 32; i++ {
				if i < 30 {
					require.NoError(t, saveByKey(key(i), []byte(data)))
				} else {
					require.NoError(t, saveByKey(key(i), []byte(fmt.Sprintf("%100d", i))))
				}
			}
		}

		for i := 0; i < 32; i++ {
			require.NoError(t, getByKey(key(i)))
		}

		var loops int
		var err error
		for err == nil {
			err = db.RunValueLogGC(0.5)
			require.NotRegexp(t, regexp.MustCompile("truncate"), err)
			loops++
		}
		t.Logf("Ran value log GC %d times. Last error: %v\n", loops, err)
	})
}

// The following test checks for issue #585.
func TestPushValueLogLimit(t *testing.T) {
	// This test takes too much memory. So, run separately.
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}

	// Passing an empty directory since it will be filled by runBadgerTest.
	opt := DefaultOptions("").
		WithValueLogMaxEntries(64).
		WithValueLogFileSize(2 << 30)
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		data := []byte(fmt.Sprintf("%30d", 1))
		key := func(i int) string {
			return fmt.Sprintf("%100d", i)
		}

		for i := 0; i < 32; i++ {
			if i == 4 {
				v := make([]byte, 2<<30)
				err := db.Update(func(txn *Txn) error {
					return txn.SetEntry(NewEntry([]byte(key(i)), v))
				})
				require.NoError(t, err)
			} else {
				err := db.Update(func(txn *Txn) error {
					return txn.SetEntry(NewEntry([]byte(key(i)), data))
				})
				require.NoError(t, err)
			}
		}

		for i := 0; i < 32; i++ {
			err := db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(key(i)))
				require.NoError(t, err, "Getting key: %s", key(i))
				err = item.Value(func(v []byte) error {
					_ = v
					return nil
				})
				require.NoError(t, err, "Getting value: %s", key(i))
				return nil
			})
			require.NoError(t, err)
		}
	})
}

// The following benchmark test is supposed to be run against a badger directory with some data.
// Use badger fill to create data if it doesn't exist.
func BenchmarkDBOpen(b *testing.B) {
	if *benchDir == "" {
		b.Skip("Please set -benchdir to badger directory")
	}
	dir := *benchDir
	// Passing an empty directory since it will be filled by runBadgerTest.
	opt := DefaultOptions(dir).
		WithReadOnly(true)
	for i := 0; i < b.N; i++ {
		db, err := Open(opt)
		require.NoError(b, err)
		require.NoError(b, db.Close())
	}
}

// Regression test for https://github.com/dgraph-io/badger/issues/830
func TestDiscardMapTooBig(t *testing.T) {
	createDiscardStats := func() map[uint32]int64 {
		stat := map[uint32]int64{}
		for i := uint32(0); i < 8000; i++ {
			stat[i] = 0
		}
		return stat
	}
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err, "error while openning db")

	// Add some data so that memtable flush happens on close
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("foo"), []byte("bar"))
	}))

	// overwrite discardstat with large value
	db.vlog.lfDiscardStats = &lfDiscardStats{
		m: createDiscardStats(),
	}

	require.NoError(t, db.Close())
	// reopen the same DB
	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err, "error while openning db")
	require.NoError(t, db.Close())
}

// Test for values of size uint32.
func TestBigValues(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	opts := DefaultOptions("").
		WithValueThreshold(1 << 20).
		WithValueLogMaxEntries(100)
	runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
		keyCount := 1000

		data := bytes.Repeat([]byte("a"), (1 << 20)) // Valuesize 1 MB.
		key := func(i int) string {
			return fmt.Sprintf("%65000d", i)
		}

		saveByKey := func(key string, value []byte) error {
			return db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte(key), value))
			})
		}

		getByKey := func(key string) error {
			return db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(key))
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					if len(val) == 0 || len(val) != len(data) || !bytes.Equal(val, []byte(data)) {
						log.Fatalf("key not found %q", len(key))
					}
					return nil
				})
			})
		}

		for i := 0; i < keyCount; i++ {
			require.NoError(t, saveByKey(key(i), []byte(data)))
		}

		for i := 0; i < keyCount; i++ {
			require.NoError(t, getByKey(key(i)))
		}
	})
}

// This test is for compaction. We are creating db with two levels. We have 10 tables on level 3 and
// 3 tables on level 2. Tables on level 2 have overlap with 2, 4, 3 tables on level 3. This test
// will take more memory and time, hence adding it for manual run.
func TestCompaction(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}

	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(DefaultOptions(dir).WithTableLoadingMode(options.LoadToRAM))
	require.NoError(t, err, "error while opening db")
	defer func() {
		require.NoError(t, db.Close())
	}()

	l3 := db.lc.levels[3]
	start := 0
	incr := 300000 // Every table has 300,000 entries.
	for i := 0; i < 10; i++ {
		tab := createTableWithRange(t, db, start, start+incr, 1)
		addToMenifest(t, db, tab, 3)
		start += incr
		require.NoError(t, l3.replaceTables([]*table.Table{}, []*table.Table{tab}))
	}

	l2 := db.lc.levels[2]
	// first table keys from 0 - 600,000
	tab := createTableWithRange(t, db, 0, 600000, 2)
	addToMenifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	// second table have keys from 600,001 to 1,800,000
	tab = createTableWithRange(t, db, 600001, 1800000, 4)
	addToMenifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	// third table have keys from 1,800,001 to 2,700,000
	tab = createTableWithRange(t, db, 1800001, 2700000, 4)
	addToMenifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	for len(db.lc.levels[2].tables) > 0 {
		now := time.Now()
		require.NoError(t, db.lc.doCompact(compactionPriority{level: 2}))
		fmt.Println("compaction completed in time: ", time.Since(now))
	}
}

// addToMenifest function is used in TestCompaction. It adds table to db menifest.
func addToMenifest(t *testing.T, db *DB, tab *table.Table, level uint32) {
	change := &pb.ManifestChange{
		Id:    tab.ID(),
		Op:    pb.ManifestChange_CREATE,
		Level: level,
	}
	require.NoError(t, db.manifest.addChanges([]*pb.ManifestChange{change}),
		"unable to add to menfest")
}

// createTableWithRange function is used in TestCompaction. It creates a table with key starting
// from with start and ending with end with diff between each key as incr.
func createTableWithRange(t *testing.T, db *DB, start, end, incr int) *table.Table {
	b := table.NewTableBuilder()
	for i := start; i < end; i += incr {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key[:], uint64(i))
		key = y.KeyWithTs(key, uint64(0))
		val := y.ValueStruct{Value: []byte(fmt.Sprintf("%d", i))}
		require.NoError(t, b.Add(key, val))
	}

	fileID := db.lc.reserveFileID()
	fd, err := y.CreateSyncedFile(table.NewFilename(fileID, db.opt.Dir), true)
	require.NoError(t, err)

	_, err = fd.Write(b.Finish())
	require.NoError(t, err, "unable to write to file")

	tab, err := table.OpenTable(fd, options.LoadToRAM, options.NoVerification)
	require.NoError(t, err)
	return tab
}
