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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"regexp"
	"testing"

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

	dir, err := ioutil.TempDir("", "badger")
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

// Badger dir to be used for performing db.Open benchmark
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
	opts := DefaultOptions
	opts.MaxTableSize = 1 << 20
	opts.ValueLogMaxEntries = 64
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
	opt := DefaultOptions
	opt.ValueLogMaxEntries = 64
	opt.ValueLogFileSize = 2 << 30
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
	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	opt.ReadOnly = true
	for i := 0; i < b.N; i++ {
		db, err := Open(opt)
		require.NoError(b, err)
		require.NoError(b, db.Close())
	}
}
