/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

func TestVlogOnlyWal(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	opt.ValueLogMaxEntries = 10 // Generate more vlog files.
	opt.WALMode = false

	var count int
	db, err := Open(opt)
	require.NoError(t, err)

	insert := func() {
		v := []byte("bar")
		for i := 0; i <= 100; i++ {
			txn := db.NewTransaction(true)
			k := []byte(fmt.Sprintf("foo-%d", i))
			require.NoError(t, txn.Set(k, v))
			require.NoError(t, txn.Commit())
		}
	}
	insert()

	count = len(db.vlog.filesMap)
	require.NoError(t, db.Close())

	// Use new Dir
	dir, err = ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt.Dir = dir
	opt.ValueDir = dir
	opt.WALMode = true

	db, err = Open(opt)
	require.NoError(t, err)

	insert()

	require.Less(t, len(db.vlog.filesMap), count)

	txn := db.NewTransaction(false)
	for i := 0; i <= 100; i++ {
		key := []byte(fmt.Sprintf("foo-%d", i))
		item, err := txn.Get(key)
		require.NoError(t, err)

		item.Value(func(v []byte) error {
			require.Equal(t, v, []byte("bar"))
			return nil
		})
	}
	txn.Discard()
	require.NoError(t, db.Close())
}

// Test if we can move between vlog and wal configurations.
func TestVlogWAL(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	opt.ValueLogMaxEntries = 10 // Generate more vlog files.

	insert := func(db *DB, start, end int) {
		v := []byte("bar")
		for i := start; i <= end; i++ {
			txn := db.NewTransaction(true)
			k := []byte(fmt.Sprintf("foo-%d", i))
			require.NoError(t, txn.Set(k, v))
			require.NoError(t, txn.Commit())
		}
	}
	read := func(db *DB, start, end int) {
		txn := db.NewTransaction(false)
		for i := start; i <= end; i++ {
			key := []byte(fmt.Sprintf("foo-%d", i))
			item, err := txn.Get(key)
			require.NoError(t, err)

			item.Value(func(v []byte) error {
				require.Equal(t, v, []byte("bar"))
				return nil
			})
		}
		txn.Discard()
	}

	var origvc int
	// run 1 in normal mode.
	func() {
		opt.WALMode = false
		db, err := Open(opt)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close())
		}()
		insert(db, 0, 100)
		read(db, 0, 100)
		vc, wc := countFiles(dir)
		require.Equal(t, vc, 17)
		// No wal files in vlog mode.
		require.Zero(t, wc)
		origvc = vc
	}()

	// Run 2 with WAL mode on the same directory.
	func() {
		opt.WALMode = true
		db1, err := Open(opt)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db1.Close())
		}()

		insert(db1, 101, 200)
		read(db1, 0, 200)
		vc, wc := countFiles(dir)
		// There should be some vlog files and wal files.
		// Number of vlog files should remain the same.
		require.Equal(t, vc, origvc)
		// Some wal files in wal mode.
		require.NotZero(t, wc)
	}()

	// Run 3 with vlog mode.
	func() {
		opt.WALMode = false
		db2, err := Open(opt)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db2.Close())
		}()
		insert(db2, 201, 300)
		read(db2, 0, 300)
		vc, wc := countFiles(dir)
		// vlog files will be 2x because we're inserting same amount of data again.
		require.Equal(t, vc, origvc*2)
		// all wal files should be cleaned up.
		require.Zero(t, wc)
	}()
}

func TestWalModeChange(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	opt.ValueLogMaxEntries = 10 // Generate more vlog files.
	opt.WALMode = false

	maxFid := uint32(0)
	openAndCount := func(walMode bool, vlogCount, walCount int) {
		opt := DefaultOptions(dir)
		opt.ValueLogMaxEntries = 10 // Generate more vlog files.
		opt.WALMode = walMode

		db, err := Open(opt)
		require.NoError(t, err)

		// Mode change should create a new file.
		db.vlog.filesLock.Lock()
		require.Equal(t, maxFid, db.vlog.maxFid)

		lf, ok := db.vlog.filesMap[db.vlog.maxFid]
		db.vlog.filesLock.Unlock()
		require.True(t, ok)
		require.Equal(t, maxFid, db.vlog.maxFid)
		if walMode {
			require.Equal(t, lf.fileType, walFile)
		} else {
			require.Equal(t, lf.fileType, vlogFile)
		}
		require.NoError(t, db.Close())

		vc, wc := countFiles(dir)
		require.Equal(t, vlogCount, vc)
		require.Equal(t, walCount, wc)
	}

	openAndCount(false, 1, 0)
	// Wal mode should create a new file.
	maxFid++
	openAndCount(true, 1, 1)
	// multiple calls in the same mode shouldn't create a new file.
	openAndCount(true, 1, 1)
	openAndCount(true, 1, 1)

	maxFid++
	openAndCount(false, 2, 0)
	// multiple calls in the same mode shouldn't create a new file.
	openAndCount(false, 2, 0)
	openAndCount(false, 2, 0)
}

func countFiles(dir string) (int, int) {
	vlogCount := 0
	walCount := 0
	files, err := ioutil.ReadDir(dir)
	y.Check(err)
	for _, fi := range files {
		switch {
		case strings.HasSuffix(fi.Name(), vlogSuffix):
			vlogCount++
		case strings.HasSuffix(fi.Name(), walSuffix):
			walCount++
		}

	}
	return vlogCount, walCount
}

func TestWALModeGC(t *testing.T) {
	opt := DefaultOptions("")
	opt.WALMode = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		require.Error(t, db.RunValueLogGC(0.1))
	})
}

func TestWALBigValue(t *testing.T) {
	opt := DefaultOptions("")
	opt.WALMode = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		bigVal := make([]byte, 10<<20)
		require.Greater(t, len(bigVal), maxValueThreshold)

		// Try inserting via txn.
		txn := db.NewTransaction(true)
		defer txn.Discard()

		expectedErr := exceedsSize("WALMode Value", int64(db.opt.ValueThreshold), bigVal).
			Error()
		require.EqualError(t, txn.Set([]byte("foo"), bigVal), expectedErr)

		wb := db.NewWriteBatch()
		require.EqualError(t, wb.Set([]byte("foo"), bigVal), expectedErr)
		wb.Cancel()

		// Try inserting via batch set.
		e := []*Entry{{Value: bigVal}}
		require.EqualError(t, db.batchSet(e), expectedErr)

		// Try inserting via stream writer.
		sw := db.NewStreamWriter()
		require.NoError(t, sw.Prepare())
		require.EqualError(t, sw.Write(&pb.KVList{
			Kv: []*pb.KV{{Value: bigVal}},
		}), expectedErr)
		require.NoError(t, sw.Flush())
	})
}

// TODO (ibrahim): Add test which generates some vlog files and then some wal
// files. Reopen the same DB in vlog mode and run GC. Ensure none of the wal
// files are GC'ed (if they exist)
func TestGcWithWAL(t *testing.T) {
}
