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
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestValueBasic(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Use value big enough that the value log writes them even if SyncWrites is false.
		const val1 = "sampleval012345678901234567890123"
		const val2 = "samplevalb012345678901234567890123"
		require.True(t, len(val1) >= db.opt.ValueThreshold)

		e1 := &Entry{
			Key:   []byte("samplekey"),
			Value: []byte(val1),
			meta:  bitValuePointer,
		}
		e2 := &Entry{
			Key:   []byte("samplekeyb"),
			Value: []byte(val2),
			meta:  bitValuePointer,
		}

		b := new(request)
		b.Entries = []*Entry{e1, e2}
		db.log.write([]*request{b})
		require.Len(t, b.Ptrs, 2)
		s := new(y.Slice)
		buf1, lf1, err1 := db.log.readValueBytes(b.Ptrs[0], s)
		buf2, lf2, err2 := db.log.readValueBytes(b.Ptrs[1], s)
		require.NoError(t, err1)
		require.NoError(t, err2)
		defer runCallback(db.log.getUnlockCallback(lf1))
		defer runCallback(db.log.getUnlockCallback(lf2))
		e1, err := lf1.decodeEntry(buf1, b.Ptrs[0].Offset)
		require.NoError(t, err)
		e2, err = lf1.decodeEntry(buf2, b.Ptrs[1].Offset)
		require.NoError(t, err)
		readEntries := []Entry{*e1, *e2}
		require.EqualValues(t, []Entry{
			{
				Key:    []byte("samplekey"),
				Value:  []byte(val1),
				meta:   bitValuePointer,
				offset: b.Ptrs[0].Offset,
			},
			{
				Key:    []byte("samplekeyb"),
				Value:  []byte(val2),
				meta:   bitValuePointer,
				offset: b.Ptrs[1].Offset,
			},
		}, readEntries)
	})
}

func TestValueGCManaged(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	N := 500
	opt := getTestOptions(dir)
	opt.ValueLogMaxEntries = uint32(N / 10)
	opt.managedTxns = true
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	var ts uint64
	newTs := func() uint64 {
		ts++
		return ts
	}

	sz := 64 << 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])

		wg.Add(1)
		txn := db.NewTransactionAt(newTs(), true)
		require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%d", i)), v)))
		require.NoError(t, txn.CommitAt(newTs(), func(err error) {
			wg.Done()
			require.NoError(t, err)
		}))
	}

	for i := 0; i < N; i++ {
		wg.Add(1)
		txn := db.NewTransactionAt(newTs(), true)
		require.NoError(t, txn.Delete([]byte(fmt.Sprintf("key%d", i))))
		require.NoError(t, txn.CommitAt(newTs(), func(err error) {
			wg.Done()
			require.NoError(t, err)
		}))
	}
	wg.Wait()
	files, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	for _, fi := range files {
		t.Logf("File: %s. Size: %s\n", fi.Name(), humanize.Bytes(uint64(fi.Size())))
	}

	// for i := 0; i < 100; i++ {
	// 	// Try at max 100 times to GC even a single value log file.
	// 	if err := db.RunValueLogGC(0.0001); err == nil {
	// 		return // Done
	// 	}
	// }
	// require.Fail(t, "Unable to GC even a single value log file.")
}
