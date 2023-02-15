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
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
)

func TestManifestBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	{
		kv, err := Open(opt)
		require.NoError(t, err)
		n := 5000
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%16x", rand.Int63()))
			txnSet(t, kv, k, k, 0x00)
		}
		txnSet(t, kv, []byte("testkey"), []byte("testval"), 0x05)
		require.NoError(t, kv.validate())
		require.NoError(t, kv.Close())
	}

	kv, err := Open(opt)
	require.NoError(t, err)

	require.NoError(t, kv.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("testkey"))
		require.NoError(t, err)
		require.EqualValues(t, "testval", string(getItemValue(t, item)))
		require.EqualValues(t, byte(0x05), item.UserMeta())
		return nil
	}))
	require.NoError(t, kv.Close())
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	{
		kv, err := Open(opt)
		require.NoError(t, err)
		require.NoError(t, kv.Close())
	}
	fp, err := os.OpenFile(filepath.Join(dir, ManifestFilename), os.O_RDWR, 0)
	require.NoError(t, err)
	// Mess with magic value or version to force error
	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	kv, err := Open(opt)
	defer func() {
		if kv != nil {
			kv.Close()
		}
	}()
	require.Error(t, err)
	require.Contains(t, err.Error(), errorContent)
}

func TestManifestMagic(t *testing.T) {
	helpTestManifestFileCorruption(t, 3, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	helpTestManifestFileCorruption(t, 6, "unsupported version")
}

func TestManifestChecksum(t *testing.T) {
	helpTestManifestFileCorruption(t, 15, "checksum mismatch")
}

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

// TODO - Move these to somewhere where table package can also use it.
// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string, bopts table.Options) *table.Table {
	if bopts.BloomFalsePositive == 0 {
		bopts.BloomFalsePositive = 0.01
	}
	if bopts.BlockSize == 0 {
		bopts.BlockSize = 4 * 1024
	}
	b := table.NewTableBuilder(bopts)
	defer b.Close()
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.

	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		b.Add(y.KeyWithTs([]byte(kv[0]), 10), y.ValueStruct{
			Value:    []byte(kv[1]),
			Meta:     'A',
			UserMeta: 0,
		}, 0)
	}

	tbl, err := table.CreateTable(filename, b)
	require.NoError(t, err)
	return tbl
}

func TestManifestRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	deletionsThreshold := 10
	mf, m, err := helpOpenOrCreateManifestFile(dir, false, 0, deletionsThreshold)
	defer func() {
		if mf != nil {
			mf.close()
		}
	}()
	require.NoError(t, err)
	require.Equal(t, 0, m.Creations)
	require.Equal(t, 0, m.Deletions)

	err = mf.addChanges([]*pb.ManifestChange{
		newCreateChange(0, 0, 0, 0),
	})
	require.NoError(t, err)

	for i := uint64(0); i < uint64(deletionsThreshold*3); i++ {
		ch := []*pb.ManifestChange{
			newCreateChange(i+1, 0, 0, 0),
			newDeleteChange(i),
		}
		err := mf.addChanges(ch)
		require.NoError(t, err)
	}
	err = mf.close()
	require.NoError(t, err)
	mf = nil
	mf, m, err = helpOpenOrCreateManifestFile(dir, false, 0, deletionsThreshold)
	require.NoError(t, err)
	require.Equal(t, map[uint64]TableManifest{
		uint64(deletionsThreshold * 3): {Level: 0},
	}, m.Tables)
}

func TestConcurrentManifestCompaction(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// overwrite the sync function to make this race condition easily reproducible
	syncFunc = func(f *os.File) error {
		// effectively making the Sync() take around 1s makes this reproduce every time
		time.Sleep(1 * time.Second)
		return f.Sync()
	}

	mf, _, err := helpOpenOrCreateManifestFile(dir, false, 0, 0)
	require.NoError(t, err)

	cs := &pb.ManifestChangeSet{}
	for i := uint64(0); i < 1000; i++ {
		cs.Changes = append(cs.Changes,
			newCreateChange(i, 0, 0, 0),
			newDeleteChange(i),
		)
	}

	// simulate 2 concurrent compaction threads
	n := 2
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, mf.addChanges(cs.Changes))
		}()
	}
	wg.Wait()

	require.NoError(t, mf.close())
}
