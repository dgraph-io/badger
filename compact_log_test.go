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
	"sort"
	"testing"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func TestCompactLogEncode(t *testing.T) {
	// Test basic serialization and deserialization.
	fd, err := ioutil.TempFile("", "badger_")
	require.NoError(t, err)
	filename := fd.Name()
	defer os.Remove(filename)

	cl := &compactLog{fd: fd}
	cl.add(&compaction{
		compactID: 1234,
		done:      0,
		toInsert:  []uint64{4, 7, 100},
		toDelete:  []uint64{666},
	})
	cl.add(&compaction{
		compactID: 5755,
		done:      1,
		toInsert:  []uint64{12, 4, 5}, // Should be ignored.
	})
	fd.Close()

	var compactions []*compaction
	compactLogIterate(filename, func(c *compaction) {
		compactions = append(compactions, c)
	})

	require.Len(t, compactions, 2)
	require.EqualValues(t, 1234, compactions[0].compactID)
	require.EqualValues(t, 0, compactions[0].done)
	require.EqualValues(t, []uint64{4, 7, 100}, compactions[0].toInsert)
	require.EqualValues(t, []uint64{666}, compactions[0].toDelete)

	require.EqualValues(t, 5755, compactions[1].compactID)
	require.EqualValues(t, 1, compactions[1].done)
	require.Empty(t, compactions[1].toDelete)
	require.Empty(t, compactions[1].toInsert)
}

func TestCompactLogBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	{
		kv, err := NewKV(opt)
		require.NoError(t, err)
		n := 5000
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%16x", rand.Int63()))
			kv.Set(k, k, 0x00)
		}
		kv.Set([]byte("testkey"), []byte("testval"), 0x05)
		kv.validate()
		require.NoError(t, kv.Close())
	}

	kv, err := NewKV(opt)
	require.NoError(t, err)

	var item KVItem
	if err := kv.Get([]byte("testkey"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, "testval", string(item.Value()))
	require.EqualValues(t, byte(0x05), item.UserMeta())
	require.NoError(t, kv.Close())
}

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	y.AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues)
}

// TODO - Move these to somewhere where table package can also use it.
// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	b := table.NewTableBuilder()
	defer b.Close()
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.

	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for i, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		err := b.Add([]byte(kv[0]), y.ValueStruct{[]byte(kv[1]), 'A', 0, uint64(i)})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	f.Write(b.Finish([]byte("somemetadata")))
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	return f
}

func TestOverlappingKeyRangeError(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	kv, err := NewKV(&opt)
	require.NoError(t, err)

	lh0 := newLevelHandler(kv, 0)
	lh1 := newLevelHandler(kv, 1)
	f := buildTestTable(t, "k", 2)
	t1, err := table.OpenTable(f, table.MemoryMap)
	require.NoError(t, err)
	defer t1.DecrRef()

	done := lh0.tryAddLevel0Table(t1)
	require.Equal(t, true, done)

	cd := compactDef{
		thisLevel: lh0,
		nextLevel: lh1,
		elog:      trace.New("Badger", "Compact"),
	}

	lc, err := newLevelsController(kv)
	require.NoError(t, err)
	done = lc.fillTablesL0(&cd)
	require.Equal(t, true, done)
	lc.runCompactDef(0, cd)

	f = buildTestTable(t, "l", 2)
	t2, err := table.OpenTable(f, table.MemoryMap)
	require.NoError(t, err)
	defer t2.DecrRef()
	done = lh0.tryAddLevel0Table(t2)
	require.Equal(t, true, done)

	cd = compactDef{
		thisLevel: lh0,
		nextLevel: lh1,
		elog:      trace.New("Badger", "Compact"),
	}
	lc.fillTablesL0(&cd)
	lc.runCompactDef(0, cd)
}
