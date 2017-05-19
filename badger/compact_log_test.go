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
	"testing"

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
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	{
		kv, _ := NewKV(opt)
		n := 5000
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%16x", rand.Int63()))
			kv.Set(k, k)
		}
		kv.Set([]byte("testkey"), []byte("testval"))
		kv.validate()
		kv.debugPrintMore()
		kv.Close()
	}

	kv, _ := NewKV(opt)
	val, _ := kv.Get([]byte("testkey"))
	require.EqualValues(t, "testval", string(val))
	kv.Close()
}

// TODO: Fix test. There seems to be some compaction being undone which is unexpected.
func TestCompactLogUnclosedIter(t *testing.T) {
	// Create unclosed iterators. This will leave a lot of files in the directory.
	// Then re-open the database and check that everything is cleanup.
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	iterOpt := IteratorOptions{}
	iterOpt.FetchValues = true
	iterOpt.PrefetchSize = 10

	opt := getTestOptions(dir)
	var sum *summary
	{
		kv, _ := NewKV(opt)
		n := 5000
		for i := 0; i < n; i++ {
			if (i % 1000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
				kv.NewIterator(iterOpt) // NOTE: Hold reference for test.
			}
			k := []byte(fmt.Sprintf("%16x", rand.Int63()))
			kv.Set(k, k)
		}
		// Don't close kv.
		sum = kv.lc.getSummary()
	}

	// Make sure our test makes sense. There should be dirty files.
	require.True(t, len(sum.fileIDs) < len(getIDMap(dir)))

	kv, _ := NewKV(opt) // This should clean up.
	summary2 := kv.lc.getSummary()
	require.Len(t, sum.fileIDs, len(summary2.fileIDs))
}
