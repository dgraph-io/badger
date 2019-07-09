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

package table

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
)

func TestTableIndex(t *testing.T) {
	rand.Seed(time.Now().Unix())
	keyPrefix := "key"
	t.Run("single key", func(t *testing.T) {
		f := buildTestTable(t, keyPrefix, 1)
		tbl, err := OpenTable(f, options.MemoryMap, options.OnTableAndBlockRead)
		require.NoError(t, err)
		require.Len(t, tbl.blockIndex, 1)
	})

	t.Run("multiple keys", func(t *testing.T) {
		keysCount := 10000
		builder := NewTableBuilder()
		filename := fmt.Sprintf("%s%c%d.sst", os.TempDir(), os.PathSeparator, rand.Int63())
		f, err := y.OpenSyncedFile(filename, true)
		require.NoError(t, err)

		blockFirstKeys := make([][]byte, 0)
		blockCount := 0
		for i := 0; i < keysCount; i++ {
			k := []byte(fmt.Sprintf("%016x", i))
			v := fmt.Sprintf("%d", i)
			vs := y.ValueStruct{Value: []byte(v)}
			if i == 0 { // This is first key for first block.
				blockFirstKeys = append(blockFirstKeys, k)
				blockCount = 1
			} else if builder.shouldFinishBlock(k, vs) {
				blockCount++
				blockFirstKeys = append(blockFirstKeys, k)
			}
			y.Check(builder.Add(k, vs))
		}
		f.Write(builder.Finish())

		tbl, err := OpenTable(f, options.LoadToRAM, options.OnTableAndBlockRead)
		require.NoError(t, err, "unable to open table")

		// Ensure index is built correctly
		require.Equal(t, blockCount, len(tbl.blockIndex))
		for i, ko := range tbl.blockIndex {
			require.Equal(t, ko.Key, blockFirstKeys[i])
		}
	})
}
