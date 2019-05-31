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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/options"
)

func TestTableIndex(t *testing.T) {
	keyPrefix := "key"
	t.Run("single key", func(t *testing.T) {
		f := buildTestTable(t, keyPrefix, 1)
		table, err := OpenTable(f, options.MemoryMap, nil)
		require.NoError(t, err)
		require.Len(t, table.blockIndex, 1)
	})
	// t.Run("multiple keys", func(t *testing.T) {
	// 	keyCount := 10000
	// 	f := buildTestTable(t, keyPrefix, keyCount)
	// 	table, err := OpenTable(f, options.MemoryMap, nil)
	// 	require.NoError(t, err)
	// 	require.Len(t, table.blockIndex, keyCount/restartInterval)
	// 	// Ensure index is built correctly
	// 	for i, ko := range table.blockIndex {
	// 		require.Equal(t, ko.Key, y.KeyWithTs([]byte(key(keyPrefix, restartInterval*i)), 0))
	// 	}
	// })
}
