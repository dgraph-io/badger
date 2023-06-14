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
	"expvar"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteMetrics(t *testing.T) {
	opt := getTestOptions("")
	opt.managedTxns = true
	opt.CompactL0OnClose = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		num := 10
		val := make([]byte, 1<<12)
		key := make([]byte, 40)
		for i := 0; i < num; i++ {
			_, err := rand.Read(key)
			require.NoError(t, err)
			_, err = rand.Read(val)
			require.NoError(t, err)

			writer := db.NewManagedWriteBatch()
			require.NoError(t, writer.SetEntryAt(NewEntry(key, val), 1))
			writer.Flush()
		}

		expectedSize := int64(len(val)) + 48 + 2 // 48 := size of key (40 + 8(ts)), 2 := meta
		write_metric := expvar.Get("badger_v4_write_user")
		require.Equal(t, expectedSize*int64(num), write_metric.(*expvar.Int).Value())

		put_metric := expvar.Get("badger_v4_puts_total")
		require.Equal(t, int64(num), put_metric.(*expvar.Int).Value())

		lsm_metric := expvar.Get("badger_v4_lsm_written_bytes")
		require.Equal(t, expectedSize*int64(num), lsm_metric.(*expvar.Int).Value())
		fmt.Println(lsm_metric)

		compactionMetric := expvar.Get("badger_v4_compaction_written_bytes")
		require.Equal(t, int64(0), compactionMetric.(*expvar.Int).Value())

		db.Close()

		db, err := OpenManaged(opt)
		require.Nil(t, err)

		compactionMetric = expvar.Get("badger_v4_compaction_written_bytes")
		require.GreaterOrEqual(t, expectedSize*int64(num)+int64(num*200), compactionMetric.(*expvar.Int).Value())
		// Because we have random values, compression is not able to do much, so we incur a cost on total size
	})
}
