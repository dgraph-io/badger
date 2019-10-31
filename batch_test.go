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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
)

// func TestWriteBatch(t *testing.T) {
// 	key := func(i int) []byte {
// 		return []byte(fmt.Sprintf("%10d", i))
// 	}
// 	val := func(i int) []byte {
// 		return []byte(fmt.Sprintf("%128d", i))
// 	}

// 	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
// 		//	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
// 		wb := db.NewWriteBatch()
// 		defer wb.Cancel()

// 		N, M := 50000, 1000
// 		start := time.Now()

// 		for i := 0; i < N; i++ {
// 			require.NoError(t, wb.Set(key(i), val(i)))
// 		}
// 		for i := 0; i < M; i++ {
// 			require.NoError(t, wb.Delete(key(i)))
// 		}
// 		require.NoError(t, wb.Flush())
// 		t.Logf("Time taken for %d writes (w/ test options): %s\n", N+M, time.Since(start))

// 		err := db.View(func(txn *Txn) error {
// 			itr := txn.NewIterator(DefaultIteratorOptions)
// 			defer itr.Close()

// 			i := M
// 			for itr.Rewind(); itr.Valid(); itr.Next() {
// 				item := itr.Item()
// 				require.Equal(t, string(key(i)), string(item.Key()))
// 				valcopy, err := item.ValueCopy(nil)
// 				require.NoError(t, err)
// 				require.Equal(t, val(i), valcopy)
// 				i++
// 			}
// 			require.Equal(t, N, i)
// 			return nil
// 		})
// 		require.NoError(t, err)
// 	})
// }

func BenchmarkWriteBatch(b *testing.B) {
	dir, err := ioutil.TempDir(".", "badger-test")
	y.Check(err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.TableLoadingMode = options.LoadToRAM
	db, err := Open(opts)
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := db.NewWriteBatch()
		for j := 0; j < 10000; j++ {
			wb.Set(key(j), val(j))
		}
		wb.Flush()
		wb.Cancel()
	}
}

func BenchmarkLogWrite(b *testing.B) {
	dir, err := ioutil.TempDir(".", "badger-test")
	y.Check(err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.TableLoadingMode = options.LoadToRAM
	db, err := Open(opts)
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}

	y.Check(err)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		req := new(request)
		for j := 0; j < 30; j++ {
			e := &Entry{
				Key:   key(j),
				Value: val(j),
			}
			if j%2 == 0 {
				e.forceWal = true
			}
			req.Entries = append(req.Entries, e)
		}
		db.log.write([]*request{req})
	}
}
