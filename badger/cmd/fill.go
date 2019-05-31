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

package cmd

import (
	"encoding/binary"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
	"github.com/spf13/cobra"
)

var fillCmd = &cobra.Command{
	Use:   "fill",
	Short: "Fill Badger with random data.",
	Long: `
This command would fill Badger with random data. Useful for testing and performance analysis.
`,
	RunE: fill,
}

var keySz, valSz int
var numKeys float64
var force, sorted bool

const mil float64 = 1e6

func init() {
	RootCmd.AddCommand(fillCmd)
	fillCmd.Flags().IntVarP(&keySz, "key-size", "k", 32, "Size of key")
	fillCmd.Flags().IntVarP(&valSz, "val-size", "v", 128, "Size of value")
	fillCmd.Flags().Float64VarP(&numKeys, "keys-mil", "m", 10.0,
		"Number of keys to add in millions")
	fillCmd.Flags().BoolVarP(&force, "force-compact", "f", true, "Force compact level 0 on close.")
	fillCmd.Flags().BoolVarP(&sorted, "sorted", "s", false, "Write keys in sorted order.")
}

func fillRandom(db *badger.DB, num uint64) error {
	value := make([]byte, valSz)
	y.Check2(rand.Read(value))

	batch := db.NewWriteBatch()
	for i := uint64(1); i <= num; i++ {
		key := make([]byte, keySz)
		y.Check2(rand.Read(key))
		if err := batch.Set(key, value); err != nil {
			return err
		}
		if i%1e5 == 0 {
			log.Printf("Written keys: %d\n", i)
		}
	}
	return batch.Flush()
}

func fillSorted(db *badger.DB, num uint64) error {
	value := make([]byte, valSz)
	y.Check2(rand.Read(value))

	writer := db.NewStreamWriter()
	if err := writer.Prepare(); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	writeCh := make(chan *pb.KVList, 3)
	writeRange := func(start, end uint64, streamId uint32) {
		// end is not included.
		defer wg.Done()
		kvs := &pb.KVList{}
		var sz int
		for i := start; i < end; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			kvs.Kv = append(kvs.Kv, &pb.KV{
				Key:      key,
				Value:    value,
				Version:  1,
				StreamId: streamId,
			})
			sz += len(key) + len(value)
			if sz >= 4<<20 { // 4 MB
				writeCh <- kvs
				kvs = &pb.KVList{}
				sz = 0
			}
		}
		writeCh <- kvs
	}

	// Let's create some streams.
	width := num / 16
	streamId := uint32(0)
	for start := uint64(0); start < num; start += width {
		end := start + width
		if end > num {
			end = num
		}
		streamId++
		wg.Add(1)
		go writeRange(start, end, streamId)
	}
	go func() {
		wg.Wait()
		close(writeCh)
	}()
	log.Printf("Max StreamId used: %d. Width: %d\n", streamId, width)
	for kvs := range writeCh {
		if err := writer.Write(kvs); err != nil {
			panic(err)
		}
	}
	log.Println("DONE streaming. Flushing...")
	return writer.Flush()
}

func fill(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.Truncate = truncate
	opts.SyncWrites = false
	opts.CompactL0OnClose = force

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer func() {
		start := time.Now()
		err := db.Close()
		log.Printf("DB.Close. Error: %v. Time taken: %s", err, time.Since(start))
	}()

	num := uint64(numKeys * mil)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		log.Printf("%d keys written. Time taken: %s\n", num, dur)
	}()
	if sorted {
		return fillSorted(db, num)
	}
	return fillRandom(db, num)
}
