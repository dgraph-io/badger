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

package cmd

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

var writeBenchCmd = &cobra.Command{
	Use:   "write",
	Short: "Writes random data to Badger to benchmark write speed.",
	Long: `
This command writes random data to Badger to benchmark write speed. Useful for testing and
performance analysis.
`,
	RunE: writeBench,
}

var (
	keySz   int
	valSz   int
	numKeys float64
	force   bool
	sorted  bool

	sizeWritten    uint64
	entriesWritten uint64
)

const (
	mil float64 = 1e6
)

func init() {
	benchCmd.AddCommand(writeBenchCmd)
	writeBenchCmd.Flags().IntVarP(&keySz, "key-size", "k", 32, "Size of key")
	writeBenchCmd.Flags().IntVarP(&valSz, "val-size", "v", 128, "Size of value")
	writeBenchCmd.Flags().Float64VarP(&numKeys, "keys-mil", "m", 10.0,
		"Number of keys to add in millions")
	writeBenchCmd.Flags().BoolVarP(&force, "force-compact", "f", true,
		"Force compact level 0 on close.")
	writeBenchCmd.Flags().BoolVarP(&sorted, "sorted", "s", false, "Write keys in sorted order.")
}

func writeRandom(db *badger.DB, num uint64) error {
	value := make([]byte, valSz)
	y.Check2(rand.Read(value))

	es := uint64(keySz + valSz) // entry size is keySz + valSz
	batch := db.NewWriteBatch()
	for i := uint64(1); i <= num; i++ {
		key := make([]byte, keySz)
		y.Check2(rand.Read(key))
		if err := batch.Set(key, value); err != nil {
			return err
		}

		atomic.AddUint64(&entriesWritten, 1)
		atomic.AddUint64(&sizeWritten, es)
	}
	return batch.Flush()
}

func writeSorted(db *badger.DB, num uint64) error {
	value := make([]byte, valSz)
	y.Check2(rand.Read(value))
	es := 8 + valSz // key size is 8 bytes and value size is valSz

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

			sz += es
			atomic.AddUint64(&entriesWritten, 1)
			atomic.AddUint64(&sizeWritten, uint64(es))

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
	streamID := uint32(0)
	for start := uint64(0); start < num; start += width {
		end := start + width
		if end > num {
			end = num
		}
		streamID++
		wg.Add(1)
		go writeRange(start, end, streamID)
	}
	go func() {
		wg.Wait()
		close(writeCh)
	}()
	log.Printf("Max StreamId used: %d. Width: %d\n", streamID, width)
	for kvs := range writeCh {
		if err := writer.Write(kvs); err != nil {
			panic(err)
		}
	}
	log.Println("DONE streaming. Flushing...")
	return writer.Flush()
}

func writeBench(cmd *cobra.Command, args []string) error {
	db, err := badger.Open(badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithTruncate(truncate).
		WithSyncWrites(false).
		WithCompactL0OnClose(force).
		WithLogger(nil))
	if err != nil {
		return err
	}
	defer func() {
		start := time.Now()
		err := db.Close()
		log.Printf("DB.Close. Error: %v. Time taken to close: %s", err, time.Since(start))
	}()

	fmt.Println("*********************************************************")
	fmt.Println("Starting to benchmark Writes")
	fmt.Println("*********************************************************")

	startTime = time.Now()
	num := uint64(numKeys * mil)
	c := y.NewCloser(1)
	go reportStats(c)

	if sorted {
		err = writeSorted(db, num)
	} else {
		err = writeRandom(db, num)
	}

	c.SignalAndWait()
	return err
}

func reportStats(c *y.Closer) {
	defer c.Done()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-t.C:
			dur := time.Since(startTime)
			sz := atomic.LoadUint64(&sizeWritten)
			entries := atomic.LoadUint64(&entriesWritten)
			bytesRate := sz / uint64(dur.Seconds())
			entriesRate := entries / uint64(dur.Seconds())
			fmt.Printf("Time elapsed: %s, bytes written: %s, speed: %s/sec, "+
				"entries written: %d, speed: %d/sec\n", y.FixedDuration(time.Since(startTime)),
				humanize.Bytes(sz), humanize.Bytes(bytesRate), entries, entriesRate)
		}
	}
}
