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
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

var readBenchCmd = &cobra.Command{
	Use:   "read",
	Short: "Read data from Badger randomly to benchmark read speed.",
	Long: `
This command reads data from existing Badger database randomly using multiple go routines.`,
	RunE: readBench,
}

var (
	sizeRead    uint64    // will store size read till now
	entriesRead uint64    // will store entries read till now
	startTime   time.Time // start time of read benchmarking

	sampleSize  int
	loadingMode string
	keysOnly    bool
	readOnly    bool
)

func init() {
	benchCmd.AddCommand(readBenchCmd)
	readBenchCmd.Flags().IntVarP(
		&numGoroutines, "goroutines", "g", 16, "Number of goroutines to run for reading.")
	readBenchCmd.Flags().StringVarP(
		&duration, "duration", "d", "1m", "How long to run the benchmark.")
	readBenchCmd.Flags().IntVar(
		&sampleSize, "sample-size", 1000000, "Keys sample size to be used for random lookup.")
	readBenchCmd.Flags().BoolVar(
		&keysOnly, "keys-only", false, "If false, values will also be read.")
	readBenchCmd.Flags().BoolVar(
		&readOnly, "read-only", true, "If true, DB will be opened in read only mode.")
	readBenchCmd.Flags().StringVar(
		&loadingMode, "loading-mode", "mmap", "Mode for accessing SSTables and value log files. "+
			"Valid loading modes are fileio and mmap.")
}

func readBench(cmd *cobra.Command, args []string) error {
	rand.Seed(time.Now().Unix())

	dur, err := time.ParseDuration(duration)
	if err != nil {
		return y.Wrapf(err, "unable to parse duration")
	}
	y.AssertTrue(numGoroutines > 0)
	mode := getLoadingMode(loadingMode)

	db, err := badger.Open(badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithReadOnly(readOnly).
		WithTableLoadingMode(mode).
		WithValueLogLoadingMode(mode))
	if err != nil {
		return y.Wrapf(err, "unable to open DB")
	}
	defer db.Close()

	now := time.Now()
	keys, err := getSampleKeys(db)
	if err != nil {
		return y.Wrapf(err, "error while sampling keys")
	}
	fmt.Println("*********************************************************")
	fmt.Printf("Total Sampled Keys: %d, read in time: %s\n", len(keys), time.Since(now))
	fmt.Println("*********************************************************")

	if len(keys) == 0 {
		fmt.Println("DB is empty, hence returning")
		return nil
	}

	fmt.Println("*********************************************************")
	fmt.Println("Starting to benchmark Reads")
	fmt.Println("*********************************************************")
	c := y.NewCloser(0)
	startTime = time.Now()
	for i := 0; i < numGoroutines; i++ {
		c.AddRunning(1)
		go readKeys(db, c, keys)
	}

	// also start printing stats
	c.AddRunning(1)
	go printStats(c)

	<-time.After(dur)
	c.SignalAndWait()

	return nil
}

func printStats(c *y.Closer) {
	defer c.Done()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-t.C:
			dur := time.Since(startTime)
			sz := atomic.LoadUint64(&sizeRead)
			entries := atomic.LoadUint64(&entriesRead)
			bytesRate := sz / uint64(dur.Seconds())
			entriesRate := entries / uint64(dur.Seconds())
			fmt.Printf("Time elapsed: %s, bytes read: %s, speed: %s/sec, "+
				"entries read: %d, speed: %d/sec\n", y.FixedDuration(time.Since(startTime)),
				humanize.Bytes(sz), humanize.Bytes(bytesRate), entries, entriesRate)
		}
	}
}

func readKeys(db *badger.DB, c *y.Closer, keys [][]byte) {
	defer c.Done()
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for {
		select {
		case <-c.HasBeenClosed():
			return
		default:
			key := keys[r.Int31n(int32(len(keys)))]
			atomic.AddUint64(&sizeRead, lookupForKey(db, key))
			atomic.AddUint64(&entriesRead, 1)
		}
	}
}

func lookupForKey(db *badger.DB, key []byte) (sz uint64) {
	err := db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		y.Check(err)

		if keysOnly {
			sz = uint64(itm.KeySize())
		} else {
			y.Check2(itm.ValueCopy(nil))
			sz = uint64(itm.EstimatedSize())
		}

		return nil
	})
	y.Check(err)
	return
}

// getSampleKeys uses stream framework internally, to get keys in random order.
func getSampleKeys(db *badger.DB) ([][]byte, error) {
	var keys [][]byte
	count := 0
	stream := db.NewStream()

	// overide stream.KeyToList as we only want keys. Also
	// we can take only first version for the key.
	stream.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		l := &pb.KVList{}
		// Since stream framework copies the item's key while calling
		// KeyToList, we can directly append key to list.
		l.Kv = append(l.Kv, &pb.KV{Key: key})
		return l, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream.Send = func(l *pb.KVList) error {
		if count >= sampleSize {
			return nil
		}
		for _, kv := range l.Kv {
			keys = append(keys, kv.Key)
			count++
			if count >= sampleSize {
				cancel()
				return nil
			}
		}
		return nil
	}

	if err := stream.Orchestrate(ctx); err != nil && err != context.Canceled {
		return nil, err
	}

	// Shuffle keys before returning to minimise locality
	// of keys coming from stream framework.
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys, nil
}

func getLoadingMode(m string) options.FileLoadingMode {
	m = strings.ToLower(m)
	var mode options.FileLoadingMode
	switch m {
	case "fileio":
		mode = options.FileIO
	case "mmap":
		mode = options.MemoryMap
	default:
		panic("loading mode not supported")
	}

	return mode
}
