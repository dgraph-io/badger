package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var readBenchCmd = &cobra.Command{
	Use:   "read_bench",
	Short: "ReadBench reads data from Badger randomly to benchmark read speed.",
	Long: `
This command would read data from existing Badger randomly using multiple go routines. Useful for 
testing and performance analysis.
`,
	RunE: readBench,
}

const maxKeys = 1000

var sizeRead, entriesRead uint64
var startTime time.Time

func init() {
	RootCmd.AddCommand(readBenchCmd)
	readBenchCmd.Flags().IntVarP(
		&numGoroutines, "goroutines", "g", 4, "Number of goroutines to run for reading.")
	readBenchCmd.Flags().StringVarP(&duration, "duration", "d", "1m",
		"How long to run the benchmark.")
}

func readBench(cmd *cobra.Command, args []string) error {
	dur, err := time.ParseDuration(duration)
	y.Check(err)
	y.AssertTrue(numGoroutines > 0)

	opts := badger.DefaultOptions
	opts.ReadOnly = true
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.TableLoadingMode = options.FileIO // TODO: make this configurable
	opts.ValueLogLoadingMode = options.FileIO

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	now := time.Now()
	keys, err := getKeys(db)
	if err != nil {
		return err
	}
	fmt.Printf("Total Keys: %d, read in time: %s\n", len(keys), time.Since(now))

	if len(keys) == 0 {
		fmt.Println("DB is empty, hence returning")
		return nil
	}

	c := y.NewCloser(0)
	startTime = time.Now()
	for i := 0; i < numGoroutines; i++ {
		c.AddRunning(1)
		go spawnRoutine(db, c, keys)
	}

	// spawn printStats
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
			durSec := dur.Seconds()
			if int(durSec) > 0 {
				sz := atomic.LoadUint64(&sizeRead)
				entries := atomic.LoadUint64(&entriesRead)
				bytesRate := sz / uint64(durSec)
				entriesRate := entries / uint64(durSec)
				fmt.Printf("Time elapsed: %s, bytes read: %s, speed: %s/sec, "+
					"entries read: %d, speed: %d/sec\n", y.FixedDuration(time.Since(startTime)),
					humanize.Bytes(sz), humanize.Bytes(bytesRate), entries, entriesRate)
			}
		}
	}
}

func spawnRoutine(db *badger.DB, c *y.Closer, keys [][]byte) {
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
		if err != nil {
			panic(err)
		}
		if _, err := itm.ValueCopy(nil); err != nil {
			panic(err)
		}
		sz = uint64(itm.EstimatedSize())
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

func getKeys(db *badger.DB) ([][]byte, error) {
	var keys [][]byte
	count := 0
	stream := db.NewStream()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.Kv {
			keys = append(keys, kv.Key)
		}
		count += len(list.Kv)
		if count >= maxKeys {
			// we only want maxKeys
			cancel()
		}
		return nil
	}

	if err := stream.Orchestrate(ctx); err != nil {
		if err != context.Canceled {
			return nil, err
		}
	}

	return keys, nil
}
