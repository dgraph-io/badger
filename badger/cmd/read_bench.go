package cmd

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
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

	rand.Seed(time.Now().UnixNano())

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

	closer := y.NewCloser(0)
	for i := 0; i < numGoroutines; i++ {
		closer.AddRunning(1)
		go spawnRoutine(i, db, closer, keys)
	}

	<-time.After(dur)
	closer.SignalAndWait()

	return nil
}

func spawnRoutine(id int, db *badger.DB, c *y.Closer, keys [][]byte) {
	defer c.Done()

	start := time.Now()
	now := time.Now()
	sz := uint64(0)
	count := uint64(0)

	printStats := func() {
		dur := time.Since(now)
		durSec := dur.Seconds()
		if int(durSec) > 0 {
			bytesRate := sz / uint64(durSec)
			countRate := count / uint64(durSec)
			fmt.Printf("Routine: %d, Time elapsed: %s, bytes read: %s, speed: %s/sec, "+
				"entries read: %d, speed: %d/sec\n", id, y.FixedDuration(time.Since(start)),
				humanize.Bytes(sz), humanize.Bytes(bytesRate), count, countRate)
		}
	}

	t := time.NewTicker(5 * time.Second) // TODO:(Ashish): decide on the tick duration
	defer t.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-t.C:
			printStats()
		default:
			sz += runIteration(db, keys)
			count++
		}
	}
}

func runIteration(db *badger.DB, keys [][]byte) uint64 {
	r := rand.Intn(len(keys))
	var sz uint64
	key := keys[r]
	err := db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			panic(err)
		}
		if _, err = itm.ValueCopy(nil); err != nil {
			panic(err)
		}
		sz = uint64(itm.EstimatedSize())
		return nil
	})
	if err != nil {
		panic(err)
	}
	return sz
}

// getKeys returns all keys present in database.
// TODO:(Ashish): Need sampling for very large DB.
func getKeys(db *badger.DB) ([][]byte, error) {
	var keys [][]byte
	err := db.View(func(txn *badger.Txn) error {
		itrOps := badger.DefaultIteratorOptions
		itrOps.PrefetchValues = false
		itr := txn.NewIterator(itrOps)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			itm := itr.Item()
			key := itm.KeyCopy(nil)
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}
