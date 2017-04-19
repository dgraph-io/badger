/* This binary is mainly used for testing, for now.
For example, test for compact log replay is hard to automate. This main is
supposed to make it easier.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"

	"github.com/dgraph-io/badger/badger"
	"github.com/dgraph-io/badger/value"
	"github.com/dgraph-io/badger/y"
)

var (
	flagDir = flag.String("dir", "bench-tmp", "Where data is temporarily stored.")
)

func getOptions(dir string) *badger.Options {
	opt := new(badger.Options)
	*opt = badger.DefaultOptions
	opt.MaxTableSize = 32 << 20  // Force more compaction.
	opt.LevelOneSize = 200 << 20 // Force more compaction.
	opt.Verbose = true
	opt.Dir = dir
	opt.ValueThreshold = 1000
	return opt
}

func main() {
	flag.Parse()
	if !flag.Parsed() {
		log.Fatal("Unable to parse flags")
	}

	y.AssertTrue(len(*flagDir) > 0)
	kv := badger.NewKV(getOptions(*flagDir))
	ctx := context.Background()

	// Keep writing random keys.
	val := make([]byte, 10)
	entries := make([]*value.Entry, 1000)
	for i := 0; i < len(entries); i++ {
		entries[i] = new(value.Entry)
		entries[i].Value = val
	}

	for i := 0; ; i++ {
		if (i % 500) == 0 {
			fmt.Printf("Num items inserted: %d\n", i)
			kv.DebugPrintMore()
		}
		for j := 0; j < len(entries); j++ {
			entries[j].Key = []byte(fmt.Sprintf("%16x", rand.Int63()))
		}
		kv.Write(ctx, entries)
	}
}
