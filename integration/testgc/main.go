package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
)

var Max int64 = 10000000
var suffix = make([]byte, 128)

type S struct {
	sync.Mutex
	vals map[uint64]uint64

	count uint64 // Not under mutex lock.
}

func encoded(i uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, i)
	return out
}

func (s *S) write(db *badger.DB) error {
	return db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			// These keys would be overwritten.
			keyi := uint64(rand.Int63n(Max))
			key := encoded(keyi)
			vali := atomic.AddUint64(&s.count, 1)
			val := encoded(vali)
			val = append(val, suffix...)
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		for i := 0; i < 20; i++ {
			// These keys would be new and never overwritten.
			keyi := atomic.AddUint64(&s.count, 1)
			if keyi%1000000 == 0 {
				log.Printf("Count: %d\n", keyi)
			}
			key := encoded(keyi)
			val := append(key, suffix...)
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *S) read(db *badger.DB) error {
	max := int64(atomic.LoadUint64(&s.count))
	keyi := uint64(rand.Int63n(max))
	key := encoded(keyi)

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		y.AssertTruef(len(val) == len(suffix)+8, "Found val of len: %d\n", len(val))
		vali := binary.BigEndian.Uint64(val[0:8])
		s.Lock()
		expected := s.vals[keyi]
		if vali < expected {
			log.Fatalf("Expected: %d. Found: %d. Key: %d\n", expected, vali, keyi)
		} else if vali == expected {
			// pass
		} else {
			s.vals[keyi] = vali
		}
		s.Unlock()
		return nil
	})
	if err == badger.ErrKeyNotFound {
		return nil
	}
	return err
}

func main() {
	fmt.Println("Badger Integration test for value log GC.")

	// dir, err := ioutil.TempDir("./", "badger")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	dir := "/mnt/drive/badgertest"
	os.RemoveAll(dir)
	// defer os.RemoveAll(dir)

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.FileIO
	// opts.ValueLogFileSize = 64 << 20 // 64 MB.
	opts.SyncWrites = false

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go http.ListenAndServe("localhost:8080", nil)

	closer := y.NewCloser(11)
	go func() {
		// Run value log GC.
		defer closer.Done()
		var count int
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
		again:
			select {
			case <-closer.HasBeenClosed():
				log.Printf("Num times value log GC was successful: %d\n", count)
				return
			default:
			}
			log.Printf("Starting a value log GC")
			err := db.RunValueLogGC(0.1)
			log.Printf("Result of value log GC: %v\n", err)
			if err == nil {
				count++
				goto again
			}
		}
	}()

	s := S{
		count: uint64(Max),
		vals:  make(map[uint64]uint64),
	}
	var numLoops uint64
	ticker := time.NewTicker(5 * time.Second)
	for i := 0; i < 10; i++ {
		go func() {
			defer closer.Done()
			for {
				if err := s.write(db); err != nil {
					log.Fatal(err)
				}
				for j := 0; j < 10; j++ {
					if err := s.read(db); err != nil {
						log.Fatal(err)
					}
				}
				nl := atomic.AddUint64(&numLoops, 1)
				select {
				case <-closer.HasBeenClosed():
					return
				case <-ticker.C:
					log.Printf("Num loops: %d\n", nl)
				default:
				}
			}
		}()
	}
	time.Sleep(5 * time.Minute)
	log.Println("Signaling...")
	closer.SignalAndWait()
	log.Println("Wait done. Now iterating over everything.")

	err = db.View(func(txn *badger.Txn) error {
		iopts := badger.DefaultIteratorOptions
		itr := txn.NewIterator(iopts)
		defer itr.Close()

		var total, tested int
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			key := item.Key()
			keyi := binary.BigEndian.Uint64(key)
			total++

			val, err := item.Value()
			if err != nil {
				return err
			}
			if len(val) < 8 {
				log.Printf("Unexpected value: %x\n", val)
				continue
			}
			vali := binary.BigEndian.Uint64(val[0:8])

			expected, ok := s.vals[keyi] // Not all keys must be in vals map.
			if ok {
				tested++
				if vali < expected {
					// vali must be equal or greater than what's in the map.
					log.Fatalf("Expected: %d. Got: %d. Key: %d\n", expected, vali, keyi)
				}
			}
		}
		log.Printf("Total iterated: %d. Tested values: %d\n", total, tested)
		return nil
	})
	if err != nil {
		log.Fatalf("Error while iterating: %v", err)
	}
	log.Println("Iteration done. Test successful.")
	time.Sleep(time.Minute) // Time to do some poking around.
}
