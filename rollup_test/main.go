package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

var atomicTs int64

func adder(db *badger.DB) {

	key := "test"

	for i := int32(0); i < 20; i++ {
		time.Sleep(10 * time.Millisecond)

		var curr delta
		// if i < 10 {
		curr.set_op = true
		curr.del_op = false
		curr.consolidated = false
		curr.last_version = 0
		curr.values = append(curr.values, i)
		// } else {
		// 	curr.set_op = false
		// 	curr.del_op = true
		// 	curr.consolidated = false
		// 	curr.last_version = 0
		// 	curr.values = append(curr.values, i, i-10)
		// }

		ts := atomic.LoadInt64(&atomicTs)
		fmt.Printf("Adding delta: %d. At time: %d\n", i, ts+1)

		txn := db.NewTransactionAt(uint64(ts), true)

		currBytes, _ := structToBytes(curr)
		txn.Set([]byte(key), currBytes)

		var wg sync.WaitGroup
		wg.Add(1)

		txn.CommitAt(uint64(ts+1), func(err error) {
			atomic.AddInt64(&atomicTs, 2)
			wg.Done()
		})

		wg.Wait()

	}
}

func getter(db *badger.DB) {

	for i := 0; i < 10; i++ {
		time.Sleep(200 * time.Millisecond)
		fmt.Printf("Getting value at time: %d\n", time.Now().UnixMilli())

		_ = db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte("keylatestmerge_")

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				k := item.Key()
				keyname := k[len("keylatestmerge_"):]

				var list delta
				err := item.Value(func(val []byte) error {
					var err error
					list, err = bytesToStruct(val)
					return err
				})
				if err != nil {
					return err
				}

				fmt.Printf("Key: %s...\n", keyname)
				for _, nums := range list.values {
					fmt.Printf("%d, ", nums)
				}
				fmt.Println("")
			}
			return nil
		})
	}
}

func merger(db *badger.DB) {
	// skipCounter := 0
	time.Sleep(2 * time.Millisecond)
	for i := 0; i < 20; i++ {
		time.Sleep(20 * time.Millisecond)

		ts := atomic.LoadInt64(&atomicTs)
		if ts < 2 {
			continue
		}
		fmt.Printf("Merging deltas till now at time: %d\n", ts)

		txn := db.NewTransactionAt(uint64(ts), false)

		opt := badger.DefaultIteratorOptions
		// opt.Reverse = true

		it := txn.NewKeyIterator([]byte("test"), opt)
		defer it.Close()

		// earliestTimestamp := 0
		var consolidated delta

		consolidated.consolidated = true
		consolidated.set_op = false
		consolidated.del_op = false
		consolidated.last_version = 0

		var allDeltas []delta
		latestVersion := uint64(0)

		for it.Seek([]byte("test")); it.Valid(); it.Next() {
			item := it.Item()
			version := item.Version()

			if version > latestVersion {
				latestVersion = version
			}

			// if version <= uint64(earliestTimestamp) {
			// 	break
			// }

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			itemValue, _ := bytesToStruct(valCopy)

			// fmt.Println("Delta item value: ", itemValue, )
			fmt.Printf("%d: ", version)
			for _, num := range itemValue.values {
				fmt.Printf("%d, ", num)
			}
			fmt.Println("")

			if itemValue.consolidated == true {
				// earliestTimestamp = int(itemValue.last_version)
				consolidated = itemValue
				break
			} else {
				allDeltas = append(allDeltas, itemValue)
			}
		}

		for i := len(allDeltas) - 1; i >= 0; i-- {
			consolidated = mergeDelta(consolidated, allDeltas[i])
		}
		consolidated.last_version = int32(latestVersion)
		fmt.Println(consolidated)
		consolidatedBytes, _ := structToBytes(consolidated)
		it.Close()

		txn.Discard()

		var wg sync.WaitGroup
		wg.Add(1)

		txn = db.NewTransactionAt(uint64(ts), true)
		defer txn.Discard()
		txn.Set([]byte("test"), consolidatedBytes)
		txn.CommitAt(uint64(latestVersion), func(err error) {
			if err == nil {
				wg.Done()
			} else {
				log.Fatal(err)
			}
		})

		wg.Wait()

		fmt.Printf("Committed rollup %d at %d\n", ts, latestVersion)
	}
}

func main() {
	db, err := badger.OpenManaged(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	atomicTs = 1
	go adder(db)
	merger(db)

	// getter(db)
}
