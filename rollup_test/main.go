package main

import (
	"fmt"
	"log"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func adder(db *badger.DB) {

	key := "test"

	for i := int32(0); i < 10; i++ {
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("Adding delta: %d. At time: %d\n", i, time.Now().UnixMilli())

		var curr delta
		curr.set_op = true
		curr.del_op = false
		curr.values = append(curr.values, i, 10+i)

		keyF := fmt.Sprintf("key_%s_ts_", key)
		keyF2 := fmt.Sprintf("keylatestadd_%s", key)

		err := db.Update(func(txn *badger.Txn) error {
			currBytes, _ := structToBytes(curr)
			timestamp := time.Now().UnixMilli()
			txn.Set(addTimestampToStringBytes(keyF, timestamp), currBytes)
			txn.Set([]byte(keyF2), addTimestampToStringBytes("", timestamp))
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	for i := int32(0); i < 10; i++ {
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("Adding delta: %d. At time: %d\n", i, time.Now().UnixMilli())

		var curr delta
		curr.set_op = false
		curr.del_op = true
		curr.values = append(curr.values, i, 10+i)

		keyF := fmt.Sprintf("key_%s_ts_", key)
		keyF2 := fmt.Sprintf("keylatestadd_%s", key)

		err := db.Update(func(txn *badger.Txn) error {
			currBytes, _ := structToBytes(curr)
			timestamp := time.Now().UnixMilli()
			txn.Set(addTimestampToStringBytes(keyF, timestamp), currBytes)
			txn.Set([]byte(keyF2), addTimestampToStringBytes("", timestamp))
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}
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
	skipCounter := 0

	for i := 0; i < 15; i++ {
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Merging deltas till now at time: %d\n", time.Now().UnixMilli())

		_ = db.Update(func(txn *badger.Txn) error {

			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte("keylatestadd_")

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				k := item.Key()
				keyname := k[len("keylatestadd_"):]
				var addTimestamp, procTimestamp int64
				var addTimestampByte []byte
				err := item.Value(func(v []byte) error {
					_, addTimestamp = byteToKeyAndTimestamp(v)
					addTimestampByte = append([]byte{}, v...)
					return nil
				})
				if err != nil {
					return err
				}

				procItem, err := txn.Get([]byte(fmt.Sprintf("keylatestproc_%s", keyname)))

				if err == badger.ErrKeyNotFound {
					procTimestamp = 0
				} else {
					procTsByte, _ := procItem.ValueCopy(nil)
					_, procTimestamp = byteToKeyAndTimestamp(procTsByte)
				}

				fmt.Printf("Key: %s, addTimestamp: %d, procTimestamp: %d\n", keyname, addTimestamp, procTimestamp)
				if procTimestamp >= addTimestamp {
					continue
				}

				// start new iterator to merge all deltas after a specific timestamp
				it2 := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it2.Close()
				prefix2 := []byte(fmt.Sprintf("key_%s_ts_", keyname))
				preprefix := []byte(fmt.Sprintf("key_%s_ts_", keyname))
				var existing delta
				if procTimestamp == 0 {
					existing.del_op = false
					existing.set_op = false
				} else {
					// prefix2 = addTimestampToStringBytes(string(prefix2), procTimestamp)
					existing = getList(fmt.Sprintf("keylatestmerge_%s", keyname), db)
				}

				for it2.Seek(prefix2); it2.ValidForPrefix(preprefix); it2.Next() {
					item2 := it2.Item()

					keyForTS := item2.Key()
					_, deltaTS := byteToKeyAndTimestamp(keyForTS)

					if deltaTS <= procTimestamp {
						skipCounter += 1
						fmt.Printf("Skip Counter: %d\n", skipCounter)
						continue
					}

					var updDelta delta
					var err2 error
					err := item2.Value(func(v []byte) error {
						updDelta, err2 = bytesToStruct(v)
						return err2
					})
					if err != nil {
						return err
					}

					existing = mergeDelta(existing, updDelta)
				}

				mergedByte, _ := structToBytes(existing)

				for _, nums := range existing.values {
					fmt.Printf("%d ", nums)
				}
				fmt.Println("")

				txn.Set([]byte(fmt.Sprintf("keylatestmerge_%s", keyname)), mergedByte)
				txn.Set([]byte(fmt.Sprintf("keylatestproc_%s", keyname)), addTimestampByte)
				// txn.Commit()
			}

			return nil
		})

	}
}

func main() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger_threaded21"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go adder(db)
	go merger(db)
	getter(db)
}
