package badger_test

import (
	"fmt"

	"github.com/dgraph-io/badger/badger"
)

var d string = "doc"

func Example() {
	opt := badger.DefaultOptions
	opt.Dir = "/tmp"
	kv := badger.NewKV(&opt)

	key := []byte("hello")

	kv.Set(key, []byte("world"))
	fmt.Printf("SET %s world\n", key)

	val, cas := kv.Get(key)
	fmt.Printf("GET %s %s\n", key, val)

	if err := kv.CompareAndSet(key, []byte("venus"), 100); err != nil {
		fmt.Println("CAS counter mismatch")
	} else {
		val, _ = kv.Get(key)
		fmt.Printf("Set to %s\n", val)
	}
	if err := kv.CompareAndSet(key, []byte("mars"), cas); err == nil {
		fmt.Println("Set to mars")
	} else {
		fmt.Printf("Unsuccessful write. Got error: %v\n", err)
	}

	// Output:
	// SET hello world
	// GET hello world
	// CAS counter mismatch
	// Set to mars
}

// func ExampleNewIterator() {
// 	opt := DefaultOptions
// 	opt.Dir = "/tmp/badger"
// 	kv := NewKV(&opt)

// 	itrOpt := IteratorOptions{
// 		PrefetchSize: 1000,
// 		FetchValues:  true,
// 		Reversed:     false,
// 	}
// 	itr := kv.NewIterator(itrOpt)
// 	for itr.Rewind(); itr.Valid(); itr.Next() {
// 		item := itr.Item()
// 		item.Key()
// 		item.Value()
// 	}
// }
