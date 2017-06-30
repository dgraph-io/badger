/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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

package badger_test

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
)

var d string = "doc"

func Example() {
	opt := badger.DefaultOptions
	opt.Dir = os.TempDir()
	opt.ValueDir = opt.Dir
	kv, _ := badger.NewKV(&opt)

	key := []byte("hello")

	kv.Set(key, []byte("world"))
	fmt.Printf("SET %s world\n", key)

	var item badger.KVItem
	if err := kv.Get(key, &item); err != nil {
		fmt.Printf("Error while getting key: %q", key)
	}
	fmt.Printf("GET %s %s\n", key, item.Value())

	if err := kv.CompareAndSet(key, []byte("venus"), 100); err != nil {
		fmt.Println("CAS counter mismatch")
	} else {
		if err = kv.Get(key, &item); err != nil {
			fmt.Printf("Error while getting key: %q", key)
		}
		fmt.Printf("Set to %s\n", item.Value())
	}
	if err := kv.CompareAndSet(key, []byte("mars"), item.Counter()); err == nil {
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
// 		Reverse:      false,
// 	}
// 	itr := kv.NewIterator(itrOpt)
// 	for itr.Rewind(); itr.Valid(); itr.Next() {
// 		item := itr.Item()
// 		item.Key()
// 		item.Value()
// 	}
// }
