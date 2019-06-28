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

package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/dgraph-io/badger/badger/cmd"
)

func main() {
	go func() {
		for i := 8080; i < 9080; i++ {
			fmt.Printf("Listening for /debug HTTP requests at port: %d\n", i)
			if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", i), nil); err != nil {
				fmt.Println("Port busy. Trying another one...")
				continue

			}
		}
	}()
	runtime.SetBlockProfileRate(100)
	runtime.GOMAXPROCS(128)
	cmd.Execute()
}
