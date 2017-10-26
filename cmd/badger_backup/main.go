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

/*
badger_backup

Usage: badger_backup --dir d [--value-dir v] [--backup-file b]

This command makes a version-independent backup of the Badger database in a
single file. It dumps out all the key-value pairs (including older versions)
into a binary file. Data is encoded in protocol buffer format.
*/
package main

import "flag"

import "log"
import "os"
import "github.com/dgraph-io/badger"
import "github.com/dgraph-io/badger/y"

func main() {
	// Parse flags
	dirFlag := flag.String("dir", "", "Directory where the LSM tree should be stored.")
	valueDirFlag := flag.String("value-dir", "",
		"Directory where the value log should be stored, if different from --dir.")
	backupFileFlag := flag.String("backup-file", "badger.bak",
		"Backup file to store data in.")
	flag.Parse()
	if *dirFlag == "" {
		log.Fatal("--dir is required.")
	}

	if *valueDirFlag == "" {
		*valueDirFlag = *dirFlag
	}

	// Open DB
	opt := badger.DefaultOptions
	opt.Dir = *dirFlag
	opt.ValueDir = *valueDirFlag
	kv, err := badger.NewKV(&opt)
	defer kv.Close()
	y.Check(err)

	// Open backup file for writing
	f, err := os.Create(*backupFileFlag)
	y.Check(err)
	defer f.Close()

	// Perform backup
	err = kv.Backup(f)
	y.Check(err)
}
