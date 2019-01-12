/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package cmd

import (
	"crypto/rand"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
	"github.com/spf13/cobra"
)

var fillCmd = &cobra.Command{
	Use:   "fill",
	Short: "Fill Badger with random data.",
	Long: `
This command would fill Badger with random data. Useful for testing and performance analysis.
`,
	RunE: fill,
}

var keySz, valSz int
var numKeys float64
var force bool

const mil float64 = 1e6

func init() {
	RootCmd.AddCommand(fillCmd)
	fillCmd.Flags().IntVarP(&keySz, "key-size", "k", 32, "Size of key")
	fillCmd.Flags().IntVarP(&valSz, "val-size", "v", 128, "Size of value")
	fillCmd.Flags().Float64VarP(&numKeys, "keys-mil", "m", 10.0,
		"Number of keys to add in millions")
	fillCmd.Flags().BoolVarP(&force, "force-compact", "f", true, "Force compact level 0 on close.")
}

func fill(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.Truncate = truncate
	opts.SyncWrites = false
	opts.CompactL0OnClose = force
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer func() {
		start := time.Now()
		err := db.Close()
		badger.Infof("DB.Close. Error: %v. Time taken: %s", err, time.Since(start))
	}()

	start := time.Now()
	batch := db.NewWriteBatch()
	num := int64(numKeys * mil)
	for i := int64(1); i <= num; i++ {
		k := make([]byte, keySz)
		v := make([]byte, valSz)
		y.Check2(rand.Read(k))
		y.Check2(rand.Read(v))
		if err := batch.Set(k, v, 0); err != nil {
			return err
		}
		if i%1e5 == 0 {
			badger.Infof("Written keys: %d\n", i)
		}
	}
	if err := batch.Flush(); err != nil {
		return err
	}
	badger.Infof("%d keys written. Time taken: %s\n", num, time.Since(start))
	return nil
}
