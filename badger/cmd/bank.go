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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/y"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run bank test on Badger.",
	Long: `
This command runs bank test on Badger, inspired by Jepsen. It creates many
accounts and moves money among them transactionally. It also reads the sum total
of all the accounts, to ensure that the total never changes.
`,
	RunE: runTest,
}

var numGoroutines, numAccounts int
var duration, keyPrefix string
var blockAll int32

const initialBal uint64 = 100

func init() {
	RootCmd.AddCommand(testCmd)
	testCmd.Flags().IntVarP(&numGoroutines, "conc", "c", 10, "Number of concurrent transactions to run.")
	testCmd.Flags().IntVarP(&numAccounts, "accounts", "a", 1000000, "Number of accounts in the bank.")
	testCmd.Flags().StringVarP(&duration, "duration", "d", "3m", "How long to run the test.")
}

func key(account int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(account))
	return append([]byte(keyPrefix), b[:]...)
}

func toAccount(key []byte) int {
	return int(binary.BigEndian.Uint32(key[len(keyPrefix):]))
}

func toUint64(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}

func toSlice(bal uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bal)
	return b
}

func getBalance(txn *badger.Txn, account int) (uint64, error) {
	item, err := txn.Get(key(account))
	if err != nil {
		return 0, err
	}

	var bal uint64
	err = item.Value(func(v []byte) {
		bal = toUint64(v)
	})
	return bal, err
	// val, err := item.Value()
	// if err != nil {
	// 	return 0, err
	// }
	// return toUint64(val), nil
}

func putBalance(txn *badger.Txn, account int, bal uint64) error {
	return txn.Set(key(account), toSlice(bal))
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

var errAbandoned = errors.New("Transaction abandonded due to insufficient balance")

func moveMoney(db *badger.DB, from, to int) error {
	return db.Update(func(txn *badger.Txn) error {
		balf, err := getBalance(txn, from)
		if err != nil {
			return err
		}
		balt, err := getBalance(txn, to)
		if err != nil {
			return err
		}

		floor := min(balf, balt)
		if floor < 5 {
			return errAbandoned
		}
		// Move the money.
		balf -= 5
		balt += 5

		if err = putBalance(txn, from, balf); err != nil {
			return err
		}
		if err = putBalance(txn, to, balt); err != nil {
			return err
		}
		return nil
	})
}

type account struct {
	Id  int
	Bal uint64
}

func diff(a, b []account) string {
	var buf bytes.Buffer
	y.AssertTruef(len(a) == len(b), "len(a)=%d. len(b)=%d\n", len(a), len(b))
	for i := range a {
		ai := a[i]
		bi := b[i]
		if ai.Id != bi.Id || ai.Bal != bi.Bal {
			buf.WriteString(fmt.Sprintf("Index: %d. Account [%+v] -> [%+v]\n", i, ai, bi))
		}
	}
	return buf.String()
}

var errFailure = errors.New("Found an balance mismatch. Test failed.")

// iterateTotal retrives the total of all accounts by iterating over the DB.
func iterateTotal(db *badger.DB) ([]account, error) {
	expected := uint64(numAccounts) * uint64(initialBal)
	var accounts []account
	err := db.View(func(txn *badger.Txn) error {
		// start := time.Now()
		itr := txn.NewIterator(badger.DefaultIteratorOptions)
		defer itr.Close()

		var total uint64
		for itr.Seek([]byte(keyPrefix)); itr.ValidForPrefix([]byte(keyPrefix)); itr.Next() {
			item := itr.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			acc := account{
				Id:  toAccount(item.Key()),
				Bal: toUint64(val),
			}
			accounts = append(accounts, acc)
			total += acc.Bal
		}
		if total != expected {
			log.Printf("Balance did NOT match up. Expected: %d. Received: %d",
				expected, total)
			atomic.AddInt32(&blockAll, 1)
			return errFailure
		}
		// log.Printf("totalMoney took: %s\n", time.Since(start).String())
		return nil
	})
	return accounts, err
}

// seekTotal retrives the total of all accounts by seeking for each account key.
func seekTotal(db *badger.DB) ([]account, error) {
	expected := uint64(numAccounts) * uint64(initialBal)
	var accounts []account
	err := db.View(func(txn *badger.Txn) error {
		var total uint64
		for i := 0; i < numAccounts; i++ {
			item, err := txn.Get(key(i))
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			acc := account{
				Id:  i,
				Bal: toUint64(val),
			}
			accounts = append(accounts, acc)
			total += acc.Bal
		}
		if total != expected {
			log.Printf("Balance did NOT match up. Expected: %d. Received: %d",
				expected, total)
			atomic.AddInt32(&blockAll, 1)
			return errFailure
		}
		return nil
	})
	return accounts, err
}

func runTest(cmd *cobra.Command, args []string) error {
	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.ValueThreshold = 1 // Make all values go to value log.

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	keyPrefix = fmt.Sprintf("a%s:", time.Now().Format(time.RFC3339Nano))

	var wg sync.WaitGroup
	var txns []*badger.Txn
	for i := 0; i < numAccounts; i++ {
		wg.Add(1)
		txn := db.NewTransaction(true)
		y.Check(putBalance(txn, i, initialBal))
		txns = append(txns, txn)
	}
	for _, txn := range txns {
		y.Check(txn.Commit(func(err error) {
			y.Check(err)
			wg.Done()
		}))
	}
	log.Println("Waiting for writes to be done")
	wg.Wait()
	log.Println("Bank initialization OK. Commencing test.")
	log.Printf("Running with %d accounts, and %d goroutines.\n", numAccounts, numGoroutines)
	log.Printf("Using keyPrefix: %s\n", keyPrefix)

	dur, err := time.ParseDuration(duration)
	y.Check(err)

	endTs := time.Now().Add(dur)
	var total, success, errors, reads uint64

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if atomic.LoadInt32(&blockAll) > 0 {
				// Do not proceed.
				continue
			}
			log.Printf("Total: %d. Success: %d. Errors: %d Reads: %d.\n",
				atomic.LoadUint64(&total),
				atomic.LoadUint64(&success),
				atomic.LoadUint64(&errors),
				atomic.LoadUint64(&reads))
			if time.Now().After(endTs) {
				return
			}
		}
	}()

	// RW goroutines.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ticker := time.NewTicker(10 * time.Microsecond)
			defer ticker.Stop()

			for range ticker.C {
				if atomic.LoadInt32(&blockAll) > 0 {
					// Do not proceed.
					continue
				}
				if time.Now().After(endTs) {
					return
				}
				err := moveMoney(db, rand.Intn(numAccounts), rand.Intn(numAccounts))
				atomic.AddUint64(&total, 1)
				if err != nil {
					atomic.AddUint64(&errors, 1)
				} else {
					atomic.AddUint64(&success, 1)
				}
			}
		}()
	}

	// RO goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(10 * time.Microsecond)
		defer ticker.Stop()

		var lastOk []account
		for range ticker.C {
			if atomic.LoadInt32(&blockAll) > 0 {
				// Do not proceed.
				continue
			}
			if time.Now().After(endTs) {
				return
			}
			current, err := seekTotal(db)
			if err == errFailure {
				log.Printf("FAILURE. Diff: %s", diff(lastOk, current))

			} else if err != nil {
				log.Printf("Error while calculating total: %v", err)
			} else {
				lastOk = current
				atomic.AddUint64(&reads, 1)
			}
		}
	}()
	wg.Wait()

	_, err = seekTotal(db)
	y.Check(err) // One last time.
	log.Println("Test OK")
	return nil
}
