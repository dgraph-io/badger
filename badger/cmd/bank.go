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
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/y"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "bank",
	Short: "Run bank test on Badger.",
	Long: `
This command runs bank test on Badger, inspired by Jepsen. It creates many
accounts and moves money among them transactionally. It also reads the sum total
of all the accounts, to ensure that the total never changes.
`,
}

var bankTest = &cobra.Command{
	Use:   "test",
	Short: "Execute bank test on Badger.",
	RunE:  runTest,
}

var bankDisect = &cobra.Command{
	Use:   "disect",
	Short: "Disect the bank output.",
	Long: `
Disect the bank output BadgerDB to find the first transaction which causes
failure of the total invariant.
`,
	RunE: runDisect,
}

var numGoroutines, numAccounts, numPrevious int
var duration string
var stopAll int32

const keyPrefix = "account:"

const initialBal uint64 = 100

func init() {
	RootCmd.AddCommand(testCmd)
	testCmd.AddCommand(bankTest)
	testCmd.AddCommand(bankDisect)

	testCmd.Flags().IntVarP(
		&numAccounts, "accounts", "a", 10000, "Number of accounts in the bank.")
	bankTest.Flags().IntVarP(
		&numGoroutines, "conc", "c", 16, "Number of concurrent transactions to run.")
	bankTest.Flags().StringVarP(&duration, "duration", "d", "3m", "How long to run the test.")
	bankDisect.Flags().IntVarP(&numPrevious, "previous", "p", 12,
		"Starting from the violation txn, how many previous versions to retrieve.")
}

func key(account int) []byte {
	return []byte(fmt.Sprintf("%s%s", keyPrefix, strconv.Itoa(account)))
}

func toAccount(key []byte) int {
	i, err := strconv.Atoi(string(key[len(keyPrefix):]))
	y.Check(err)
	return i
}

func toUint64(val []byte) uint64 {
	u, err := strconv.ParseUint(string(val), 10, 64)
	y.Check(err)
	return uint64(u)
}

func toSlice(bal uint64) []byte {
	return []byte(strconv.FormatUint(bal, 10))
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

// // iterateTotal retrives the total of all accounts by iterating over the DB.
// func iterateTotal(db *badger.DB) ([]account, error) {
// 	expected := uint64(numAccounts) * uint64(initialBal)
// 	var accounts []account
// 	err := db.View(func(txn *badger.Txn) error {
// 		// start := time.Now()
// 		itr := txn.NewIterator(badger.DefaultIteratorOptions)
// 		defer itr.Close()

// 		var total uint64
// 		for itr.Seek([]byte(keyPrefix)); itr.ValidForPrefix([]byte(keyPrefix)); itr.Next() {
// 			item := itr.Item()
// 			val, err := item.ValueCopy(nil)
// 			if err != nil {
// 				return err
// 			}
// 			acc := account{
// 				Id:  toAccount(item.Key()),
// 				Bal: toUint64(val),
// 			}
// 			accounts = append(accounts, acc)
// 			total += acc.Bal
// 		}
// 		if total != expected {
// 			log.Printf("Balance did NOT match up. Expected: %d. Received: %d",
// 				expected, total)
// 			atomic.AddInt32(&stopAll, 1)
// 			return errFailure
// 		}
// 		// log.Printf("totalMoney took: %s\n", time.Since(start).String())
// 		return nil
// 	})
// 	return accounts, err
// }

// seekTotal retrives the total of all accounts by seeking for each account key.
func seekTotal(txn *badger.Txn) ([]account, error) {
	expected := uint64(numAccounts) * uint64(initialBal)
	var accounts []account

	var total uint64
	for i := 0; i < numAccounts; i++ {
		item, err := txn.Get(key(i))
		if err != nil {
			log.Printf("Error for account: %d. err=%v. key=%q\n", i, err, key(i))
			return accounts, err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return accounts, err
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
		atomic.AddInt32(&stopAll, 1)
		return accounts, errFailure
	}
	return accounts, nil
}

// Range is [lowTs, highTs).
func findFirstInvalidTxn(db *badger.DB, lowTs, highTs uint64) uint64 {
	checkAt := func(ts uint64) error {
		txn := db.NewTransactionAt(ts, false)
		_, err := seekTotal(txn)
		txn.Discard()
		return err
	}

	if highTs-lowTs < 1 {
		log.Printf("Checking at lowTs: %d\n", lowTs)
		err := checkAt(lowTs)
		if err == errFailure {
			fmt.Printf("Violation at ts: %d\n", lowTs)
			return lowTs
		} else if err != nil {
			log.Printf("Error at lowTs: %d. Err=%v\n", lowTs, err)
			return 0
		}
		fmt.Printf("No violation found at ts: %d\n", lowTs)
		return 0
	}

	midTs := (lowTs + highTs) / 2
	log.Println()
	log.Printf("Checking. low=%d. high=%d. mid=%d\n", lowTs, highTs, midTs)
	err := checkAt(midTs)
	if err == badger.ErrKeyNotFound || err == nil {
		// If no failure, move to higher ts.
		return findFirstInvalidTxn(db, midTs+1, highTs)
	} else {
		// Found an error.
		return findFirstInvalidTxn(db, lowTs, midTs)
	}
}

func compareTwo(db *badger.DB, before, after uint64) {
	fmt.Printf("Comparing @ts=%d with @ts=%d\n", before, after)
	txn := db.NewTransactionAt(before, false)
	prev, err := seekTotal(txn)
	if err == errFailure {
		// pass
	} else {
		y.Check(err)
	}
	txn.Discard()

	txn = db.NewTransactionAt(after, false)
	now, err := seekTotal(txn)
	if err == errFailure {
		// pass
	} else {
		y.Check(err)
	}
	txn.Discard()

	fmt.Println(diff(prev, now))
}

func runDisect(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.ReadOnly = true

	// The total did not match up. So, let's disect the DB to find the
	// transction which caused the total mismatch.
	db, err := badger.OpenManaged(opts)
	if err != nil {
		return err
	}
	fmt.Println("opened db")

	var min, max uint64 = math.MaxUint64, 0
	{
		txn := db.NewTransactionAt(uint64(math.MaxUint32), false)
		iopt := badger.DefaultIteratorOptions
		iopt.AllVersions = true
		itr := txn.NewIterator(iopt)
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			if min > item.Version() {
				min = item.Version()
			}
			if max < item.Version() {
				max = item.Version()
			}
		}
		itr.Close()
		txn.Discard()
	}

	log.Printf("min=%d. max=%d\n", min, max)
	ts := findFirstInvalidTxn(db, min, max)
	fmt.Println()
	if ts == 0 {
		fmt.Println("Nothing found. Exiting.")
		return nil
	}

	for i := 0; i < numPrevious; i++ {
		compareTwo(db, ts-1-uint64(i), ts-uint64(i))
	}
	return nil
}

func runTest(cmd *cobra.Command, args []string) error {
	rand.Seed(time.Now().UnixNano())

	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.MaxTableSize = 4 << 20 // Force more compactions.
	opts.NumLevelZeroTables = 2
	opts.NumMemtables = 2
	// Do not GC any versions, because we need them for the disect.
	opts.NumVersionsToKeep = int(math.MaxInt32)
	// opts.ValueThreshold = 1 // Make all values go to value log.

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

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

	y.Check(db.View(func(txn *badger.Txn) error {
		log.Printf("LowTs: %d\n", txn.ReadTs())
		return nil
	}))
	log.Println("Bank initialization OK. Commencing test.")
	log.Printf("Running with %d accounts, and %d goroutines.\n", numAccounts, numGoroutines)
	log.Printf("Using keyPrefix: %s\n", keyPrefix)

	dur, err := time.ParseDuration(duration)
	y.Check(err)

	// startTs := time.Now()
	endTs := time.Now().Add(dur)
	var total, errors, reads uint64

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if atomic.LoadInt32(&stopAll) > 0 {
				// Do not proceed.
				return
			}
			// log.Printf("[%6s] Total: %d. Errors: %d Reads: %d.\n",
			// 	time.Since(startTs).Round(time.Second).String(),
			// 	atomic.LoadUint64(&total),
			// 	atomic.LoadUint64(&errors),
			// 	atomic.LoadUint64(&reads))
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
				if atomic.LoadInt32(&stopAll) > 0 {
					// Do not proceed.
					return
				}
				if time.Now().After(endTs) {
					return
				}
				from := rand.Intn(numAccounts)
				to := rand.Intn(numAccounts)
				if from == to {
					continue
				}
				err := moveMoney(db, from, to)
				atomic.AddUint64(&total, 1)
				if err == nil {
					log.Printf("Moved $5. %d -> %d\n", from, to)
				} else {
					atomic.AddUint64(&errors, 1)
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

		for range ticker.C {
			if atomic.LoadInt32(&stopAll) > 0 {
				// Do not proceed.
				return
			}
			if time.Now().After(endTs) {
				return
			}

			y.Check(db.View(func(txn *badger.Txn) error {
				_, err := seekTotal(txn)
				if err != nil {
					log.Printf("Error while calculating total: %v", err)
				} else {
					atomic.AddUint64(&reads, 1)
				}
				return nil
			}))
		}
	}()
	wg.Wait()

	if atomic.LoadInt32(&stopAll) == 0 {
		log.Println("Test OK")
	} else {
		log.Println("Test FAILED")
	}
	return nil
}
