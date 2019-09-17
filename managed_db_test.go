package badger

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func val(large bool) []byte {
	var buf []byte
	if large {
		buf = make([]byte, 8192)
	} else {
		buf = make([]byte, 16)
	}
	rand.Read(buf)
	return buf
}

func numKeys(db *DB) int {
	var count int
	err := db.View(func(txn *Txn) error {
		itr := txn.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {
			count++
		}
		return nil
	})
	y.Check(err)
	return count
}

func numKeysManaged(db *DB, readTs uint64) int {
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()

	itr := txn.NewIterator(DefaultIteratorOptions)
	defer itr.Close()

	var count int
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	return count
}

func TestDropAllManaged(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.managedTxns = true
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)

	N := uint64(10000)
	populate := func(db *DB, start uint64) {
		var wg sync.WaitGroup
		for i := start; i < start+N; i++ {
			wg.Add(1)
			txn := db.NewTransactionAt(math.MaxUint64, true)
			require.NoError(t, txn.SetEntry(NewEntry([]byte(key("key", int(i))), val(true))))
			require.NoError(t, txn.CommitAt(uint64(i), func(err error) {
				require.NoError(t, err)
				wg.Done()
			}))
		}
		wg.Wait()
	}

	populate(db, N)
	require.Equal(t, int(N), numKeysManaged(db, math.MaxUint64))

	require.NoError(t, db.DropAll())
	require.NoError(t, db.DropAll()) // Just call it twice, for fun.
	require.Equal(t, 0, numKeysManaged(db, math.MaxUint64))

	// Check that we can still write to mdb, and using lower timestamps.
	populate(db, 1)
	require.Equal(t, int(N), numKeysManaged(db, math.MaxUint64))
	require.NoError(t, db.Close())

	// Ensure that value log is correctly replayed, that we are preserving badgerHead.
	opts.managedTxns = true
	db2, err := Open(opts)
	require.NoError(t, err)
	require.Equal(t, int(N), numKeysManaged(db2, math.MaxUint64))
	require.NoError(t, db2.Close())
}

func TestDropAll(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)

	N := uint64(10000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))

	require.NoError(t, db.DropAll())
	require.Equal(t, 0, numKeys(db))

	// Check that we can still write to mdb, and using lower timestamps.
	populate(db)
	require.Equal(t, int(N), numKeys(db))
	require.NoError(t, db.Close())

	// Ensure that value log is correctly replayed.
	db2, err := Open(opts)
	require.NoError(t, err)
	require.Equal(t, int(N), numKeys(db2))
	require.NoError(t, db2.Close())
}

func TestDropAllTwice(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	N := uint64(10000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))

	require.NoError(t, db.DropAll())
	require.Equal(t, 0, numKeys(db))

	// Call DropAll again.
	require.NoError(t, db.DropAll())
}

func TestDropAllWithPendingTxn(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	N := uint64(10000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))

	txn := db.NewTransaction(true)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		itr := txn.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		var keys []string
		for {
			var count int
			for itr.Rewind(); itr.Valid(); itr.Next() {
				count++
				item := itr.Item()
				keys = append(keys, string(item.KeyCopy(nil)))
				_, err := item.ValueCopy(nil)
				if err != nil {
					t.Logf("Got error during value copy: %v", err)
					return
				}
			}
			t.Logf("Got number of keys: %d\n", count)
			for _, key := range keys {
				item, err := txn.Get([]byte(key))
				if err != nil {
					t.Logf("Got error during key lookup: %v", err)
					return
				}
				if _, err := item.ValueCopy(nil); err != nil {
					t.Logf("Got error during second value copy: %v", err)
					return
				}
			}
		}
	}()
	// Do not cancel txn.

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		require.NoError(t, db.DropAll())
	}()
	wg.Wait()
}

func TestDropReadOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	N := uint64(1000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))
	require.NoError(t, db.Close())

	opts.ReadOnly = true
	db2, err := Open(opts)
	// acquireDirectoryLock returns ErrWindowsNotSupported on Windows. It can be ignored safely.
	if runtime.GOOS == "windows" {
		require.Equal(t, err, ErrWindowsNotSupported)
	} else {
		require.NoError(t, err)
	}
	require.Panics(t, func() { db2.DropAll() })
}

func TestWriteAfterClose(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	N := uint64(1000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))
	require.NoError(t, db.Close())
	err = db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte("a"), []byte("b")))
	})
	require.Equal(t, ErrBlockedWrites, err)
}

func TestDropAllRace(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.managedTxns = true
	db, err := Open(opts)
	require.NoError(t, err)

	N := 10000
	// Start a goroutine to keep trying to write to DB while DropAll happens.
	closer := y.NewCloser(1)
	go func() {
		defer closer.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		i := N + 1 // Writes would happen above N.
		var errors int32
		for {
			select {
			case <-ticker.C:
				i++
				txn := db.NewTransactionAt(math.MaxUint64, true)
				require.NoError(t, txn.SetEntry(NewEntry([]byte(key("key", i)), val(false))))
				if err := txn.CommitAt(uint64(i), func(err error) {
					if err != nil {
						atomic.AddInt32(&errors, 1)
					}
				}); err != nil {
					atomic.AddInt32(&errors, 1)
				}
			case <-closer.HasBeenClosed():
				// The following causes a data race.
				// t.Logf("i: %d. Number of (expected) write errors: %d.\n", i, errors)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		txn := db.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.SetEntry(NewEntry([]byte(key("key", i)), val(false))))
		require.NoError(t, txn.CommitAt(uint64(i), func(err error) {
			require.NoError(t, err)
			wg.Done()
		}))
	}
	wg.Wait()

	before := numKeysManaged(db, math.MaxUint64)
	require.True(t, before > N)

	require.NoError(t, db.DropAll())
	closer.SignalAndWait()

	after := numKeysManaged(db, math.MaxUint64)
	t.Logf("Before: %d. After dropall: %d\n", before, after)
	require.True(t, after < before)
	db.Close()
}

func TestDropPrefix(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)

	N := uint64(10000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))

	require.NoError(t, db.DropPrefix([]byte("key000")))
	require.Equal(t, int(N)-10, numKeys(db))

	require.NoError(t, db.DropPrefix([]byte("key00")))
	require.Equal(t, int(N)-100, numKeys(db))

	expected := int(N)
	for i := 0; i < 10; i++ {
		require.NoError(t, db.DropPrefix([]byte(fmt.Sprintf("key%d", i))))
		expected -= 1000
		require.Equal(t, expected, numKeys(db))
	}
	require.NoError(t, db.DropPrefix([]byte("key1")))
	require.Equal(t, 0, numKeys(db))
	require.NoError(t, db.DropPrefix([]byte("key")))
	require.Equal(t, 0, numKeys(db))

	// Check that we can still write to mdb, and using lower timestamps.
	populate(db)
	require.Equal(t, int(N), numKeys(db))
	require.NoError(t, db.DropPrefix([]byte("key")))
	db.Close()

	// Ensure that value log is correctly replayed.
	db2, err := Open(opts)
	require.NoError(t, err)
	require.Equal(t, 0, numKeys(db2))
	db2.Close()
}

func TestDropPrefixWithPendingTxn(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	N := uint64(10000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))

	txn := db.NewTransaction(true)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		itr := txn.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		var keys []string
		for {
			var count int
			for itr.Rewind(); itr.Valid(); itr.Next() {
				count++
				item := itr.Item()
				keys = append(keys, string(item.KeyCopy(nil)))
				_, err := item.ValueCopy(nil)
				if err != nil {
					t.Logf("Got error during value copy: %v", err)
					return
				}
			}
			t.Logf("Got number of keys: %d\n", count)
			for _, key := range keys {
				item, err := txn.Get([]byte(key))
				if err != nil {
					t.Logf("Got error during key lookup: %v", err)
					return
				}
				if _, err := item.ValueCopy(nil); err != nil {
					t.Logf("Got error during second value copy: %v", err)
					return
				}
			}
		}
	}()
	// Do not cancel txn.

	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		require.NoError(t, db.DropPrefix([]byte("key0")))
		require.NoError(t, db.DropPrefix([]byte("key00")))
		require.NoError(t, db.DropPrefix([]byte("key")))
	}()
	wg.Wait()
}

func TestDropPrefixReadOnly(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 5 << 20
	db, err := Open(opts)
	require.NoError(t, err)
	N := uint64(1000)
	populate := func(db *DB) {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			require.NoError(t, writer.Set([]byte(key("key", int(i))), val(true)))
		}
		require.NoError(t, writer.Flush())
	}

	populate(db)
	require.Equal(t, int(N), numKeys(db))
	require.NoError(t, db.Close())

	opts.ReadOnly = true
	db2, err := Open(opts)
	// acquireDirectoryLock returns ErrWindowsNotSupported on Windows. It can be ignored safely.
	if runtime.GOOS == "windows" {
		require.Equal(t, err, ErrWindowsNotSupported)
	} else {
		require.NoError(t, err)
	}
	require.Panics(t, func() { db2.DropPrefix([]byte("key0")) })
}

func TestDropPrefixRace(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()
	opts := getTestOptions(dir)
	opts.managedTxns = true
	db, err := Open(opts)
	require.NoError(t, err)

	N := 10000
	// Start a goroutine to keep trying to write to DB while DropPrefix happens.
	closer := y.NewCloser(1)
	go func() {
		defer closer.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		i := N + 1 // Writes would happen above N.
		var errors int32
		for {
			select {
			case <-ticker.C:
				i++
				txn := db.NewTransactionAt(math.MaxUint64, true)
				require.NoError(t, txn.SetEntry(NewEntry([]byte(key("key", i)), val(false))))
				if err := txn.CommitAt(uint64(i), func(err error) {
					if err != nil {
						atomic.AddInt32(&errors, 1)
					}
				}); err != nil {
					atomic.AddInt32(&errors, 1)
				}
			case <-closer.HasBeenClosed():
				// The following causes a data race.
				// t.Logf("i: %d. Number of (expected) write errors: %d.\n", i, errors)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		txn := db.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.SetEntry(NewEntry([]byte(key("key", i)), val(false))))
		require.NoError(t, txn.CommitAt(uint64(i), func(err error) {
			require.NoError(t, err)
			wg.Done()
		}))
	}
	wg.Wait()

	before := numKeysManaged(db, math.MaxUint64)
	require.True(t, before > N)

	require.NoError(t, db.DropPrefix([]byte("key00")))
	require.NoError(t, db.DropPrefix([]byte("key1")))
	require.NoError(t, db.DropPrefix([]byte("key")))
	closer.SignalAndWait()

	after := numKeysManaged(db, math.MaxUint64)
	t.Logf("Before: %d. After dropprefix: %d\n", before, after)
	require.True(t, after < before)
	db.Close()
}

func TestWriteBatchManagedMode(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}
	opt := DefaultOptions("")
	opt.managedTxns = true
	opt.MaxTableSize = 1 << 15 // This would create multiple transactions in write batch.
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatchAt(1)
		defer wb.Cancel()

		N, M := 50000, 1000
		start := time.Now()

		for i := 0; i < N; i++ {
			require.NoError(t, wb.Set(key(i), val(i)))
		}
		for i := 0; i < M; i++ {
			require.NoError(t, wb.Delete(key(i)))
		}
		require.NoError(t, wb.Flush())
		t.Logf("Time taken for %d writes (w/ test options): %s\n", N+M, time.Since(start))

		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			defer itr.Close()

			i := M
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, string(key(i)), string(item.Key()))
				valcopy, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, val(i), valcopy)
				i++
			}
			require.Equal(t, N, i)
			return nil
		})
		require.NoError(t, err)
	})
}
