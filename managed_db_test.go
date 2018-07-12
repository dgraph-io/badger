package badger

import (
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func val(large bool) []byte {
	var buf []byte
	if large {
		buf = make([]byte, 64)
	} else {
		buf = make([]byte, 16)
	}
	rand.Read(buf)
	return buf
}

func numKeys(mdb *ManagedDB, readTs uint64) int {
	txn := mdb.NewTransactionAt(readTs, false)
	defer txn.Discard()

	itr := txn.NewIterator(DefaultIteratorOptions)
	defer itr.Close()

	var count int
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	return count
}

func TestDropAll(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	mdb, err := OpenManaged(opts)
	require.NoError(t, err)

	N := uint64(10000)
	populate := func(db *ManagedDB, start uint64) {
		var wg sync.WaitGroup
		for i := start; i < start+N; i++ {
			wg.Add(1)
			txn := db.NewTransactionAt(math.MaxUint64, true)
			require.NoError(t, txn.Set([]byte(key("key", int(i))), val(false)))
			require.NoError(t, txn.CommitAt(uint64(i), func(err error) {
				require.NoError(t, err)
				wg.Done()
			}))
		}
		wg.Wait()
	}

	populate(mdb, 1)
	require.Equal(t, int(N), numKeys(mdb, math.MaxUint64))

	require.NoError(t, mdb.DropAll())
	require.Equal(t, 0, numKeys(mdb, math.MaxUint64))

	// Check that we can still write to mdb, and using the same timestamps as before.
	populate(mdb, 1)
	require.Equal(t, int(N), numKeys(mdb, math.MaxUint64))
	mdb.Close()

	// Ensure that value log is correctly replayed, that we are preserving badgerHead.
	mdb2, err := OpenManaged(opts)
	require.NoError(t, err)
	require.Equal(t, int(N), numKeys(mdb2, math.MaxUint64))
	mdb2.Close()
}

func TestDropAllRace(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	mdb, err := OpenManaged(opts)
	require.NoError(t, err)

	N := 10000
	// Start a goroutine to keep trying to write to DB while DropAll happens.
	closer := y.NewCloser(1)
	go func() {
		defer closer.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		i := N + 1 // Writes would happen above N.
		for {
			select {
			case <-ticker.C:
				i++
				txn := mdb.NewTransactionAt(math.MaxUint64, true)
				require.NoError(t, txn.Set([]byte(key("key", i)), val(false)))
				_ = txn.CommitAt(uint64(i), nil)
			case <-closer.HasBeenClosed():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		txn := mdb.NewTransactionAt(math.MaxUint64, true)
		require.NoError(t, txn.Set([]byte(key("key", i)), val(false)))
		require.NoError(t, txn.CommitAt(uint64(i), func(err error) {
			require.NoError(t, err)
			wg.Done()
		}))
	}
	wg.Wait()

	before := numKeys(mdb, math.MaxUint64)
	require.True(t, before > N)

	require.NoError(t, mdb.DropAll())
	closer.SignalAndWait()

	after := numKeys(mdb, math.MaxUint64)
	t.Logf("Before: %d. After dropall: %d\n", before, after)
	require.True(t, after < before)
	mdb.Close()
}
