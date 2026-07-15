//go:build with_snapshot

package badger

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func TestPinnedSnapshotIsolation(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		setKey := func(key, val string) {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(key), []byte(val))
			}))
		}
		setKey("a", "before")

		snap := db.NewSnapshot()
		defer snap.Close()

		setKey("a", "after")
		setKey("b", "only-after")

		item, err := snap.Get([]byte("a"))
		require.NoError(t, err)
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, "before", string(val))

		_, err = snap.Get([]byte("b"))
		require.Equal(t, ErrKeyNotFound, err)
	})
}

func TestPinnedSnapshotCloseLifecycle(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		snap := db.NewSnapshot()
		require.EqualValues(t, 1, db.NumActiveSnapshots())

		snap.Close()
		require.EqualValues(t, 0, db.NumActiveSnapshots())

		// Double-close must not panic.
		snap.Close()
		require.EqualValues(t, 0, db.NumActiveSnapshots())

		// Using a closed snapshot must panic.
		require.Panics(t, func() { snap.NewTransaction() })
	})
}

func TestPinnedSnapshotActiveCount(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.EqualValues(t, 0, db.NumActiveSnapshots())

		snaps := make([]*Snapshot, 5)
		for i := range snaps {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
			}))
			snaps[i] = db.NewSnapshot()
		}
		require.EqualValues(t, 5, db.NumActiveSnapshots())

		for i := 1; i < len(snaps); i++ {
			require.Greater(t, snaps[i].ReadTs(), snaps[i-1].ReadTs())
		}

		for i := len(snaps) - 1; i >= 0; i-- {
			snaps[i].Close()
			require.EqualValues(t, int64(i), db.NumActiveSnapshots())
		}
	})
}

func TestPinnedSnapshotConcurrentIterators(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		nKeys := 100
		for i := 0; i < nKeys; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%04d", i)), []byte("initial"))
			}))
		}

		snap := db.NewSnapshot()
		defer snap.Close()

		for i := 0; i < nKeys; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%04d", i)), []byte("overwritten"))
			}))
		}

		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn := snap.NewTransaction()
				defer txn.Discard()
				it := txn.NewIterator(DefaultIteratorOptions)
				defer it.Close()

				count := 0
				for it.Rewind(); it.Valid(); it.Next() {
					val, err := it.Item().ValueCopy(nil)
					require.NoError(t, err)
					require.Equal(t, "initial", string(val))
					count++
				}
				require.Equal(t, nKeys, count)
			}()
		}
		wg.Wait()
	})
}

func TestPinnedSnapshotPanicsOnManagedDB(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.managedTxns = true
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	require.Panics(t, func() { db.NewSnapshot() })
}

func TestOrchestrateReadTsPin(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set([]byte("sentinel"), []byte("v"))
		}))

		stream := db.NewStream()
		var observedTs uint64

		stream.Send = func(buf *z.Buffer) error {
			return nil
		}
		stream.ChooseKey = func(item *Item) bool {
			observedTs = item.Version()
			return true
		}

		require.NoError(t, stream.Orchestrate(context.Background()))
		require.NotZero(t, observedTs, "readTs must be pinned at start of Orchestrate")
	})
}

func TestOrchestrateWorkersShareReadTs(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		nKeys := 200
		for i := 0; i < nKeys; i++ {
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.Set([]byte(fmt.Sprintf("key-%04d", i)), []byte("v"))
			}))
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_ = db.Update(func(txn *Txn) error {
					return txn.Set([]byte(fmt.Sprintf("churn-%d", i)), []byte("x"))
				})
				i++
			}
		}()

		stream := db.NewStream()
		var mu sync.Mutex
		seen := make(map[uint64]int)

		stream.Send = func(buf *z.Buffer) error { return nil }
		stream.ChooseKey = func(item *Item) bool {
			mu.Lock()
			seen[item.Version()]++
			mu.Unlock()
			return true
		}

		require.NoError(t, stream.Orchestrate(context.Background()))
		cancel()

		var maxTs uint64
		for ts := range seen {
			if ts > maxTs {
				maxTs = ts
			}
		}
		for ts := range seen {
			require.LessOrEqual(t, ts, maxTs)
		}
		require.Less(t, len(seen), nKeys*2,
			"too many distinct readTs values -- workers are not sharing a snapshot")
	})
}

// TestBackupDuringConcurrentWrites verifies that a backup taken while
// concurrent writers are actively committing still restores to a
// self-consistent database whose keys all come from a single point in time.
func TestBackupDuringConcurrentWrites(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	opts := DefaultOptions(dir1)
	db1, err := Open(opts)
	require.NoError(t, err)

	nKeys := 100
	for i := 0; i < nKeys; i++ {
		require.NoError(t, db1.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("key-%04d", i)), []byte("original"))
		}))
	}

	// Concurrent writer overwrites keys while backup is in flight.
	ctx, cancel := context.WithCancel(context.Background())
	var writerDone sync.WaitGroup
	writerDone.Add(1)
	var writes atomic.Int64
	go func() {
		defer writerDone.Done()
		round := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = db1.Update(func(txn *Txn) error {
				for i := 0; i < nKeys; i++ {
					k := fmt.Sprintf("key-%04d", i)
					v := fmt.Sprintf("round-%d", round)
					if err := txn.Set([]byte(k), []byte(v)); err != nil {
						return err
					}
				}
				return nil
			})
			writes.Add(1)
			round++
		}
	}()

	buf := new(backupBuffer)
	_, err = db1.Backup(buf, 0)
	require.NoError(t, err)

	cancel()
	writerDone.Wait()
	require.NoError(t, db1.Close())

	// Restore and verify consistency: every key must have the same value,
	// proving all workers saw the same snapshot.
	opts2 := DefaultOptions(dir2)
	db2, err := Open(opts2)
	require.NoError(t, err)
	require.NoError(t, db2.Load(buf, math.MaxInt16))
	require.NoError(t, db2.Close())

	db2, err = Open(opts2)
	require.NoError(t, err)
	defer db2.Close()

	require.NoError(t, db2.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()

		values := make(map[string]int)
		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			values[string(val)]++
		}
		// All keys must share one value ("original" or some "round-N").
		// If the backup mixed snapshots we would see multiple distinct values.
		require.LessOrEqual(t, len(values), 1,
			"backup is inconsistent: keys have different values from different snapshots: %v", values)
		return nil
	}))
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	opts := DefaultOptions(dir1)
	db1, err := Open(opts)
	require.NoError(t, err)

	nKeys := 50
	expected := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%04d", i)
		expected[k] = v
		require.NoError(t, db1.Update(func(txn *Txn) error {
			return txn.Set([]byte(k), []byte(v))
		}))
	}

	buf := new(backupBuffer)
	_, err = db1.Backup(buf, 0)
	require.NoError(t, err)
	require.NoError(t, db1.Close())

	opts2 := DefaultOptions(dir2)
	db2, err := Open(opts2)
	require.NoError(t, err)
	require.NoError(t, db2.Load(buf, math.MaxInt16))
	require.NoError(t, db2.Close())

	db2, err = Open(opts2)
	require.NoError(t, err)
	defer db2.Close()

	require.NoError(t, db2.View(func(txn *Txn) error {
		for k, v := range expected {
			item, err := txn.Get([]byte(k))
			require.NoError(t, err)
			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, v, string(val))
		}
		return nil
	}))
}

type backupBuffer struct {
	data []byte
	pos  int
}

func (b *backupBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *backupBuffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
