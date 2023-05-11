package badger

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackupTemp(t *testing.T) {
	// Create a small db and create a backup. The total memory used by this should not be
	// more than 300 mb.
	opt := DefaultOptions("")
	opt.BackupBatchSize = 1 << 20
	opt.NumGoroutines = 1
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		N := 5
		for i := 0; i < N; i++ {
			require.NoError(t, populateEntries(db, createEntries(100000)))
		}
		totalMemUsed := ReadMemoryStatsAndRunTest(db, t)
		require.LessOrEqual(t, totalMemUsed, uint64(300<<20), "Extra memory used")
	})
}

func BackUpBadger(db *DB, t *testing.T) {
	ti := time.Now()
	formatted := fmt.Sprintf("%d-%02d-%02d_%02d_%02d",
		ti.Year(), ti.Month(), ti.Day(),
		ti.Hour(), ti.Minute())
	err := os.MkdirAll("/tmp/timestone/", os.ModePerm)
	if err != nil {
		panic(err)
	}
	f, _ := os.OpenFile("/tmp/timestone/backup_"+formatted, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	w := io.Writer(f)
	_, err = db.Backup(w, 0)
	if err != nil {
		panic(err)
	}
}

func ReadMemoryStatsAndRunTest(db *DB, t *testing.T) uint64 {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	BackUpBadger(db, t)

	runtime.ReadMemStats(&m2)
	return m2.TotalAlloc - m1.TotalAlloc
}
