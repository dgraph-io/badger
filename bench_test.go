package badger

import (
	"expvar"
	"math/rand"
	"os"
	"testing"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"
)

type BFrame struct {
	db      *DB
	run     func()
	cleanup func()
	reset   func()
}

func BenchmarkBackup(b *testing.B) {
	init := func() (string, *DB) {
		dir, err := os.MkdirTemp("", "badger-test")
		opts := getTestOptions(dir)
		require.NoError(b, err)
		db, err := Open(opts)
		require.NoError(b, err)
		return dir, db
	}

	dir, db := init()
	N := uint64(100000)
	writer := db.NewWriteBatch()
	for i := uint64(0); i < N; i++ {
		writer.Set([]byte(key("key", int(i))), val(false))
	}
	writer.Flush()

	read := func() {
		//db.Backup()
	}

	cleanup := func() {
		db.Close()
		removeDir(dir)
	}

	reset := func() {
	}

	s := BFrame{db: db, cleanup: cleanup, run: read, reset: reset}
	s.BenchmarkMemory(b)
}

func BenchmarkReadB(b *testing.B) {
	init := func() (string, *DB) {
		dir, err := os.MkdirTemp("", "badger-test")
		opts := getTestOptions(dir)
		require.NoError(b, err)
		db, err := Open(opts)
		require.NoError(b, err)
		return dir, db
	}

	dir, db := init()
	N := uint64(100000)
	M := uint64(1000)
	writer := db.NewWriteBatch()
	for i := uint64(0); i < N; i++ {
		writer.Set([]byte(key("key", int(i))), val(false))
	}
	writer.Flush()

	read := func() {
		txn2 := db.NewTransaction(true)
		for i := uint64(0); i < M; i++ {
			txn2.Get([]byte(key("key", rand.Intn(int(N)))))
		}
	}

	cleanup := func() {
		db.Close()
		removeDir(dir)
	}

	reset := func() {
	}

	s := BFrame{db: db, cleanup: cleanup, run: read, reset: reset}
	s.Benchmark(b)
}

func BenchmarkWriteBatch(b *testing.B) {
	init := func() (string, *DB) {
		dir, err := os.MkdirTemp("", "badger-test")
		opts := getTestOptions(dir)
		require.NoError(b, err)
		db, err := Open(opts)
		require.NoError(b, err)
		return dir, db
	}

	dir, db := init()
	N := uint64(100000)

	populate := func() {
		writer := db.NewWriteBatch()
		for i := uint64(0); i < N; i++ {
			writer.Set([]byte(key("key", int(i))), val(false))
		}
		writer.Flush()
	}

	cleanup := func() {
		db.Close()
		removeDir(dir)
	}

	reset := func() {
		cleanup()
		dir, db = init()
	}

	s := BFrame{db: db, cleanup: cleanup, run: populate, reset: reset}
	//s.BenchmarkMetric(b, "badger_v4_puts_total")
	s.BenchmarkMemory(b)
}

func (s *BFrame) Benchmark(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.reset()
		b.StartTimer()
		s.run()
		b.StopTimer()
	}
	s.cleanup()
}

func (s *BFrame) BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {

		s.reset()

		p := profile.Start(profile.MemProfile, profile.MemProfileRate(1))
		b.StartTimer()
		s.run()
		b.StopTimer()
		p.Stop()

	}

	s.cleanup()
}

func clearExpvarVariables(metric_name string) {
	expvars := expvar.Get(metric_name)
	switch v := expvars.(type) {
	case *expvar.Int:
		v.Set(0)
	case *expvar.Float:
		v.Set(0)
	case *expvar.Map:
		v.Init()
	case *expvar.String:
		v.Set("")
	}
}

func (s *BFrame) BenchmarkMetric(b *testing.B, metric_name string) {
	for i := 0; i < b.N; i++ {
		s.reset()
		clearExpvarVariables(metric_name)
		b.StartTimer()
		s.run()
		b.StopTimer()

		metric := expvar.Get(metric_name).(*expvar.Int).Value()
		b.ReportMetric(float64(metric), "num_metric/op")
	}

	s.cleanup()
}
