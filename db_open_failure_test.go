/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openFailureOptions(dir string) Options {
	return DefaultOptions(dir).WithLogger(nil).WithMemTableSize(1 << 20).
		WithValueLogFileSize(1 << 20).WithValueThreshold(32).
		WithBlockCacheSize(1 << 20).WithIndexCacheSize(1 << 20).
		WithNumCompactors(0).WithCompactL0OnClose(false)
}

// Count worker stacks, rather than the process-wide goroutine count, so unrelated
// runtime activity does not hide a leaked Badger/Ristretto worker.
func openFailureWorkers() map[string]int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	counts := make(map[string]int)
	for _, name := range []string{
		"monitorCache", "freeupAllocators", "updateSize", "processItems", "process",
		"listenForValueThresholdUpdate", "runCompactor", "flushMemtable", "doWrites", "listenForUpdates",
	} {
		for _, stack := range strings.Split(buf.String(), "\n\n") {
			if strings.Contains(stack, ")."+name+"(") &&
				(strings.Contains(stack, "github.com/dgraph-io/badger/v4") ||
					strings.Contains(stack, "github.com/dgraph-io/ristretto/v2")) {
				counts[name]++
			}
		}
	}
	return counts
}

func waitOpenFailureWorkers(t *testing.T, before map[string]int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		after := openFailureWorkers()
		leaked := false
		for name, count := range after {
			if count > before[name] {
				leaked = true
			}
		}
		if !leaked {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("worker leak: before=%v after=%v", before, after)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func openFailureFDs() int {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		if files, err := os.ReadDir(dir); err == nil {
			return len(files)
		}
	}
	return -1 // Descriptor enumeration is not available on every platform.
}

type openFailureLogger struct {
	sync.Mutex
	cleanupErrors []string
}

func (l *openFailureLogger) Errorf(format string, args ...interface{}) {
	if strings.HasPrefix(format, "While closing") || strings.HasPrefix(format, "While cleaning up") {
		l.Lock()
		defer l.Unlock()
		l.cleanupErrors = append(l.cleanupErrors, fmt.Sprintf(format, args...))
	}
}

func (*openFailureLogger) Warningf(string, ...interface{}) {}
func (*openFailureLogger) Infof(string, ...interface{})    {}
func (*openFailureLogger) Debugf(string, ...interface{})   {}

func TestOpenFailureCleanup(t *testing.T) {
	for _, stage := range []string{
		"block-cache", "index-cache", "wrong-key", "invalid-key", "malformed-wal", "partial-readonly-wal",
		"wal-key-after-replay", "missing-sst", "malformed-vlog", "vlog-key-after-open",
	} {
		t.Run(stage, func(t *testing.T) {
			opt := openFailureOptions(t.TempDir())
			if stage == "wrong-key" {
				opt.EncryptionKey = bytes.Repeat([]byte{0x11}, 32)
			}
			beforeSeed := openFailureWorkers()
			db, err := Open(opt)
			require.NoError(t, err)
			value := bytes.Repeat([]byte("sentinel"), 64)
			require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("sentinel"), value) }))
			// A complete WAL containing a committed transaction, suitable for replay.
			wal := append([]byte(nil), db.mt.wal.Data[:db.mt.wal.writeAt]...)
			require.NoError(t, db.Close())
			waitOpenFailureWorkers(t, beforeSeed)
			bad := opt
			logger := &openFailureLogger{}
			bad.Logger = logger
			var fixtures []string
			addFixture := func(name string, data []byte) {
				path := filepath.Join(opt.Dir, name)
				require.NoError(t, os.WriteFile(path, data, 0600))
				fixtures = append(fixtures, path)
			}
			var movedSST string
			invalidHeader := make([]byte, vlogHeaderSize)
			binary.BigEndian.PutUint64(invalidHeader, 123456789)
			switch stage {
			case "block-cache":
				// Overflow the counter calculation to exercise NewCache's validation
				// error before it allocates a cache of this size.
				bad.BlockCacheSize = math.MaxInt64
				bad.BlockSize = 1
			case "index-cache":
				bad.MemTableSize = 128
				bad.ValueThreshold = 1
				bad.IndexCacheSize = math.MaxInt64
			case "wrong-key":
				bad.EncryptionKey = bytes.Repeat([]byte{0x22}, 32)
			case "invalid-key":
				bad.EncryptionKey = []byte("invalid")
			case "malformed-wal":
				addFixture("invalid.mem", []byte("test fixture"))
			case "partial-readonly-wal":
				addFixture("00001.mem", append(wal, 0xFF))
				bad.ReadOnly = true
			case "wal-key-after-replay":
				addFixture("00001.mem", wal)
				addFixture("00002.mem", invalidHeader)
				bad.ReadOnly = true
			case "missing-sst":
				files, err := filepath.Glob(filepath.Join(opt.Dir, "*.sst"))
				require.NoError(t, err)
				require.NotEmpty(t, files)
				movedSST = files[0]
				require.NoError(t, os.Rename(movedSST, movedSST+".saved"))
			case "malformed-vlog":
				addFixture("invalid.vlog", []byte("test fixture"))
				bad.NumCompactors = 2
			case "vlog-key-after-open":
				addFixture("000002.vlog", invalidHeader)
				bad.NumCompactors = 2
			}
			hashes := make(map[string][32]byte)
			for _, ext := range []string{"*.mem", "*.vlog", "*.sst"} {
				paths, err := filepath.Glob(filepath.Join(opt.Dir, ext))
				require.NoError(t, err)
				for _, path := range paths {
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					hashes[path] = sha256.Sum256(data)
				}
			}
			before := openFailureWorkers()
			fds := openFailureFDs()
			for i := 0; i < 3; i++ {
				failed, err := Open(bad)
				require.Error(t, err)
				if failed != nil {
					t.Errorf("attempt %d: Open returned a non-nil DB on error: %v", i, err)
				}
				if stage == "wrong-key" {
					require.ErrorIs(t, err, ErrEncryptionKeyMismatch)
				}
				t.Logf("attempt=%d db_non_nil=%v err=%v", i+1, failed != nil, err)
				for path, want := range hashes {
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, want, sha256.Sum256(data), "file changed on failed Open: %s", path)
				}
			}
			// Check before a GC can hide an unclosed file behind os.File's finalizer.
			if after := openFailureFDs(); fds >= 0 && after > fds {
				t.Errorf("descriptor leak: before=%d after=%d", fds, after)
			}
			waitOpenFailureWorkers(t, before)
			logger.Lock()
			require.Empty(t, logger.cleanupErrors, "cleanup must not double-close files")
			logger.Unlock()
			for _, path := range fixtures {
				require.NoError(t, os.Remove(path))
			}
			if movedSST != "" {
				require.NoError(t, os.Rename(movedSST+".saved", movedSST))
			}
			db, err = Open(opt)
			require.NoError(t, err, "failed Open must release the directory lock")
			require.NoError(t, db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte("sentinel"))
				if err != nil {
					return err
				}
				got, err := item.ValueCopy(nil)
				require.Equal(t, value, got)
				return err
			}))
			require.NoError(t, db.Close())
			require.NoError(t, db.Close()) // Successful Close remains idempotent.
			waitOpenFailureWorkers(t, before)
		})
	}
}

func TestOpenFailureNewMemTablePreservesWAL(t *testing.T) {
	opt := openFailureOptions(t.TempDir())
	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("only-in-wal"), []byte("recover me"))
	}))
	wal := append([]byte(nil), db.mt.wal.Data[:db.mt.wal.writeAt]...)
	require.NoError(t, db.Close())
	// Put the WAL in a fresh, empty DB so recovery cannot read the key from an SST.
	opt = openFailureOptions(t.TempDir())
	empty, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, empty.Close())
	path := filepath.Join(opt.Dir, "00001.mem")
	require.NoError(t, os.WriteFile(path, wal, 0600))
	// Exercise the constructor's existing-file error without a filesystem race.
	stub := &DB{opt: opt, registry: newKeyRegistry(KeyRegistryOptions{InMemory: true}), nextMemFid: 1}
	fds := openFailureFDs()
	mt, err := stub.newMemTable()
	require.ErrorContains(t, err, "already exists")
	require.Nil(t, mt)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, wal, got)
	if after := openFailureFDs(); fds >= 0 {
		require.LessOrEqual(t, after, fds)
	}
	recovered, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, recovered.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("only-in-wal"))
		if err != nil {
			return err
		}
		got, err := item.ValueCopy(nil)
		require.Equal(t, []byte("recover me"), got)
		return err
	}))
	require.NoError(t, recovered.Close())
}
