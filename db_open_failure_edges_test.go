/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tableLoaderCount() int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	count := 0
	for _, stack := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(stack, "github.com/dgraph-io/badger/v4.newLevelsController.func") {
			count++
		}
	}
	return count
}

func TestOpenFailureWaitsForTableLoaders(t *testing.T) {
	opt := openFailureOptions(t.TempDir())
	db, err := Open(opt)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		createAndOpen(db, []keyValVersion{{fmt.Sprintf("key-%d", i), "preserve", 1, 0}}, 0)
	}
	require.NoError(t, db.Close())
	paths, err := filepath.Glob(filepath.Join(opt.Dir, "*.sst"))
	require.NoError(t, err)
	require.Len(t, paths, 5)
	hashes := make(map[string][32]byte)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		hashes[path] = sha256.Sum256(data)
	}

	// Fill all three loader slots. One worker then fails while the other two
	// remain paused; a fourth, if scheduled, pauses too. With five manifest
	// entries, the dispatcher must observe the error from Throttle.Do.
	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var started atomic.Int32
	bad := opt
	bad.tableOpenHook = func(tf *TableManifest) {
		n := started.Add(1)
		if n == 3 {
			close(ready)
		}
		if n == 2 {
			<-ready
			tf.KeyID = 123456789 // The real key registry rejects this key ID.
		} else {
			<-release
		}
	}
	type result struct {
		db  *DB
		err error
	}
	resultCh := make(chan result, 1)
	fds := openFailureFDs()
	go func() {
		db, err := Open(bad)
		resultCh <- result{db, err}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("table loaders did not start")
	}
	var got result
	early := false
	select {
	case got = <-resultCh:
		early = true
		t.Error("Open returned while other table loaders were still running")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	if !early {
		select {
		case got = <-resultCh:
		case <-time.After(5 * time.Second):
			t.Fatal("Open did not join released table loaders")
		}
	}
	require.Nil(t, got.db)
	require.ErrorContains(t, got.err, "Invalid datakey id")
	require.Eventually(t, func() bool { return tableLoaderCount() == 0 }, 5*time.Second, 10*time.Millisecond)
	if after := openFailureFDs(); fds >= 0 {
		require.LessOrEqual(t, after, fds, "tables opened after the error must also be closed")
	}
	for path, want := range hashes {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, sha256.Sum256(data))
	}
	db, err = Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.View(func(txn *Txn) error {
		for i := 0; i < 5; i++ {
			item, err := txn.Get([]byte(fmt.Sprintf("key-%d", i)))
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("preserve"), value)
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestOpenFailureMultipleTableErrors(t *testing.T) {
	opt := openFailureOptions(t.TempDir())
	db, err := Open(opt)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		createAndOpen(db, []keyValVersion{{fmt.Sprintf("key-%d", i), "preserve", 1, 0}}, 0)
	}
	require.NoError(t, db.Close())
	fds := openFailureFDs()
	for attempt := 0; attempt < 8; attempt++ {
		ready := make(chan struct{})
		var started atomic.Int32
		bad := opt
		bad.tableOpenHook = func(tf *TableManifest) {
			if started.Add(1) == 3 {
				close(ready)
			}
			<-ready
			tf.KeyID = 123456789
		}
		failed, err := Open(bad)
		require.Nil(t, failed)
		require.ErrorContains(t, err, "Invalid datakey id")
	}
	if after := openFailureFDs(); fds >= 0 {
		require.LessOrEqual(t, after, fds)
	}
	db, err = Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
