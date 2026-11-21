/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

func TestAddLevel0TableSyncsDirBeforeManifest(t *testing.T) {
	dir := t.TempDir()
	valueDir := t.TempDir()
	opt := DefaultOptions(dir).WithValueDir(valueDir).WithSyncWrites(true).WithLogger(nil)

	var mu sync.Mutex
	recording := false
	var events []string
	note := func(ev string) {
		mu.Lock()
		defer mu.Unlock()
		if recording {
			events = append(events, ev)
		}
	}
	setRecording := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		recording = v
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), events...)
	}

	origSyncDir := syncDirFunc
	origSyncFunc := syncFunc
	defer func() {
		syncDirFunc = origSyncDir
		syncFunc = origSyncFunc
	}()
	syncDirFunc = func(d string) error {
		note("syncDir:" + d)
		return origSyncDir(d)
	}
	syncFunc = func(f *os.File) error {
		note("manifestSync")
		return origSyncFunc(f)
	}

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	bopts := buildTableOptions(db)
	builder := table.NewTableBuilder(bopts)
	defer builder.Close()
	builder.Add(y.KeyWithTs([]byte("k1"), 1), y.ValueStruct{Value: []byte("v1")}, 0)

	fileID := db.lc.reserveFileID()
	tbl, err := table.CreateTable(table.NewFilename(fileID, db.opt.Dir), builder)
	require.NoError(t, err)
	defer func() { _ = tbl.DecrRef() }()

	setRecording(true)
	require.NoError(t, db.lc.addLevel0Table(tbl))
	setRecording(false)

	got := snapshot()
	syncDirIdx := -1
	manifestSyncIdx := -1
	for i, ev := range got {
		if ev == "syncDir:"+dir && syncDirIdx == -1 {
			syncDirIdx = i
		}
		if ev == "manifestSync" && manifestSyncIdx == -1 {
			manifestSyncIdx = i
		}
	}
	require.NotEqual(t, -1, manifestSyncIdx, "expected MANIFEST fsync")
	require.NotEqual(t, -1, syncDirIdx,
		"expected syncDir(opt.Dir) before MANIFEST fsync, got events=%v", got)
	require.Less(t, syncDirIdx, manifestSyncIdx,
		"syncDir(opt.Dir) must happen strictly before MANIFEST fsync, got events=%v", got)
}

func TestAddLevel0TableSyncDirError(t *testing.T) {
	dir := t.TempDir()
	valueDir := t.TempDir()
	opt := DefaultOptions(dir).WithValueDir(valueDir).WithSyncWrites(true).WithLogger(nil)

	injectedErr := errors.New("injected syncDir error")
	var mu sync.Mutex
	failSyncDir := false
	manifestSynced := false
	arm := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		failSyncDir = v
	}

	origSyncDir := syncDirFunc
	origSyncFunc := syncFunc
	defer func() {
		syncDirFunc = origSyncDir
		syncFunc = origSyncFunc
	}()
	syncDirFunc = func(d string) error {
		mu.Lock()
		fail := failSyncDir
		mu.Unlock()
		if fail {
			return injectedErr
		}
		return origSyncDir(d)
	}
	syncFunc = func(f *os.File) error {
		mu.Lock()
		manifestSynced = true
		mu.Unlock()
		return origSyncFunc(f)
	}

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	bopts := buildTableOptions(db)
	builder := table.NewTableBuilder(bopts)
	defer builder.Close()
	builder.Add(y.KeyWithTs([]byte("k1"), 1), y.ValueStruct{Value: []byte("v1")}, 0)

	fileID := db.lc.reserveFileID()
	tbl, err := table.CreateTable(table.NewFilename(fileID, db.opt.Dir), builder)
	require.NoError(t, err)
	defer func() { _ = tbl.DecrRef() }()

	l0Before := db.lc.levels[0].numTables()
	mu.Lock()
	manifestSynced = false
	mu.Unlock()
	arm(true)
	// Disarm before the deferred Close runs, so a failed assertion below cannot
	// make Close fail too.
	defer arm(false)

	err = db.lc.addLevel0Table(tbl)
	require.ErrorIs(t, err, injectedErr)

	mu.Lock()
	called := manifestSynced
	mu.Unlock()

	require.False(t, called, "MANIFEST syncFunc must not be called when syncDir fails")
	require.Equal(t, l0Before, db.lc.levels[0].numTables(),
		"table must not be added to L0 when syncDir fails")
}

func TestCreateVlogFileSyncsValueDir(t *testing.T) {
	dir := t.TempDir()
	valueDir := t.TempDir()
	opt := DefaultOptions(dir).
		WithValueDir(valueDir).
		WithSyncWrites(true).
		WithValueThreshold(256).
		WithValueLogFileSize(1 << 20).
		WithLogger(nil)

	var mu sync.Mutex
	recording := false
	var syncedDirs []string
	note := func(d string) {
		mu.Lock()
		defer mu.Unlock()
		if recording {
			syncedDirs = append(syncedDirs, d)
		}
	}
	setRecording := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		recording = v
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), syncedDirs...)
	}

	origSyncDir := syncDirFunc
	defer func() { syncDirFunc = origSyncDir }()
	syncDirFunc = func(d string) error {
		note(d)
		return origSyncDir(d)
	}

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	setRecording(true)
	val := bytes.Repeat([]byte("V"), 64<<10)
	rotated := false
	for i := 0; i < 30; i++ {
		err := db.Update(func(txn *Txn) error {
			return txn.Set([]byte(fmt.Sprintf("k_%03d", i)), val)
		})
		require.NoError(t, err)
		if _, err := os.Stat(filepath.Join(valueDir, "000002.vlog")); err == nil {
			rotated = true
			break
		}
	}
	setRecording(false)
	require.True(t, rotated, "expected 000002.vlog to be created via rotation")

	got := snapshot()
	found := false
	for _, d := range got {
		if d == valueDir {
			found = true
			break
		}
	}
	require.True(t, found, "expected syncDir(%s) on .vlog rotation, got %v", valueDir, got)
}

func TestCreateVlogFileCleansUpOnSyncDirError(t *testing.T) {
	dir := t.TempDir()
	valueDir := t.TempDir()
	opt := DefaultOptions(dir).WithValueDir(valueDir).WithSyncWrites(true).WithLogger(nil)

	injectedErr := errors.New("injected syncDir error")
	var mu sync.Mutex
	failSyncDir := false
	arm := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		failSyncDir = v
	}

	origSyncDir := syncDirFunc
	defer func() { syncDirFunc = origSyncDir }()
	syncDirFunc = func(d string) error {
		mu.Lock()
		fail := failSyncDir
		mu.Unlock()
		if fail {
			return injectedErr
		}
		return origSyncDir(d)
	}

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	prevMaxFid := db.vlog.maxFid
	nextFid := prevMaxFid + 1
	nextPath := db.vlog.fpath(nextFid)

	arm(true)
	// Disarm before the deferred Close runs, so a failed assertion below cannot
	// make Close fail too.
	defer arm(false)

	_, err = db.vlog.createVlogFile()
	require.ErrorContains(t, err, injectedErr.Error())

	arm(false)

	_, statErr := os.Stat(nextPath)
	require.True(t, os.IsNotExist(statErr),
		"expected %s to be deleted after syncDir failure", nextPath)
	require.Equal(t, prevMaxFid, db.vlog.maxFid,
		"maxFid must remain unchanged after syncDir failure")
	_, existsInMap := db.vlog.filesMap[nextFid]
	require.False(t, existsInMap, "filesMap must not contain failed fid %d", nextFid)

	lf, err := db.vlog.createVlogFile()
	require.NoError(t, err)
	require.Equal(t, nextFid, lf.fid)
}
