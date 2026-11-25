/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncRecorder replaces vlogSyncFunc and syncFunc (MANIFEST) with versions
// that record each call, then do the real sync. It restores both when the
// test ends.
type syncRecorder struct {
	mu        sync.Mutex
	recording bool
	events    []string
}

func recordSyncs(t *testing.T) *syncRecorder {
	r := &syncRecorder{}
	origVlog, origManifest := vlogSyncFunc, syncFunc
	t.Cleanup(func() { vlogSyncFunc, syncFunc = origVlog, origManifest })
	vlogSyncFunc = func(lf *logFile) error {
		r.note("vlog:" + filepath.Base(lf.path))
		return origVlog(lf)
	}
	syncFunc = func(f *os.File) error {
		r.note("manifest")
		return origManifest(f)
	}
	return r
}

func (r *syncRecorder) note(ev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recording {
		r.events = append(r.events, ev)
	}
}

func (r *syncRecorder) setRecording(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recording = v
}

func (r *syncRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

// With SyncWrites=false, a memtable flush makes value pointers durable in an
// L0 table and MANIFEST. The value log they point into must be synced first,
// or a crash leaves flushed keys reading back as empty values.
func TestFlushSyncsValueLogBeforeManifest(t *testing.T) {
	r := recordSyncs(t)
	opt := DefaultOptions(t.TempDir()).WithSyncWrites(false).WithValueThreshold(64).WithLogger(nil)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	val := make([]byte, 1024) // above ValueThreshold, so it goes to the value log
	for i := range 10 {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(fmt.Appendf(nil, "key-%d", i), val)
		}))
	}

	db.lock.RLock()
	mt := db.mt
	db.lock.RUnlock()
	r.setRecording(true)
	require.NoError(t, db.handleMemTableFlush(mt, nil))
	r.setRecording(false)

	got := r.snapshot()
	vlogIdx := slices.Index(got, "vlog:000001.vlog")
	manifestIdx := slices.Index(got, "manifest")
	require.NotEqual(t, -1, manifestIdx, "expected a MANIFEST sync, got %v", got)
	require.NotEqual(t, -1, vlogIdx, "expected the value log to be synced during the flush, got %v", got)
	require.Less(t, vlogIdx, manifestIdx, "value log must be synced before MANIFEST, got %v", got)
}

// With SyncWrites=false, the old value log must still be synced when it is
// rotated: later syncs (vlog.sync, DB.Sync) only sync the newest file.
func TestRotationSyncsOldValueLog(t *testing.T) {
	r := recordSyncs(t)
	opt := DefaultOptions(t.TempDir()).WithSyncWrites(false).WithValueThreshold(64).
		WithValueLogMaxEntries(5).WithLogger(nil)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	r.setRecording(true)
	val := make([]byte, 1024)
	for i := range 10 {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Set(fmt.Appendf(nil, "key-%d", i), val)
		}))
	}
	r.setRecording(false)

	_, err = os.Stat(filepath.Join(opt.ValueDir, "000002.vlog"))
	require.NoError(t, err, "expected a rotation to 000002.vlog")
	got := r.snapshot()
	require.Contains(t, got, "vlog:000001.vlog", "expected the old value log to be synced at rotation, got %v", got)
}
