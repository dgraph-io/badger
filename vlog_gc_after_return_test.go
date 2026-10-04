/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// GC relocation in the active memtable must remain hidden even when a later
// Flatten removes obsolete versions from the SSTs without flushing that memtable.
func TestVlogGCDeletedKeyAfterFlatten(t *testing.T) {
	opt := DefaultOptions(t.TempDir()).WithLogger(nil).WithNumCompactors(0).WithCompactL0OnClose(false).WithValueThreshold(1024).WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
	d, e := Open(opt)
	require.NoError(t, e)
	key := []byte("c-deleted")
	require.NoError(t, d.Update(func(tx *Txn) error { return tx.Set(key, bytes.Repeat([]byte{42}, 2<<20)) }))
	require.NoError(t, d.Close())
	d, e = Open(opt)
	require.NoError(t, e)
	require.NoError(t, d.lc.doCompact(0, compactionPriority{level: 0, score: 1.71}))
	require.NoError(t, d.Update(func(tx *Txn) error { return tx.Delete(key) }))
	require.NoError(t, d.Close())
	d, e = Open(opt)
	require.NoError(t, e)
	defer d.Close()
	check := func(stage string) {
		err := d.View(func(tx *Txn) error {
			i, e := tx.Get(key)
			if e == nil {
				t.Logf("%s: visible historical version=%d", stage, i.Version())
			}
			return e
		})
		require.ErrorIs(t, err, ErrKeyNotFound, stage)
	}
	check("before GC")
	gcTestAdvanceRead(t, d)
	for _, f := range gcTestFiles(d) {
		require.NoError(t, d.vlog.rewrite(f))
	}
	check("after GC, before Flatten")
	require.NoError(t, d.Flatten(1))
	check("after Flatten, without flushing GC memtable")
}
