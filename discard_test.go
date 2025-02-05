/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscardStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	ds, err := InitDiscardStats(opt)
	require.NoError(t, err)
	require.Zero(t, ds.nextEmptySlot)
	fid, _ := ds.MaxDiscard()
	require.Zero(t, fid)

	for i := uint32(0); i < 20; i++ {
		require.Equal(t, int64(i*100), ds.Update(i, int64(i*100)))
	}
	ds.Iterate(func(id, val uint64) {
		require.Equal(t, id*100, val)
	})
	for i := uint32(0); i < 10; i++ {
		require.Equal(t, 0, int(ds.Update(i, -1)))
	}
	ds.Iterate(func(id, val uint64) {
		if id < 10 {
			require.Zero(t, val)
			return
		}
		require.Equal(t, int(id*100), int(val))
	})
}

func TestReloadDiscardStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	ds := db.vlog.discardStats

	ds.Update(uint32(1), 1)
	ds.Update(uint32(2), 1)
	ds.Update(uint32(1), -1)
	require.NoError(t, db.Close())

	// Reopen the DB, discard stats should be same.
	db2, err := Open(opt)
	require.NoError(t, err)
	ds2 := db2.vlog.discardStats
	require.Zero(t, ds2.Update(uint32(1), 0))
	require.Equal(t, 1, int(ds2.Update(uint32(2), 0)))
}
