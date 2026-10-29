/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// An empty memtable file is a creation that did not finish, as on a full disk: it holds no entry.
// Open skips it, and removes it unless the database is read-only.
func TestOpenSkipsEmptyMemtableFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := getTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	}))
	empty := db.mtFilePath(db.nextMemFid + 10)
	require.NoError(t, db.Close())
	require.NoError(t, os.WriteFile(empty, nil, 0666))

	get := func(db *DB) error {
		return db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("key"))
			return err
		})
	}
	db, err = Open(opt.WithReadOnly(true))
	require.NoError(t, err)
	require.NoError(t, get(db))
	require.NoError(t, db.Close())
	require.FileExists(t, empty)

	db, err = Open(opt)
	require.NoError(t, err)
	require.NoError(t, get(db))
	require.NoError(t, db.Close())
	require.NoFileExists(t, empty)
}
