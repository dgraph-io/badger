//go:build !windows && !plan9 && !js && !wasip1 && !aix
// +build !windows,!plan9,!js,!wasip1,!aix

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A copy of the directory's descriptor that outlives Close must not keep the directory locked. A child process forked
// while the database is open holds such a copy until it calls exec, and flock locks belong to the open file
// description that all the copies share.
func TestDirectoryLockReleasedWhileACopyOfItsDescriptorIsOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	inherited, err := unix.Dup(int(db.dirLockGuard.f.Fd()))
	require.NoError(t, err)
	defer func() { _ = unix.Close(inherited) }()
	require.NoError(t, db.Close())

	db, err = Open(DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err, "the directory stays locked by the copy of the closed database's descriptor")
	require.NoError(t, db.Close())
}
