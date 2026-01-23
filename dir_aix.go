//go:build aix
// +build aix

/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/dgraph-io/badger/v4/y"
)

// AIX flock locks files, not descriptors. So, multiple descriptors cannot
// be used in the same file. The first to close removals the lock on the
// file.
type directoryLockGuard struct {
	// The absolute path to our pid file.
	path string
	// Was this a shared lock for a read-only database?
	readOnly bool
}

// AIX flocking is file x process, not fd x file x process like linux. We can
// only hold one descriptor with a lock open at any given time.
type aixFlock struct {
	file     *os.File
	count    int
	readOnly bool
}

// Keep a map of locks synchronized by a mutex.
var aixFlockMap = map[string]*aixFlock{}
var aixFlockMapLock sync.Mutex

// acquireDirectoryLock gets a lock on the directory (using flock). If
// this is not read-only, it will also write our pid to
// dirPath/pidFileName for convenience.
func acquireDirectoryLock(dirPath string, pidFileName string, readOnly bool) (
	*directoryLockGuard, error) {

	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absPidFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, y.Wrapf(err, "cannot get absolute path for pid lock file")
	}

	aixFlockMapLock.Lock()
	defer aixFlockMapLock.Unlock()

	lg := &directoryLockGuard{absPidFilePath, readOnly}

	if lock, fnd := aixFlockMap[absPidFilePath]; fnd {
		if !readOnly || lock.readOnly != readOnly {
			return nil, fmt.Errorf(
				"Cannot acquire directory lock on %q.  Another process is using this Badger database.", dirPath)
		}
		lock.count++
	} else {
		// This is the first acquirer, set up a lock file and register it.
		f, err := os.OpenFile(absPidFilePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, y.Wrapf(err, "cannot create/open pid file %q", absPidFilePath)
		}

		opts := unix.F_WRLCK
		if readOnly {
			opts = unix.F_RDLCK
		}

		flckt := unix.Flock_t{int16(opts), 0, 0, 0, 0, 0, 0}
		err = unix.FcntlFlock(uintptr(f.Fd()), unix.F_SETLK, &flckt)
		if err != nil {
			f.Close()
			return nil, y.Wrapf(err,
				"Cannot acquire directory lock on %q.  Another process is using this Badger database.", dirPath)
		}

		if !readOnly {
			f.Truncate(0)
			// Write our pid to the file.
			_, err = f.Write([]byte(fmt.Sprintf("%d\n", os.Getpid())))
			if err != nil {
				f.Close()
				return nil, y.Wrapf(err,
					"Cannot write pid file %q", absPidFilePath)
			}
		}
		aixFlockMap[absPidFilePath] = &aixFlock{f, 1, readOnly}
	}
	return lg, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() error {
	var err error

	aixFlockMapLock.Lock()
	defer aixFlockMapLock.Unlock()

	if lock, fnd := aixFlockMap[guard.path]; fnd {
		lock.count--
		if lock.count == 0 {
			if !lock.readOnly {
				// Try to clear the PID if we succeed.
				lock.file.Truncate(0)
				os.Remove(guard.path)
			}

			if closeErr := lock.file.Close(); err == nil {
				err = closeErr
			}
			delete(aixFlockMap, guard.path)
			guard.path = ""
		}
	} else {
		err = errors.New(fmt.Sprintf("unknown lock %v", guard.path))
	}

	return err
}

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) { return os.Open(path) }

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	var err error
	// TODO: AIX does not support fsync on a directory. Doing a full file system sync may be
	// undesirable too (e.x os.Sync()).
	return err
}
