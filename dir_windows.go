// +build windows

/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

// OpenDir opens a directory in windows with write access for syncing.
import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	"github.com/theckman/go-flock"
)

func openDir(path string) (*os.File, error) {
	fd, err := openDirWin(path)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openDirWin(path string) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, syscall.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	access := uint32(syscall.GENERIC_READ | syscall.GENERIC_WRITE)
	sharemode := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE)
	createmode := uint32(syscall.OPEN_EXISTING)
	fl := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS)
	return syscall.CreateFile(pathp, access, sharemode, nil, createmode, fl, 0)
}

// DirectoryLockGuard holds a lock on the directory.
type directoryLockGuard struct {
	lock *flock.Flock
}

// AcquireDirectoryLock acquires exclusive access to a directory.
func acquireDirectoryLock(dirPath string, lockFileName string, readOnly bool) (*directoryLockGuard, error) {
	if readOnly {
		return nil, ErrWindowsNotSupported
	}

	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absLockFilePath, err := filepath.Abs(filepath.Join(dirPath, lockFileName))
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get absolute path for pid lock file")
	}

	lock := flock.NewFlock(absLockFilePath)
	unlocked, err := lock.TryLock()
	if err != nil {
		return nil, errors.Wrapf(err,"Cannot create pid lock file %q", absLockFilePath)
	}
	if !unlocked {
		return nil, fmt.Errorf("Cannot get lock. Another process is using this Badger database")
	}
	return &directoryLockGuard{lock}, nil
}

// Release removes the directory lock.
func (g *directoryLockGuard) release() error {
	return g.lock.Unlock()
}
