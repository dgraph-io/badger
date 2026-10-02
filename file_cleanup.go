/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/ristretto/v2/z"
)

// closeMmapOnError releases an exclusively owned file mapping after failed
// initialization. It never truncates or removes the file. Unlike MmapFile.Close,
// a sync failure must not prevent unmapping or closing the descriptor.
// This is only for filesystem mappings, not in-memory tables backed by Go slices.
func closeMmapOnError(mf *z.MmapFile) error {
	return closeMmapOnErrorWith(mf, z.Msync, z.Munmap)
}

// Explicit operations allow cleanup failures to be tested with real mappings
// without changing process-global syscall functions.
func closeMmapOnErrorWith(mf *z.MmapFile, sync, unmap func([]byte) error) error {
	if mf == nil {
		return nil
	}
	name := "file mapping"
	if mf.Fd != nil {
		name = mf.Fd.Name()
	}
	var errs []error
	if len(mf.Data) > 0 {
		if err := sync(mf.Data); err != nil {
			errs = append(errs, fmt.Errorf("sync %s: %w", name, err))
		}
		if err := unmap(mf.Data); err != nil {
			errs = append(errs, fmt.Errorf("unmap %s: %w", name, err))
		} else {
			mf.Data = nil
		}
	}
	if mf.Fd != nil {
		if err := mf.Fd.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", name, err))
		}
		// os.File.Close makes the handle unusable even when it returns an error.
		// Do not close it again: the descriptor number may have been reused.
		mf.Fd = nil
	}
	return errors.Join(errs...)
}

func (lf *logFile) closeOnError() error {
	if lf.MmapFile == nil {
		return nil
	}
	err := closeMmapOnError(lf.MmapFile)
	// A failed unmap leaves Data reachable so an enclosing cleanup can retry.
	if len(lf.Data) == 0 && lf.Fd == nil {
		lf.MmapFile = nil
	}
	return err
}
