//go:build windows
// +build windows

package badger

import (
	"errors"
	"os"
	"strings"
	"syscall"
)

const errnoUserMappedFile = syscall.Errno(1224)

func isUserMappedFileError(err error) bool {
	if errors.Is(err, errnoUserMappedFile) {
		return true
	}
	// Fall back to string matching because some dependencies (e.g. ristretto)
	// wrap errors with fmt.Errorf using %v instead of %w, losing the error chain.
	return strings.Contains(err.Error(), errnoUserMappedFile.Error())
}

// probeFileTruncate attempts to truncate the file to detect whether the OS
// will allow it. On Windows, this fails with ERROR_USER_MAPPED_FILE when
// another process has the file memory-mapped. By probing before Munmap we
// avoid leaving the mmap in a broken state.
func probeFileTruncate(fd *os.File, size int64) error {
	return fd.Truncate(size)
}
