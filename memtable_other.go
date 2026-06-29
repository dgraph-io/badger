//go:build !windows
// +build !windows

package badger

import "os"

func isUserMappedFileError(_ error) bool {
	return false
}

func probeFileTruncate(_ *os.File, _ int64) error {
	return nil
}
