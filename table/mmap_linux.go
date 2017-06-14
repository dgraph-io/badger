package table

import (
	"os"
	"syscall"
)

func mmap(fd *os.File, size int64) ([]byte, error) {
	return syscall.Mmap(int(fd.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) (err error) {
	return syscall.Munmap(b)
}
