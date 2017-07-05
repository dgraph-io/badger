package y

import (
	"golang.org/x/sys/unix"
)

func init() {
	syncFileFlag = unix.O_SYNC
}
