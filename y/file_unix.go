// +build darwin linux netbsd openbsd solaris

package y

import (
	"golang.org/x/sys/unix"
)

func init() {
	syncFileFlag = unix.O_DSYNC
}
