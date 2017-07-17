// +build darwin linux netbsd openbsd solaris

package y

import (
	"golang.org/x/sys/unix"
)

func init() {
	datasyncFileFlag = unix.O_DSYNC
}
