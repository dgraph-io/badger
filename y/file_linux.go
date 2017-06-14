package y

import (
	"syscall"
)

func init() {
	syncFileFlag = syscall.O_DSYNC
}
