//go:build !dragonfly && !freebsd && !windows && !plan9 && !js && !wasip1 && !zos
// +build !dragonfly,!freebsd,!windows,!plan9,!js,!wasip1,!zos

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import "golang.org/x/sys/unix"

func init() {
	datasyncFileFlag = unix.O_DSYNC
}
