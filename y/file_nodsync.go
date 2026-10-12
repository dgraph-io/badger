//go:build dragonfly || freebsd || windows || plan9 || zos
// +build dragonfly freebsd windows plan9 zos

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import "syscall"

func init() {
	datasyncFileFlag = syscall.O_SYNC
}
