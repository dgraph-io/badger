/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompletionWithoutDir(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			oldSstDir := sstDir
			defer func() { sstDir = oldSstDir }()
			sstDir = ""

			RootCmd.SetOut(new(bytes.Buffer))
			RootCmd.SetArgs([]string{"completion", shell})

			err := RootCmd.Execute()
			require.NoError(t, err, "completion %s should not require --dir", shell)
		})
	}
}

func TestInfoWithoutDirFails(t *testing.T) {
	oldSstDir := sstDir
	defer func() { sstDir = oldSstDir }()
	sstDir = ""

	RootCmd.SetArgs([]string{"info"})
	err := RootCmd.Execute()
	require.Error(t, err, "info without --dir should fail")
	require.Contains(t, err.Error(), "--dir not specified")
}
