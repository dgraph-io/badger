/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"
)

// TestMaxLevelsFlag tests the --max-levels CLI flag
func TestMaxLevelsFlag(t *testing.T) {
	tests := []struct {
		name        string
		maxLevels   int
		shouldError bool
	}{
		{"max-levels-0", 0, true},
		{"max-levels-negative", -1, true},
		{"max-levels-10", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "badger-max-levels-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			// Set the vMaxLevels variable to test value
			oldMaxLevels := vMaxLevels
			vMaxLevels = tt.maxLevels
			defer func() { vMaxLevels = oldMaxLevels }()

			sstDir = dir
			vlogDir = dir

			// Test validation - create a mock cobra command to avoid nil pointer dereference
			cmd := &cobra.Command{Use: "test"}
			err = validateRootCmdArgs(cmd, []string{})

			if tt.shouldError {
				require.Error(t, err, "Expected validation to fail for max-levels=%d", tt.maxLevels)
				require.Contains(t, err.Error(), "--max-levels should be at least 1")
			} else {
				require.NoError(t, err, "Expected validation to pass for max-levels=%d", tt.maxLevels)

				// Test that DB can be opened with the max-levels setting
				opts := badger.DefaultOptions(dir).WithMaxLevels(vMaxLevels)
				db, err := badger.Open(opts)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
		})
	}
}
