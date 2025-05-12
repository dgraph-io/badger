/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"github.com/spf13/cobra"
)

var benchCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Benchmark Badger database.",
	Long: `This command will benchmark Badger for different usecases. 
	Useful for testing and performance analysis.`,
}

func init() {
	RootCmd.AddCommand(benchCmd)
}
