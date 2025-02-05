/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var sstDir, vlogDir string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:               "badger",
	Short:             "Tools to manage Badger database.",
	PersistentPreRunE: validateRootCmdArgs,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVar(&sstDir, "dir", "",
		"Directory where the LSM tree files are located. (required)")

	RootCmd.PersistentFlags().StringVar(&vlogDir, "vlog-dir", "",
		"Directory where the value log files are located, if different from --dir")
}

func validateRootCmdArgs(cmd *cobra.Command, args []string) error {
	if strings.HasPrefix(cmd.Use, "help ") { // No need to validate if it is help
		return nil
	}
	if sstDir == "" {
		return errors.New("--dir not specified")
	}
	if vlogDir == "" {
		vlogDir = sstDir
	}
	return nil
}
