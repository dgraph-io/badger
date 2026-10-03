/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"errors"
	"fmt"
	"os"

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
	for c := cmd; c != nil; c = c.Parent() {
		if c.Name() == "help" || c.Name() == "completion" {
			return nil
		}
	}
	if sstDir == "" {
		return errors.New("--dir not specified")
	}
	if vlogDir == "" {
		vlogDir = sstDir
	}
	return nil
}
