/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var oldKeyPath string
var newKeyPath string
var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Rotate encryption key.",
	Long:  "Rotate will rotate the old key with new encryption key.",
	RunE:  doRotate,
}

func init() {
	RootCmd.AddCommand(rotateCmd)
	rotateCmd.Flags().StringVarP(&oldKeyPath, "old-key-path", "o",
		"", "Path of the old key")
	rotateCmd.Flags().StringVarP(&newKeyPath, "new-key-path", "n",
		"", "Path of the new key")
}

func doRotate(cmd *cobra.Command, args []string) error {
	oldKey, err := getKey(oldKeyPath)
	if err != nil {
		return err
	}
	opt := badger.KeyRegistryOptions{
		Dir:                           sstDir,
		ReadOnly:                      true,
		EncryptionKey:                 oldKey,
		EncryptionKeyRotationDuration: 10 * 24 * time.Hour,
	}
	kr, err := badger.OpenKeyRegistry(opt)
	if err != nil {
		return err
	}
	newKey, err := getKey(newKeyPath)
	if err != nil {
		return err
	}
	opt.EncryptionKey = newKey
	err = badger.WriteKeyRegistry(kr, opt)
	if err != nil {
		return err
	}
	return nil
}

func getKey(path string) ([]byte, error) {
	if path == "" {
		// Empty bytes for plain text to encryption(vice versa).
		return []byte{}, nil
	}
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(fp)
}
