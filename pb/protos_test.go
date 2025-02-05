/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package pb

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func Exec(argv ...string) error {
	cmd := exec.Command(argv[0], argv[1:]...)

	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Wait()
}

func TestProtosRegenerate(t *testing.T) {
	err := Exec("./gen.sh")
	require.NoError(t, err, "Got error while regenerating protos: %v\n", err)

	generatedProtos := "badgerpb4.pb.go"
	err = Exec("git", "diff", "--quiet", "--", generatedProtos)
	require.NoError(t, err, "badgerpb4.pb.go changed after regenerating")
}
