/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombineWithBothErrorsPresent(t *testing.T) {
	combinedError := CombineErrors(errors.New("one"), errors.New("two"))
	require.Equal(t, "one; two", combinedError.Error())
}

func TestCombineErrorsWithOneErrorPresent(t *testing.T) {
	combinedError := CombineErrors(errors.New("one"), nil)
	require.Equal(t, "one", combinedError.Error())
}

func TestCombineErrorsWithOtherErrorPresent(t *testing.T) {
	combinedError := CombineErrors(nil, errors.New("other"))
	require.Equal(t, "other", combinedError.Error())
}

func TestCombineErrorsWithBothErrorsAsNil(t *testing.T) {
	combinedError := CombineErrors(nil, nil)
	require.NoError(t, combinedError)
}

func TestWrapKeepsTheErrorChain(t *testing.T) {
	defer func(old bool) { debugMode = old }(debugMode)
	cause := errors.New("no space left on device")
	for _, mode := range []bool{false, true} {
		debugMode = mode
		require.ErrorIs(t, Wrap(cause, "while writing"), cause)
		require.ErrorIs(t, Wrapf(cause, "while writing file %d", 1), cause)
	}
}

// stackError prints a stack trace for %+v, as an error from github.com/pkg/errors does.
type stackError struct{}

func (stackError) Error() string { return "no space left on device" }

func (e stackError) Format(s fmt.State, verb rune) {
	_, _ = io.WriteString(s, e.Error())
	if verb == 'v' && s.Flag('+') {
		_, _ = io.WriteString(s, "\nstack trace")
	}
}

func TestWrapKeepsTheDetailedText(t *testing.T) {
	defer func(old bool) { debugMode = old }(debugMode)
	debugMode = false
	require.Equal(t, "while writing err: no space left on device\nstack trace",
		Wrap(stackError{}, "while writing").Error())
}
