/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottleDoneWithPendingError(t *testing.T) {
	first := errors.New("first worker failed")
	second := errors.New("second worker failed")
	for attempt := 0; attempt < 32; attempt++ {
		th := NewThrottle(1)
		require.NoError(t, th.Do())
		th.Done(first)
		// Do may observe either the available slot or the buffered error. Exercise
		// the valid interleaving where it starts more work before observing error.
		if err := th.Do(); err != nil {
			require.ErrorIs(t, err, first)
			_ = th.Finish()
			continue
		}
		done := make(chan struct{})
		go func() {
			th.Done(second)
			close(done)
		}()
		select {
		case <-done:
			require.Error(t, th.Finish())
		case <-time.After(time.Second):
			// Release the buggy worker through the public API before failing.
			require.ErrorIs(t, th.Do(), first)
			<-done
			_ = th.Finish()
			t.Fatal("Done blocked behind an earlier error; Finish would deadlock")
		}
		return
	}
	t.Fatal("did not exercise scheduling work while an error was pending")
}
