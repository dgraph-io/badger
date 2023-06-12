/*
 * Copyright 2023 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"testing"
	"time"
)

// waitForMessage(ch, expected, count, timeout, t) will block until either
// `timeout` seconds have occurred or `count` instances of the string `expected`
// have occurred on the channel `ch`. We log messages or generate errors using `t`.
func waitForMessage(ch chan string, expected string, count int, timeout int, t *testing.T) {
	if count <= 0 {
		t.Logf("Will skip waiting for %s since exected count <= 0.",
			expected)
		return
	}
	tout := time.NewTimer(time.Duration(timeout) * time.Second)
	remaining := count
	for {
		select {
		case curMsg, ok := <-ch:
			if !ok {
				t.Errorf("Test channel closed while waiting for "+
					"message %s with %d remaining instances expected",
					expected, remaining)
				return
			}
			t.Logf("Found message: %s", curMsg)
			if curMsg == expected {
				remaining--
				if remaining == 0 {
					return
				}
			}
		case <-tout.C:
			t.Errorf("Timed out after %d seconds while waiting on test chan "+
				"for message '%s' with %d remaining instances expected",
				timeout, expected, remaining)
			return
		}
	}
}
