/*
* Copyright 2019 Dgraph Labs, Inc. and Contributors
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

	"github.com/stretchr/testify/require"
)

func TestGetMergeOperator(t *testing.T) {
	// Merge function to merge two byte slices
	add := func(originalValue, newValue []byte) []byte {
		// We append original value to new value because the values
		// are retrieved in reverse order (Last insertion will be the first value)
		return append(newValue, originalValue...)
	}
	t.Run("get should return error", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			m := db.GetMergeOperator([]byte("foo"), add, 200*time.Millisecond)
			defer m.Stop()

			value, err := m.Get()
			// MergeOperator should return key not found error
			require.Error(t, err)
			require.Nil(t, value)
		})
	})
	t.Run("add and get", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			m := db.GetMergeOperator([]byte("fooprefix"), add, 2*time.Millisecond)
			defer m.Stop()

			require.Nil(t, m.Add([]byte("1")))
			require.Nil(t, m.Add([]byte("2")))
			require.Nil(t, m.Add([]byte("3")))

			value, err := m.Get()
			require.Nil(t, err)
			require.Equal(t, "123", string(value))
		})
	})
}
