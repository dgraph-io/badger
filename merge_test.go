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
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("__key"), []byte("0000"), 0x00)
		// Merge function to merge two byte slices
		add := func(existing, new []byte) []byte {
			return append(existing, new...)
		}

		m := db.GetMergeOperator([]byte("__"), add, 200*time.Millisecond)
		defer m.Stop()

		m.Add([]byte("foo"))

		res, err := m.Get()
		require.Nil(t, err)
		require.Equal(t, []byte("foo0000"), res)
	})
}
