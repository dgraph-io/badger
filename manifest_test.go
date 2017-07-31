/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestChange(t *testing.T) {
	var buf bytes.Buffer
	change := manifestChange{tableChange{33, tableCreate, 3, 66, []byte("bar"), []byte("foo")}}
	change.Encode(&buf)
	var newChange manifestChange
	err := newChange.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, change, newChange)
}
