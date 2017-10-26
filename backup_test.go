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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDump(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)

	// Write some stuff
	entries := []*Entry{
		{Key: []byte("answer1"), Value: []byte("42")},
		{Key: []byte("answer2"), Value: []byte("43"), UserMeta: 1},
	}

	require.NoError(t, kv.BatchSet(entries))

	bak, err := ioutil.TempFile(dir, "badgerbak")
	require.NoError(t, err)
	err = kv.Backup(bak)
	require.NoError(t, err)
	require.NoError(t, bak.Close())
	require.NoError(t, kv.Close())
}
