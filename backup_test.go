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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger/protos"
	"github.com/stretchr/testify/require"
)

func LoadBackupData(t *testing.T, kv *KV, limit int) {
	entries := make([]*Entry, 0, limit)
	for i := 0; i < limit; i++ {
		entries = append(entries, &Entry{
			Key:      []byte(fmt.Sprintf("key%09d", i)),
			Value:    []byte(fmt.Sprintf("val%d", i)),
			UserMeta: uint8(i),
		})
	}
	err := kv.BatchSet(entries)
	require.NoError(t, err)

	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}
}

func TestBasicBackup(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	LoadBackupData(t, kv, 100)

	i := 0
	// Now stream a backup
	kv.StreamBackup(0, func(item protos.BackupItem) error {
		require.Equal(t, fmt.Sprintf("key%09d", i), string(item.Key))
		require.True(t, item.HasValue)
		require.Equal(t, fmt.Sprintf("val%d", i), string(item.Value))
		require.Equal(t, uint32(uint8(i)), item.UserMeta)
		i++
		return nil
	})
	require.Equal(t, 100, i)
}
