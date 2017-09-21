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

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/protos"
	"github.com/stretchr/testify/require"
)

func getTestOptions(dir string) *badger.Options {
	opt := new(badger.Options)
	*opt = badger.DefaultOptions
	opt.MaxTableSize = 1 << 15 // Force more compaction.
	opt.LevelOneSize = 4 << 15 // Force more compaction.
	opt.Dir = dir
	opt.ValueDir = dir
	opt.SyncWrites = true // Some tests seem to need this to pass.
	return opt
}

func LoadBackupData(t *testing.T, kv *badger.KV, limit int) {
	entries := make([]*badger.Entry, 0, limit)
	for i := 0; i < limit; i++ {
		entries = append(entries, &badger.Entry{
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
	kv, err := badger.NewKV(getTestOptions(dir))
	require.NoError(t, err)
	defer kv.Close()

	LoadBackupData(t, kv, 100)

	i := 0
	// Now stream a backup
	stream, err := kv.NewBackupStream(0)
	require.NoError(t, err)
	err = stream.StreamAndClose(func(item protos.BackupItem) error {
		require.Equal(t, fmt.Sprintf("key%09d", i), string(item.Key))
		require.True(t, item.HasValue)
		require.Equal(t, fmt.Sprintf("val%d", i), string(item.Value))
		require.Equal(t, uint32(uint8(i)), item.UserMeta)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 100, i)
}

func TestBackupStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	storeDir := filepath.Join(dir, "store")

	err = badger.CreateBackupStore(storeDir)
	require.NoError(t, err)

	status, err := badger.ReadBackupStatus(storeDir)
	require.NoError(t, err)
	require.Empty(t, status.Backups)

	// Perform two backups and test that restoration omits deletions.
	const count = 100
	i := 0
	err = badger.NewBackup(storeDir, 2*count, func() ([]protos.BackupItem, error) {
		if i == count {
			return nil, nil
		}
		ret := []protos.BackupItem{
			{
				Key:      []byte(fmt.Sprintf("key%09d-A", i)),
				HasValue: true,
				Value:    []byte(fmt.Sprintf("value%d", i)),
				UserMeta: uint32(uint8(i)),
				Counter:  uint64(2*i + 1),
			},
			{
				Key:      []byte(fmt.Sprintf("key%09d-B", i)),
				HasValue: true,
				Value:    []byte(fmt.Sprintf("val%d", i)),
				UserMeta: 0,
				Counter:  uint64(2*i + 2),
			},
		}
		i++
		return ret, nil
	})
	require.NoError(t, err)
	i = 0
	err = badger.NewBackup(storeDir, 3*count, func() ([]protos.BackupItem, error) {
		if i == count {
			return nil, nil
		}
		ret := []protos.BackupItem{
			{
				Key:      []byte(fmt.Sprintf("key%09d-B", i)),
				HasValue: false,
				Value:    nil,
				UserMeta: 0,
				Counter:  uint64(2*count + i + 1),
			},
		}
		i++
		return ret, nil
	})

	i = 0
	err = RetrieveBackup(storeDir, func(item protos.BackupItem) error {
		require.True(t, i < count)
		require.Equal(t, fmt.Sprintf("key%09d-A", i), string(item.Key))
		require.True(t, item.HasValue)
		require.Equal(t, uint64(2*i+1), item.Counter)
		require.Equal(t, uint32(uint8(i)), item.UserMeta)
		require.Equal(t, fmt.Sprintf("value%d", i), string(item.Value))
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, count, i)
}
