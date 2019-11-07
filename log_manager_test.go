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
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/stretchr/testify/require"
)

func getMockDB(t *testing.T, dir string) *DB {
	opt := getTestOptions(dir)
	encryptionKey := make([]byte, 32)
	_, err := rand.Read(encryptionKey)
	require.NoError(t, err)
	db := &DB{
		opt: opt,
	}
	kro := getRegistryTestOptions(db.opt.Dir, encryptionKey)
	kr, err := OpenKeyRegistry(kro)
	require.NoError(t, err)
	db.registry = kr
	return db
}
func TestLogManagerWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	db := getMockDB(t, dir)
	manager, err := openLogManager(db, valuePointer{}, valuePointer{}, db.replayFunction())
	require.NoError(t, err)
	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		v0 = []byte("value0-012345678901234567890123012345678901234567890123")
		v1 = []byte("value1-012345678901234567890123012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123012345678901234567890123")
	)
	reqs := []*request{
		&request{
			Entries: []*Entry{{Key: k0, Value: v0, forceWal: true},
				{Key: k1, Value: v1},
				{Key: k2, Value: v2}},
		},
	}
	require.NoError(t, manager.write(reqs))
	// check value pointer updated or not.
	require.Equal(t, len(reqs[0].Ptrs), 3)
	// read value and check.
	s := new(y.Slice)
	val, cb, err := manager.Read(reqs[0].Ptrs[1], s)
	require.NoError(t, err)
	runCallback(cb)
	require.Equal(t, val, v1)

	// let's rotate one log and do the same.
	require.NoError(t, manager.rotateLog())
	reqs[0].Ptrs = []valuePointer{}
	require.NoError(t, manager.write(reqs))

	// check max id incremented or not.
	require.Equal(t, manager.maxLogID, uint32(2))

	// read value and check.
	s = new(y.Slice)
	val, cb, err = manager.Read(reqs[0].Ptrs[1], s)
	require.NoError(t, err)
	runCallback(cb)
	require.Equal(t, val, v1)
}
