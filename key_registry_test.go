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
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestBuildRegistry(t *testing.T) {
	storageKey := make([]byte, 32)
	path := fmt.Sprintf("%s%c%d", os.TempDir(), os.PathSeparator, rand.Int63())
	os.Mkdir(path, os.ModePerm)
	_, err := rand.Read(storageKey)
	y.Check(err)
	kr, err := openKeyRegistry(path, false, storageKey)
	require.NoError(t, err)
	dk, err := kr.getDataKey()
	require.NoError(t, err)
	kr.lastCreated = 0
	dk1, err := kr.getDataKey()
	require.NoError(t, err)
	kr.close()
	kr2, err := openKeyRegistry(path, false, storageKey)
	require.NoError(t, err)
	require.Equal(t, 2, len(kr2.dataKeys))
	require.Equal(t, dk.Data, kr.dataKeys[dk.KeyID].Data)
	require.Equal(t, dk1.Data, kr.dataKeys[dk1.KeyID].Data)
	kr.close()
	os.Remove(filepath.Join(path, keyRegistryFileName))
}

func TestRewriteRegistry(t *testing.T) {
	path := fmt.Sprintf("%s%c%d", os.TempDir(), os.PathSeparator, rand.Int63())
	os.Mkdir(path, os.ModePerm)
	storageKey := make([]byte, 32)
	_, err := rand.Read(storageKey)
	y.Check(err)
	kr, err := openKeyRegistry(path, false, storageKey)
	require.NoError(t, err)
	_, err = kr.getDataKey()
	require.NoError(t, err)
	kr.lastCreated = 0
	_, err = kr.getDataKey()
	require.NoError(t, err)
	require.NoError(t, err)
	kr.close()
	delete(kr.dataKeys, 1)
	rewriteRegistry(path, kr, storageKey)
	kr2, err := openKeyRegistry(path, false, storageKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(kr2.dataKeys))
	kr.close()
	os.Remove(filepath.Join(path, keyRegistryFileName))
}
