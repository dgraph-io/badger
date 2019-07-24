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

package cmd

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/require"
)

func TestRotate(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := badger.DefaultOptions(dir)
	key := make([]byte, 32)
	rand.Read(key)
	fp, err := ioutil.TempFile("", "*.key")
	require.NoError(t, err)
	fp.Write(key)
	defer fp.Close()
	opts.EncryptionKey = key
	db, err := badger.Open(opts)
	require.NoError(t, err)
	db.Close()
	key2 := make([]byte, 32)
	rand.Read(key2)
	fp2, err := ioutil.TempFile("", "*.key")
	require.NoError(t, err)
	fp2.Write(key2)
	defer fp2.Close()
	oldKeyPath = fp2.Name()
	sstDir = dir
	err = doRotate(nil, []string{})
	require.Equal(t, err, badger.ErrEncryptionKeyMismatch)
	oldKeyPath = fp.Name()
	newKeyPath = fp2.Name()
	err = doRotate(nil, []string{})
	require.NoError(t, err)
	opts.EncryptionKey = key2
	db.Close()
	db, err = badger.Open(opts)
	require.NoError(t, err)
	oldKeyPath = newKeyPath
	newKeyPath = ""
	err = doRotate(nil, []string{})
	opts.EncryptionKey = []byte{}
	db.Close()
	db, err = badger.Open(opts)
	require.NoError(t, err)
}
