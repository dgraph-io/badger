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

package cmd

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/stretchr/testify/require"
)

func TestRotate(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	// Creating sample key.
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	fp, err := ioutil.TempFile("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	// Opening DB with the encryption key.
	opts := badger.DefaultOptions(dir)
	opts.EncryptionKey = key
	db, err := badger.Open(opts)
	require.NoError(t, err)
	// Closing the db.
	require.NoError(t, db.Close())

	// Opening the db again for the successful open.
	db, err = badger.Open(opts)
	require.NoError(t, err)
	// Closing so that we can open another db
	require.NoError(t, db.Close())

	// Creating another sample key.
	key2 := make([]byte, 32)
	_, err = rand.Read(key2)
	require.NoError(t, err)
	fp2, err := ioutil.TempFile("", "*.key")
	require.NoError(t, err)
	_, err = fp2.Write(key2)
	require.NoError(t, err)
	defer fp2.Close()
	oldKeyPath = fp2.Name()
	sstDir = dir

	// Check whether we able to rotate the key with some sample key. We should get mismatch
	// error.
	require.EqualError(t, doRotate(nil, []string{}), badger.ErrEncryptionKeyMismatch.Error())

	// rotating key with proper key.
	oldKeyPath = fp.Name()
	newKeyPath = fp2.Name()
	require.NoError(t, doRotate(nil, []string{}))

	// Checking whether db opens with the new key.
	opts.EncryptionKey = key2
	db, err = badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Checking for plain text rotation.
	oldKeyPath = newKeyPath
	newKeyPath = ""
	require.NoError(t, doRotate(nil, []string{}))
	opts.EncryptionKey = []byte{}
	db, err = badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()
}
