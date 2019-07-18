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

package y

import (
	"crypto/aes"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const keySuffix = ".key"

// IDToKeyName return key file name
func IDToKeyName(id uint64) string {
	return fmt.Sprintf("%06d", id) + keySuffix
}

// NewKeyName should be named KeyFilepath -- it combines the dir with the ID to make a key
// filepath.
func NewKeyName(id uint64, dir string) string {
	return filepath.Join(dir, IDToKeyName(id))
}

// GenereateIV generate IV.
func GenereateIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	_, err := io.ReadFull(rand.Reader, iv)
	if err != nil {
		return iv, err
	}
	return iv, nil
}

// GetDataKey decrypts encrypted datakey.
func GetDataKey(fd *os.File, storeKey []byte) ([]byte, error) {
	eKey, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}
	iv := eKey[:aes.BlockSize]
	if len(storeKey) < 0 {
		return eKey[aes.BlockSize:], nil
	}
	dataKey, err := XORBlock(storeKey, iv, eKey[aes.BlockSize:], 0)
	if err != nil {
		return nil, err
	}
	return dataKey, nil
}

// StoreDataKey encrypts and store datakey to the file.
func StoreDataKey(path string, storeKey []byte, dataKey []byte) error {
	fd, err := CreateSyncedFile(path, true)
	if err != nil {
		return err
	}
	defer fd.Close()
	iv, err := GenereateIV()
	if err != nil {
		return err
	}
	eDataKey, err := XORBlock(storeKey, iv, dataKey, 0)
	if err != nil {
		return err
	}
	_, err = fd.Write(iv)
	if err != nil {
		return err
	}
	_, err = fd.Write(eDataKey)
	if err != nil {
		return err
	}
	return nil
}
