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

package y

import (
	//	"fmt"
	"io/ioutil"
	"os"
	"runtime"
)

var EmptySlice = []byte{}

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	byteData   = 0
	byteDelete = 1
)

// ExtractValue extracts the value from v. If it is a deletion, we return nil.
func ExtractValue(v []byte) []byte {
	AssertTrue(len(v) >= 1)
	if v[0] == byteDelete {
		return nil
	}
	return v[1:]
}

// TempFile returns a tempfile in given directory. It sets a finalizer to delete the file.
// This is necessary especially if we use the RAM disk.
// If putting level 0, 1 on RAM gains us a lot, we will refactor to use a bigger skiplist instead.
func TempFile(dir string) (*os.File, error) {
	f, err := ioutil.TempFile(dir, "table_")
	if err != nil {
		return nil, err
	}
	// Note: A finalizer is not guaranteed to be run. We need to clean this up.
	runtime.SetFinalizer(f, func(f *os.File) {
		f.Close()
		os.Remove(f.Name()) // Might put on a channel to be deleted instead.
	})
	return f, nil
}
