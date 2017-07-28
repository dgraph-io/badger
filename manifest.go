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

import "os"
import "github.com/dgraph-io/badger/y"

// The MANIFEST file describes the startup state of the db -- all value log files and LSM files,
// and their sizes (except the last value log file), what level they're at, key range info for each
// LSM file.
type manifest struct {
}

// manifestFile holds the file pointer (and other info) about the manifest file, which is a log
// file we append to.
type manifestFile struct {
	fp *os.File
}

func openManifestFile(path string) (ret *manifestFile, err error) {
	ret.fp, err = y.OpenSyncedFile(path, true)
	return
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}
