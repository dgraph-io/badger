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
	"hash/crc32"
	"os"
	"sync"
)

// Constants used in serialization sizes, and in ValueStruct serialization
const (
	MetaSize     = 1
	UserMetaSize = 1
	CasSize      = 8
)

var (
	// This is O_DSYNC (datasync) on platforms that support it -- see file_unix.go
	datasyncFileFlag = 0x0

	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

// OpenExistingSyncedFile opens an existing file, errors if it doesn't exist.
func OpenExistingSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0)
}

// CreateSyncedFile creates a new file (using O_EXCL), errors if it already existed.
func CreateSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

// OpenSyncedFile creates the file if one doesn't exist.
func OpenSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

func OpenTruncFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if sync {
		flags |= datasyncFileFlag
	}
	return os.OpenFile(filename, flags, 0666)
}

func Safecopy(a []byte, src []byte) []byte {
	if cap(a) < len(src) {
		a = make([]byte, len(src))
	}
	a = a[:len(src)]
	copy(a, src)
	return a
}

type Slice struct {
	buf []byte
}

func (s *Slice) Resize(sz int) []byte {
	if cap(s.buf) < sz {
		s.buf = make([]byte, sz)
	}
	return s.buf[0:sz]
}

type LevelCloser struct {
	closed  chan struct{}
	waiting sync.WaitGroup
}

func NewLevelCloser(initial int32) *LevelCloser {
	ret := &LevelCloser{closed: make(chan struct{}, 10)}
	ret.waiting.Add(int(initial))
	return ret
}

func (lc *LevelCloser) AddRunning(delta int32) {
	lc.waiting.Add(int(delta))
}

func (lc *LevelCloser) Signal() {
	close(lc.closed)
}

func (lc *LevelCloser) HasBeenClosed() <-chan struct{} {
	return lc.closed
}

func (lc *LevelCloser) Done() {
	lc.waiting.Done()
}

func (lc *LevelCloser) Wait() {
	lc.waiting.Wait()
}

func (lc *LevelCloser) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}
