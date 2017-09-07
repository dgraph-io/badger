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

	"github.com/pkg/errors"
)

// Constants used in serialization sizes, and in ValueStruct serialization
const (
	MetaSize     = 1
	UserMetaSize = 1
	CasSize      = 8
)

// ErrEOF indicates an end of file when trying to read from a memory mapped file
// and encountering the end of slice.
var ErrEOF = errors.New("End of mapped region")

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

// Closer holds the two things we need to close a goroutine and wait for it to finish: a chan
// to tell the goroutine to shut down, and a WaitGroup with which to wait for it to finish shutting
// down.
type Closer struct {
	closed  chan struct{}
	waiting sync.WaitGroup
}

func NewCloser(initial int) *Closer {
	ret := &Closer{closed: make(chan struct{}, 10)}
	ret.waiting.Add(initial)
	return ret
}

func (lc *Closer) AddRunning(delta int) {
	lc.waiting.Add(delta)
}

func (lc *Closer) Signal() {
	close(lc.closed)
}

func (lc *Closer) HasBeenClosed() <-chan struct{} {
	return lc.closed
}

func (lc *Closer) Done() {
	lc.waiting.Done()
}

func (lc *Closer) Wait() {
	lc.waiting.Wait()
}

func (lc *Closer) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}
