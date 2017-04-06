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

package value

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/x"
	"github.com/pkg/errors"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete      = 1
	BitValueOffset = 2
)

type Log struct {
	x.SafeMutex
	fd *os.File
}

type Entry struct {
	Key   []byte
	Meta  byte
	Value []byte
}

type header struct {
	klen uint32
	vlen uint32
}

func (h header) Encode() []byte {
	b := make([]byte, 4+4)
	binary.BigEndian.PutUint32(b[0:4], h.klen)
	binary.BigEndian.PutUint32(b[4:8], h.vlen)
	return b
}

func (h *header) Decode(buf []byte) []byte {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	return buf[8:]
}

type Pointer struct {
	Len    uint32
	Offset uint64
}

// Encode encodes Pointer into byte buffer. We don't return because this can avoid mem allocation.
func (p Pointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 12)
	binary.BigEndian.PutUint32(b[:4], p.Len)
	binary.BigEndian.PutUint64(b[4:12], uint64(p.Offset)) // Might want to use uint64 for Offset.
	return b[:12]
}

func (p *Pointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 12)
	p.Len = binary.BigEndian.Uint32(b[:4])
	p.Offset = binary.BigEndian.Uint64(b[4:12])
}

func (l *Log) Open(fname string) {
	var err error
	l.fd, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE|syscall.O_DSYNC, 0666)
	y.Check(err)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// Write batches the write of an array of entries to value log.
func (l *Log) Write(entries []Entry) ([]Pointer, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	var h header
	ptrs := make([]Pointer, 0, len(entries))

	for _, e := range entries {
		h.klen = uint32(len(e.Key))
		h.vlen = uint32(len(e.Value))
		header := h.Encode()

		var p Pointer
		p.Len = uint32(len(header)) + h.klen + h.vlen + 1
		p.Offset = uint64(buf.Len())
		ptrs = append(ptrs, p)

		buf.Write(header)
		buf.Write(e.Key)
		buf.WriteByte(e.Meta)
		buf.Write(e.Value)
	}

	l.Lock()
	defer l.Unlock()

	off, err := l.fd.Seek(0, io.SeekEnd)
	y.AssertTrue(off >= 0)
	if err != nil {
		return ptrs, errors.Wrap(err, "Unable to seek")
	}
	for i := range ptrs {
		p := &ptrs[i]
		p.Offset += uint64(off)
	}
	_, err = l.fd.Write(buf.Bytes())
	return ptrs, errors.Wrap(err, "Unable to write to file")
}

// Read reads the value log at a given location.
func (l *Log) Read(p Pointer, fn func(Entry)) error {
	var e Entry
	buf := make([]byte, p.Len)
	if _, err := l.fd.ReadAt(buf, int64(p.Offset)); err != nil {
		return err
	}
	var h header
	buf = h.Decode(buf)
	e.Key = buf[0:h.klen]
	e.Meta = buf[h.klen]
	buf = buf[h.klen+1:]
	e.Value = buf[0:h.vlen]
	fn(e)
	return nil
}
