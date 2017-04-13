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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/x"
	"github.com/pkg/errors"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete       = 1 // Set if the key has been deleted.
	BitValuePointer = 2 // Set if the value is NOT stored directly next to key.
)

type Log struct {
	x.SafeMutex
	fd     *os.File
	offset int64
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

// Encode encodes Pointer into byte buffer.
func (p Pointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 12)
	binary.BigEndian.PutUint32(b[:4], p.Len)
	binary.BigEndian.PutUint64(b[4:12], uint64(p.Offset))
	return b[:12]
}

func (p *Pointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 12)
	p.Len = binary.BigEndian.Uint32(b[:4])
	p.Offset = binary.BigEndian.Uint64(b[4:12])
}

func (l *Log) Open(fname string) {
	var err error
	l.fd, err = y.OpenSyncedFile(fname)
	y.Check(err)
}

func (l *Log) Replay(offset uint64, fn func(k, v []byte, meta byte)) {
	fmt.Printf("Seeking at offset: %v\n", offset)

	read := func(r *bufio.Reader, buf []byte) error {
		for {
			n, err := r.Read(buf)
			if err != nil {
				return err
			}
			if n == len(buf) {
				return nil
			}
			buf = buf[n:]
		}
	}

	_, err := l.fd.Seek(int64(offset), 0)
	y.Check(err)
	reader := bufio.NewReader(l.fd)

	hbuf := make([]byte, 8)
	var h header
	var count int
	for {
		if err := read(reader, hbuf); err == io.EOF {
			break
		}
		h.Decode(hbuf)
		// fmt.Printf("[%d] Header read: %+v\n", count, h)

		k := make([]byte, h.klen)
		v := make([]byte, h.vlen)

		y.Check(read(reader, k))
		meta, err := reader.ReadByte()
		y.Check(err)
		y.Check(read(reader, v))

		fn(k, v, meta)
		count++
	}
	fmt.Printf("Replayed %d KVs\n", count)

	// Seek to the end to start writing.
	l.offset, err := l.fd.Seek(0, io.SeekEnd)
	if err != nil {
		y.Fatalf("Unable to seek to the end. Error: %v", err)
	}
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

	for i := range ptrs {
		p := &ptrs[i]
		p.Offset += uint64(l.offset)
	}
	n, err = l.fd.Write(buf.Bytes())
	l.offset += n
	return ptrs, errors.Wrap(err, "Unable to write to file")
}

// Read reads the value log at a given location.
func (l *Log) Read(ctx context.Context, p Pointer) (e Entry, err error) {
	y.Trace(ctx, "Reading value with pointer: %+v", p)
	defer y.Trace(ctx, "Read done")

	buf := make([]byte, p.Len)
	if _, err := l.fd.ReadAt(buf, int64(p.Offset)); err != nil {
		return e, err
	}
	var h header
	buf = h.Decode(buf)
	e.Key = buf[0:h.klen]
	e.Meta = buf[h.klen]
	buf = buf[h.klen+1:]
	e.Value = buf[0:h.vlen]
	return e, nil
}
