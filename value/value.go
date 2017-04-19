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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/y"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete             = 1 // Set if the key has been deleted.
	BitValuePointer       = 2 // Set if the value is NOT stored directly next to key.
	LogSize         int64 = 1 << 30
	bufSize         int   = 4 << 20
)

var Corrupt error = errors.New("Unable to find log. Potential data corruption.")

type logFile struct {
	fd     *os.File
	fid    int32
	offset int64
}

type LogEntry func(e Entry)

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (f *logFile) iterate(offset int64, fn LogEntry) error {
	_, err := f.fd.Seek(offset, 0)
	y.Check(err)

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

	reader := bufio.NewReader(f.fd)
	hbuf := make([]byte, 8)
	var h header
	var count int
	k := make([]byte, 1<<10)
	v := make([]byte, 1<<20)
	var e Entry
	recordOffset := offset
	for {
		if err = read(reader, hbuf); err == io.EOF {
			break
		}
		h.Decode(hbuf)
		// fmt.Printf("[%d] Header read: %+v\n", count, h)

		kl := int(h.klen)
		vl := int(h.vlen)
		if cap(k) < kl {
			k = make([]byte, 2*kl)
		}
		if cap(v) < vl {
			v = make([]byte, 2*vl)
		}
		e.Offset = recordOffset
		e.Key = k[:kl]
		e.Value = v[:vl]

		if err = read(reader, e.Key); err != nil {
			return err
		}
		if e.Meta, err = reader.ReadByte(); err != nil {
			return err
		}
		if err = read(reader, e.Value); err != nil {
			return err
		}

		fn(e)
		count++
		recordOffset += int64(8 + kl + vl + 1)
	}
	return nil
}

type Log struct {
	sync.RWMutex
	files []logFile
	// fds    []*os.File
	offset    int64
	elog      trace.EventLog
	bch       chan *block
	done      sync.WaitGroup
	logPrefix string
	dirPath   string
}

type Entry struct {
	Key    []byte
	Meta   byte
	Value  []byte
	Offset int64
}

type header struct {
	klen uint32
	vlen uint32
}

func (h header) Encode(out []byte) {
	y.AssertTrue(len(out) >= 8)
	binary.BigEndian.PutUint32(out[0:4], h.klen)
	binary.BigEndian.PutUint32(out[4:8], h.vlen)
}

func (h *header) Decode(buf []byte) []byte {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	return buf[8:]
}

type Pointer struct {
	Fid    uint32
	Len    uint32
	Offset uint64
}

// Encode encodes Pointer into byte buffer.
func (p Pointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 16)
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint64(b[8:16], uint64(p.Offset))
	return b[:16]
}

func (p *Pointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 16)
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint64(b[8:16])
}

func (l *Log) fpath(fid int32) string {
	return fmt.Sprintf("%s/%s_%06d", l.dirPath, l.logPrefix, fid)
}

func (l *Log) openOrCreateFiles() {
	files, err := ioutil.ReadDir(l.dirPath)
	y.Check(err)

	found := make(map[int]struct{})
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), l.logPrefix+"-") {
			continue
		}
		fid, err := strconv.Atoi(file.Name()[len(l.logPrefix)+1:])
		y.Check(err)
		if _, ok := found[fid]; ok {
			y.Fatalf("Found the same file twice: %d", fid)
		}
		found[fid] = struct{}{}

		lf := logFile{fid: int32(fid)}
		l.files = append(l.files, lf)
	}

	sort.Slice(l.files, func(i, j int) bool {
		return l.files[i].fid < l.files[i].fid
	})

	// Open all previous log files as read only. Open the last log file
	// as read write.
	for i := range l.files {
		lf := &l.files[i]
		if i == len(l.files)-1 {
			lf.fd, err = os.OpenFile(l.fpath(lf.fid), os.O_RDWR|os.O_CREATE, 0666)
			y.Check(err)
		} else {
			lf.fd, err = os.OpenFile(l.fpath(lf.fid), os.O_RDONLY, 0666)
			y.Check(err)
		}
	}

	// If no files are found, then create a new file.
	if len(l.files) == 0 {
		lf := logFile{fid: 0}
		lf.fd, err = y.OpenSyncedFile(l.fpath(lf.fid))
		y.Check(err)
		l.files = append(l.files, lf)
	}
}

func (l *Log) Open(dir, prefix string) {
	l.logPrefix = prefix
	l.dirPath = dir
	l.openOrCreateFiles()

	l.elog = trace.NewEventLog("Badger", "Valuelog")
	l.bch = make(chan *block, 1000)
	l.done.Add(1)
	go l.writeToDisk()
}

func (l *Log) Close() {
	close(l.bch)
	l.done.Wait()
	for _, f := range l.files {
		f.fd.Close()
	}
	l.elog.Finish()
}

// Replay replays the value log. The kv provided is only valid for the lifetime of function call.
func (l *Log) Replay(ptr Pointer, fn LogEntry) {
	fid := int32(ptr.Fid)
	offset := int64(ptr.Offset)
	fmt.Printf("Seeking at value pointer: %+v\n", ptr)

	for _, f := range l.files {
		if f.fid < fid {
			continue
		}
		of := offset
		if f.fid > fid {
			of = 0
		}
		err := f.iterate(of, fn)
		y.Check(err)
	}

	// Seek to the end to start writing.
	var err error
	last := &l.files[len(l.files)-1]
	last.offset, err = last.fd.Seek(0, io.SeekEnd)
	y.Checkf(err, "Unable to seek to the end")
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return new(block)
	},
}

type block struct {
	entries []*Entry
	ptrs    []Pointer
	wg      sync.WaitGroup
}

func (l *Log) writeToDisk() {
	l.elog.Printf("Starting write to disk routine")
	buf := new(bytes.Buffer)
	buf.Grow(2 * bufSize)

	blocks := make([]*block, 0, 1000)
	curlf := &l.files[len(l.files)-1]

	toDisk := func() {
		if buf.Len() == 0 {
			return
		}
		if buf.Len() > bufSize {
			l.elog.Printf("Flushing buffer of size: %d", buf.Len())
			n, err := curlf.fd.Write(buf.Bytes())
			if err != nil {
				y.Fatalf("Unable to write to value log: %v", err)
			}
			curlf.offset += int64(n)
			buf.Reset()
		}
	}

	write := func() {
		l.elog.Printf("Buffering %d blocks", len(blocks))

		var h header
		headerEnc := make([]byte, 8)

		for i := range blocks {
			b := blocks[i]
			b.ptrs = b.ptrs[:0]
			for j := range b.entries {
				e := b.entries[j]

				h.klen = uint32(len(e.Key))
				h.vlen = uint32(len(e.Value))
				h.Encode(headerEnc)

				var p Pointer
				p.Fid = uint32(curlf.fid)
				p.Len = uint32(len(headerEnc)) + h.klen + h.vlen + 1
				p.Offset = uint64(curlf.offset) + uint64(buf.Len())
				b.ptrs = append(b.ptrs, p)

				buf.Write(headerEnc)
				buf.Write(e.Key)
				buf.WriteByte(e.Meta)
				buf.Write(e.Value)
				toDisk()
			}
		}
		toDisk()
		y.Check(curlf.fd.Sync())

		n, err := curlf.fd.Write(buf.Bytes())
		if err != nil {
			y.Fatalf("Unable to write to value log: %v", err)
		}
		buf.Reset()

		l.elog.Printf("Wrote %d bytes to disk", n)
		curlf.offset += int64(n)

		// Acquire mutex locks around this manipulation, so that the reads don't try to use
		// an invalid file descriptor.
		if curlf.offset > LogSize {
			l.Lock()
			y.Check(curlf.fd.Close())
			curlf.fd, err = os.OpenFile(l.fpath(curlf.fid), os.O_RDONLY, 0666)
			y.Check(err)

			newlf := logFile{fid: curlf.fid + 1, offset: 0}
			newlf.fd, err = y.OpenSyncedFile(l.fpath(newlf.fid))
			y.Check(err)
			l.files = append(l.files, newlf)

			curlf = &newlf
			l.Unlock()
		}

		for i := range blocks {
			b := blocks[i]
			b.wg.Done()
		}
	}

LOOP:
	for {
		select {
		case b, open := <-l.bch:
			if !open {
				write()
				break LOOP
			}
			blocks = append(blocks, b)

		default:
			if len(blocks) == 0 {
				time.Sleep(time.Millisecond)
				break
			}
			write()
			blocks = blocks[:0]
		}
	}
	l.done.Done()
}

// Write batches the write of an array of entries to value log.
func (l *Log) Write(entries []*Entry) ([]Pointer, error) {
	b := blockPool.Get().(*block)
	b.entries = entries
	b.wg = sync.WaitGroup{}
	b.wg.Add(1)
	l.bch <- b
	b.wg.Wait()
	defer blockPool.Put(b)
	return b.ptrs, nil
}

func (l *Log) getFile(fid int32) (*logFile, error) {
	l.RLock()
	defer l.RUnlock()

	idx := sort.Search(len(l.files), func(idx int) bool {
		return l.files[idx].fid >= fid
	})
	if idx == len(l.files) || l.files[idx].fid != fid {
		return nil, Corrupt
	}
	return &l.files[idx], nil
}

// Read reads the value log at a given location.
func (l *Log) Read(ctx context.Context, p Pointer) (e Entry, err error) {
	y.Trace(ctx, "Reading value with pointer: %+v", p)
	defer y.Trace(ctx, "Read done")

	lf, err := l.getFile(int32(p.Fid))
	if err != nil {
		return e, err
	}

	buf := make([]byte, p.Len)
	if _, err := lf.fd.ReadAt(buf, int64(p.Offset)); err != nil {
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

func (l *Log) RunGC(getKey func(k []byte) ([]byte, byte)) {
	var lf *logFile
	l.RLock()
	if len(l.files) <= 1 {
		return
	}
	// This file shouldn't be being written to.
	lf = &l.files[0]
	l.RUnlock()

	var discard, keep int
	err := lf.iterate(0, func(e Entry) {
		fmt.Printf("iterate. key: %s\n", e.Key)
		vptr, meta := getKey(e.Key)
		if (meta & BitValuePointer) == 0 {
			discard++
			fmt.Printf("got value stored along with key: %s\n", e.Key)
		} else {
			fmt.Printf("got vptr key: %s\n", e.Key)
			y.AssertTrue(len(vptr) > 0)
			var vp Pointer
			vp.Decode(vptr)
			if int32(vp.Fid) > lf.fid {
				discard++
			} else if int64(vp.Offset) > e.Offset {
				discard++
			} else if int32(vp.Fid) == lf.fid && int64(vp.Offset) == e.Offset {
				keep++
			} else {
				y.Fatalf("This shouldn't happen. Entry:%+v Latest Pointer:%+v", e, vp)
			}
		}
	})
	y.Checkf(err, "While iterating for RunGC.")
	fmt.Printf("Records to discard: %d. to keep: %d\n", discard, keep)
}
