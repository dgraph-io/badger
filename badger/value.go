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

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/y"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete             = 1 // Set if the key has been deleted.
	BitValuePointer       = 2 // Set if the value is NOT stored directly next to key.
	LogSize         int64 = 1 << 30
	M               int   = 1 << 20
)

var Corrupt error = errors.New("Unable to find log. Potential data corruption.")

type logFile struct {
	sync.RWMutex
	fd     *os.File
	fid    int32
	offset int64
}

func (lf *logFile) read(buf []byte, offset int64) error {
	lf.RLock()
	defer lf.RUnlock()

	_, err := lf.fd.ReadAt(buf, offset)
	return err
}

func (lf *logFile) doneWriting() {
	var err error
	lf.Lock()
	defer lf.Unlock()
	path := lf.fd.Name()
	y.Check(lf.fd.Close())
	lf.fd, err = os.OpenFile(path, os.O_RDONLY, 0666)
	y.Check(err)
}

type logEntry func(e Entry)

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (f *logFile) iterate(offset int64, fn logEntry) error {
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

type valueLog struct {
	sync.RWMutex
	files []logFile
	// fds    []*os.File
	offset    int64
	elog      trace.EventLog
	logPrefix string
	dirPath   string
	buf       bytes.Buffer
}

type Entry struct {
	Key    []byte
	Meta   byte
	Value  []byte
	Offset int64
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d Offset: %d len(val)=%d\n",
		prefix, e.Key, e.Meta, e.Offset, len(e.Value))
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

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint64
}

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 16)
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint64(b[8:16], uint64(p.Offset))
	return b[:16]
}

func (p *valuePointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 16)
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint64(b[8:16])
}

func (l *valueLog) fpath(fid int32) string {
	return fmt.Sprintf("%s/%s_%06d", l.dirPath, l.logPrefix, fid)
}

func (l *valueLog) openOrCreateFiles() {
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
			lf.fd, err = y.OpenSyncedFile(l.fpath(lf.fid))
			// lf.fd, err = os.OpenFile(l.fpath(lf.fid), os.O_RDWR|os.O_CREATE, 0666)
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

func (l *valueLog) Open(dir, prefix string) {
	l.logPrefix = prefix
	l.dirPath = dir
	l.openOrCreateFiles()

	l.elog = trace.NewEventLog("Badger", "Valuelog")
}

func (l *valueLog) Close() {
	for _, f := range l.files {
		f.fd.Close()
	}
	l.elog.Finish()
}

// Replay replays the value log. The kv provided is only valid for the lifetime of function call.
func (l *valueLog) Replay(ptr valuePointer, fn logEntry) {
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

type block struct {
	Entries []*Entry
	Ptrs    []valuePointer
	Wg      sync.WaitGroup
}

// Write is thread-unsafe by design and should not be called concurrently.
func (l *valueLog) Write(blocks []*block) {
	l.RLock()
	curlf := &l.files[len(l.files)-1]
	l.RUnlock()

	toDisk := func() {
		if l.buf.Len() == 0 {
			return
		}
		l.elog.Printf("Flushing %d blocks of total size: %d", len(blocks), l.buf.Len())
		n, err := curlf.fd.Write(l.buf.Bytes())
		if err != nil {
			y.Fatalf("Unable to write to value log: %v", err)
		}
		l.elog.Printf("Done")
		curlf.offset += int64(n)
		l.buf.Reset()
	}

	var h header
	headerEnc := make([]byte, 8)

	for i := range blocks {
		b := blocks[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]

			h.klen = uint32(len(e.Key))
			h.vlen = uint32(len(e.Value))
			h.Encode(headerEnc)

			var p valuePointer
			p.Fid = uint32(curlf.fid)
			p.Len = uint32(len(headerEnc)) + h.klen + h.vlen + 1
			p.Offset = uint64(curlf.offset) + uint64(l.buf.Len())
			b.Ptrs = append(b.Ptrs, p)

			l.buf.Write(headerEnc)
			l.buf.Write(e.Key)
			l.buf.WriteByte(e.Meta)
			l.buf.Write(e.Value)
		}
	}
	toDisk()

	// Acquire mutex locks around this manipulation, so that the reads don't try to use
	// an invalid file descriptor.
	if curlf.offset > LogSize {
		var err error
		curlf.doneWriting()

		newlf := logFile{fid: curlf.fid + 1, offset: 0}
		newlf.fd, err = y.OpenSyncedFile(l.fpath(newlf.fid))
		y.Check(err)

		l.Lock()
		l.files = append(l.files, newlf)
		l.Unlock()
	}
}

// Write batches the write of an array of entries to value log.
// func (l *Log) Write(entries []*Entry) ([]Pointer, error) {
// 	b := blockPool.Get().(*block)
// 	b.entries = entries
// 	b.wg = sync.WaitGroup{}
// 	b.wg.Add(1)
// 	l.bch <- b
// 	b.wg.Wait()
// 	defer blockPool.Put(b)
// 	return b.ptrs, nil
// }

func (l *valueLog) getFile(fid int32) (*logFile, error) {
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
func (l *valueLog) Read(ctx context.Context, p valuePointer) (e Entry, err error) {
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

func (l *valueLog) RunGC(getKey func(k []byte) ([]byte, byte, bool)) {
	var lf *logFile
	l.RLock()
	if len(l.files) <= 1 {
		fmt.Println("Need at least 2 value log files to run GC.")
		l.RUnlock()
		return
	}
	// This file shouldn't be being written to.
	lfi := rand.Intn(len(l.files) - 1)
	lf = &l.files[lfi]
	l.RUnlock()

	reason := make(map[string]int)
	err := lf.iterate(0, func(e Entry) {
		esz := len(e.Key) + len(e.Value) + 1
		vptr, meta, ok := getKey(e.Key)
		if !ok {
			return
		}

		if (meta & BitDelete) > 0 {
			// Key has been deleted. Discard.
			reason["discard"] += esz
			reason["discard-deleted"] += esz

		} else if (meta & BitValuePointer) == 0 {
			// Value is stored alongside key. Discard.
			reason["discard"] += esz
			reason["discard-value-alongside"] += esz

		} else {
			// Value is still present in value log.
			y.AssertTrue(len(vptr) > 0)
			var vp valuePointer
			vp.Decode(vptr)

			if int32(vp.Fid) > lf.fid {
				// Value is present in a later log. Discard.
				reason["discard"] += esz
				reason["discard-new-value-higher-fid"] += esz

			} else if int64(vp.Offset) > e.Offset {
				// Value is present in a later offset, but in the same log.
				reason["discard"] += esz
				reason["discard-new-value-higher-offset"] += esz

			} else if int32(vp.Fid) == lf.fid && int64(vp.Offset) == e.Offset {
				// This is still the active entry. This would need to be rewritten.
				reason["keep"] += esz

			} else {
				for k, v := range reason {
					fmt.Printf("%s = %d\n", k, v)
				}
				ne, err := l.Read(context.Background(), vp)
				y.Check(err)
				ne.Offset = int64(vp.Offset)
				ne.print("Latest Entry in LSM")
				e.print("Latest Entry in Log")
				y.Fatalf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.", vp, meta)
			}
		}
	})
	y.Checkf(err, "While iterating for RunGC.")
	for k, v := range reason {
		fmt.Printf("%s = %d\n", k, v)
	}
	fmt.Printf("\nFid: %d Keep: %d. Discard: %d\n", lfi, reason["keep"]/M, reason["discard"]/M)
}
