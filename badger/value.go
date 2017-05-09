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
	"sync/atomic"
	"time"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/y"

	"github.com/bkaradzic/go-lz4"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete       byte  = 1 // Set if the key has been deleted.
	BitValuePointer byte  = 2 // Set if the value is NOT stored directly next to key.
	BitCompressed   byte  = 4 // Set if the key value pair is stored compressed.
	LogSize         int64 = 1 << 30
	M               int   = 1 << 20
)

var Corrupt error = errors.New("Unable to find log. Potential data corruption.")

type logFile struct {
	sync.RWMutex
	path   string
	fd     *os.File
	fid    int32
	offset int64
	size   int64
}

// openReadOnly assumes that we have a write lock on logFile.
func (lf *logFile) openReadOnly() {
	var err error
	lf.fd, err = os.OpenFile(lf.path, os.O_RDONLY, 0666)
	y.Check(err)

	fi, err := lf.fd.Stat()
	y.Check(err)
	lf.size = fi.Size()
}

func (lf *logFile) read(buf []byte, offset int64) error {
	lf.RLock()
	defer lf.RUnlock()

	_, err := lf.fd.ReadAt(buf, offset)
	return err
}

func (lf *logFile) doneWriting() {
	lf.Lock()
	defer lf.Unlock()
	y.Check(lf.fd.Close())

	lf.openReadOnly()
}

type logEntry func(e Entry) bool

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
	var hbuf [13]byte
	var h header
	var count int
	k := make([]byte, 1<<10)
	v := make([]byte, 1<<20)
	decompressed := make([]byte, 1<<20)

	var e Entry
	var hlen int
	recordOffset := offset
	for {
		if err = read(reader, hbuf[:]); err == io.EOF {
			break
		}

		e.Offset = recordOffset
		_, hlen = h.Decode(hbuf[:])
		// fmt.Printf("[%d] Header read: %+v\n", count, h)
		vl := int(h.vlen)
		if cap(v) < vl {
			v = make([]byte, 2*vl)
		}

		if h.meta&BitCompressed > 0 { // entry is compressed
			if err = read(reader, v[:vl]); err != nil {
				return err
			}
			decompressed, err = lz4.Decode(decompressed, v[:vl])

			e.Meta = h.meta
			e.casCounter = h.casCounter
			e.CASCounterCheck = h.casCounterCheck
			e.Key = decompressed[:h.klen]
			e.Value = decompressed[h.klen:]

			recordOffset += int64(hlen + vl)
		} else {
			kl := int(h.klen)
			if cap(k) < kl {
				k = make([]byte, 2*kl)
			}
			e.Key = k[:kl]
			e.Value = v[:vl]

			if err = read(reader, e.Key); err != nil {
				return err
			}
			e.Meta = h.meta
			e.casCounter = h.casCounter
			e.CASCounterCheck = h.casCounterCheck
			if err = read(reader, e.Value); err != nil {
				return err
			}

			recordOffset += int64(hlen + kl + vl)
		}

		if !fn(e) {
			break
		}
		count++
	}
	return nil
}

var entries = make([]*Entry, 0, 1000000)

func (vlog *valueLog) rewrite(f *logFile) {
	maxFid := atomic.LoadInt32(&vlog.maxFid)
	y.AssertTruef(f.fid < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)

	elog := trace.NewEventLog("badger", "vlog-rewrite")
	defer elog.Finish()
	elog.Printf("Rewriting fid: %d", f.fid)
	fmt.Println("rewrite called")

	entries = entries[:0]
	y.AssertTrue(vlog.kv != nil)
	var count int
	fe := func(e Entry) {
		count++
		if count%10000 == 0 {
			elog.Printf("Processing entry %d", count)
		}

		vs := vlog.kv.get(e.Key)
		if (vs.Meta & BitDelete) > 0 {
			return
		}
		if (vs.Meta & BitValuePointer) == 0 {
			return
		}

		// Value is still present in value log.
		y.AssertTrue(len(vs.Value) > 0)
		var vp valuePointer
		vp.Decode(vs.Value)

		if int32(vp.Fid) > f.fid {
			return
		}
		if int64(vp.Offset) > e.Offset {
			return
		}
		if int32(vp.Fid) == f.fid && int64(vp.Offset) == e.Offset {
			// This new entry only contains the key, and a pointer to the value.
			var ne Entry
			y.AssertTruef(e.Meta&^BitCompressed == 0, "Got meta: %v", e.Meta)
			ne.Meta = e.Meta & (^BitCompressed)
			ne.Key = make([]byte, len(e.Key))
			copy(ne.Key, e.Key)
			ne.Value = make([]byte, len(e.Value))
			copy(ne.Value, e.Value)
			ne.CASCounterCheck = vs.CASCounter // CAS counter check. Do not rewrite if key has a newer value.
			entries = append(entries, &ne)

		} else {
			// This can now happen because we can move some entries forward, but then not write
			// them to LSM tree due to CAS check failure.
			// y.Fatalf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.", vp, vs.Meta)
		}
	}

	f.iterate(0, func(e Entry) bool {
		fe(e)
		return true
	})
	elog.Printf("Processed %d entries in total", count)
	// Sort the entries, so lookups can potentially use page cache better.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	vlog.writeToKV(elog)

	elog.Printf("Removing fid: %d", f.fid)
	// Entries written to LSM. Remove the older file now.
	{
		vlog.RLock()
		idx := sort.Search(len(vlog.files), func(idx int) bool {
			return vlog.files[idx].fid >= f.fid
		})
		if idx == len(vlog.files) || vlog.files[idx].fid != f.fid {
			y.Fatalf("Unable to find fid: %d", f.fid)
		}
		vlog.files = append(vlog.files[:idx], vlog.files[idx+1:]...)
		vlog.RUnlock()
	}

	rem := vlog.fpath(f.fid)
	elog.Printf("Removing %s", rem)
	y.Check(os.Remove(rem))
}

func (vlog *valueLog) writeToKV(elog trace.EventLog) {
	req := &request{
		Wg:      sync.WaitGroup{},
		Entries: make([]*Entry, 0, 1000),
	}
	requests := make([]*request, 0, 10)
	requests = append(requests, req)
	for _, e := range entries {
		req.Entries = append(req.Entries, e)
		if len(req.Entries) >= 1000 {
			req = &request{
				Wg:      sync.WaitGroup{},
				Entries: make([]*Entry, 0, 1000),
			}
			requests = append(requests, req)
		}
	}
	for i, b := range requests {
		elog.Printf("req %d has %d entries", i, len(b.Entries))
		fmt.Printf("req %d has %d entries\n", i, len(b.Entries))
		b.Wg.Add(1)
		y.AssertTrue(len(b.Entries) > 0)
		vlog.kv.writeCh <- b // Write out these blocks with newer value offsets.
	}
	for i, b := range requests {
		elog.Printf("req %d done", i)
		fmt.Printf("req %d done\n", i)
		b.Wg.Wait()
	}
}

type Entry struct {
	Key             []byte
	Meta            byte
	Value           []byte
	Offset          int64
	CASCounterCheck uint16 // If nonzero, we will check if existing casCounter matches.

	// Fields maintained internally.
	casCounter uint16
}

type entryEncoder struct {
	opt          Options
	decompressed *bytes.Buffer // buffer for data prepared for compression
	compressed   []byte
}

// Encodes e to buf either plain or compressed.
// Returns number of bytes written.
func (enc *entryEncoder) Encode(e *Entry, buf *bytes.Buffer) int {
	var headerEnc [13]byte
	var h header

	if enc.opt.ValueCompressionMinSize < len(e.Key)+len(e.Value) {
		var err error

		enc.decompressed.Reset()
		enc.decompressed.Write(e.Key)
		enc.decompressed.Write(e.Value)

		enc.compressed, err = lz4.Encode(enc.compressed, enc.decompressed.Bytes())

		if err != nil {
			// Unable to compress value
			y.Fatalf("Error while compressing file: %s", err.Error())
		}
		compressionRatio := float64(enc.decompressed.Len()) / float64(len(enc.compressed))
		if compressionRatio >= enc.opt.ValueCompressionMinRatio {
			h.klen = uint32(len(e.Key))
			h.vlen = uint32(len(enc.compressed))
			h.meta = e.Meta | BitCompressed
			h.casCounter = e.casCounter
			h.casCounterCheck = e.CASCounterCheck
			h.Encode(headerEnc[:])

			buf.Write(headerEnc[:])
			buf.Write(enc.compressed)
			return len(headerEnc) + len(enc.compressed)
		}
	}

	h.klen = uint32(len(e.Key))
	h.vlen = uint32(len(e.Value))
	h.meta = e.Meta
	h.casCounter = e.casCounter
	h.casCounterCheck = e.CASCounterCheck
	h.Encode(headerEnc[:])

	buf.Write(headerEnc[:])
	buf.Write(e.Key)
	buf.Write(e.Value)
	return len(headerEnc) + len(e.Key) + len(e.Value)
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d Offset: %d len(val)=%d cas=%d check=%d\n",
		prefix, e.Key, e.Meta, e.Offset, len(e.Value), e.casCounter, e.CASCounterCheck)
}

type header struct {
	klen            uint32
	vlen            uint32 // len of value or length of compressed kv if entry storessed compressed
	meta            byte
	casCounter      uint16
	casCounterCheck uint16
}

func (h header) Encode(out []byte) {
	y.AssertTrue(len(out) >= 13)
	binary.BigEndian.PutUint32(out[0:4], h.klen)
	binary.BigEndian.PutUint32(out[4:8], h.vlen)
	out[8] = h.meta
	binary.BigEndian.PutUint16(out[9:11], h.casCounter)
	binary.BigEndian.PutUint16(out[11:13], h.casCounterCheck)
}

// Decodes h from buf. Returns buf without header and number of bytes read.
func (h *header) Decode(buf []byte) ([]byte, int) {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	h.meta = buf[8]
	h.casCounter = binary.BigEndian.Uint16(buf[9:11])
	h.casCounterCheck = binary.BigEndian.Uint16(buf[11:13])
	return buf[13:], 13
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

type valueLog struct {
	sync.RWMutex
	buf     bytes.Buffer
	dirPath string
	elog    trace.EventLog
	files   []*logFile
	kv      *KV
	maxFid  int32
	offset  int64
	opt     Options
}

func (l *valueLog) fpath(fid int32) string {
	return fmt.Sprintf("%s/%06d.vlog", l.dirPath, fid)
}

func (l *valueLog) openOrCreateFiles() {
	files, err := ioutil.ReadDir(l.dirPath)
	y.Check(err)

	found := make(map[int]struct{})
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.Atoi(file.Name()[:fsz-5])
		y.Check(err)
		if _, ok := found[fid]; ok {
			y.Fatalf("Found the same file twice: %d", fid)
		}
		found[fid] = struct{}{}

		lf := &logFile{fid: int32(fid), path: l.fpath(int32(fid))}
		l.files = append(l.files, lf)
	}

	sort.Slice(l.files, func(i, j int) bool {
		return l.files[i].fid < l.files[i].fid
	})

	// Open all previous log files as read only. Open the last log file
	// as read write.
	for i := range l.files {
		lf := l.files[i]
		if i == len(l.files)-1 {
			lf.fd, err = y.OpenSyncedFile(l.fpath(lf.fid), l.opt.SyncWrites)
			y.Check(err)
			l.maxFid = lf.fid

		} else {
			lf.openReadOnly()
		}
	}

	// If no files are found, then create a new file.
	if len(l.files) == 0 {
		lf := &logFile{fid: 0, path: l.fpath(0)}
		lf.fd, err = y.OpenSyncedFile(l.fpath(lf.fid), l.opt.SyncWrites)
		y.Check(err)
		l.files = append(l.files, lf)
	}
}

func (l *valueLog) Open(kv *KV, opt *Options) {
	l.dirPath = opt.Dir
	l.openOrCreateFiles()
	l.opt = *opt
	l.kv = kv

	l.elog = trace.NewEventLog("Badger", "Valuelog")
}

func (l *valueLog) Close() {
	l.elog.Printf("Stopping garbage collection of values.")
	for _, f := range l.files {
		y.Check(f.fd.Close())
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
	last := l.files[len(l.files)-1]
	last.offset, err = last.fd.Seek(0, io.SeekEnd)
	y.Checkf(err, "Unable to seek to the end")
}

type request struct {
	Entries []*Entry
	Ptrs    []valuePointer
	Wg      sync.WaitGroup
}

// Write is thread-unsafe by design and should not be called concurrently.
func (l *valueLog) Write(reqs []*request) {
	l.RLock()
	curlf := l.files[len(l.files)-1]
	l.RUnlock()

	toDisk := func() {
		if l.buf.Len() == 0 {
			return
		}
		l.elog.Printf("Flushing %d blocks of total size: %d", len(reqs), l.buf.Len())
		n, err := curlf.fd.Write(l.buf.Bytes())
		if err != nil {
			y.Fatalf("Unable to write to value log: %v", err)
		}
		l.elog.Printf("Done")
		curlf.offset += int64(n)
		l.buf.Reset()

		if curlf.offset > LogSize {
			var err error
			curlf.doneWriting()

			newlf := &logFile{fid: atomic.AddInt32(&l.maxFid, 1), offset: 0}
			newlf.path = l.fpath(newlf.fid)
			newlf.fd, err = y.OpenSyncedFile(newlf.path, l.opt.SyncWrites)
			y.Check(err)

			l.Lock()
			l.files = append(l.files, newlf)
			l.Unlock()
			curlf = newlf
		}
	}

	entryEncoder := &entryEncoder{
		opt:          l.opt,
		decompressed: bytes.NewBuffer(make([]byte, 1<<20)),
		compressed:   make([]byte, 1<<20),
	}

	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]
			y.AssertTruef(e.Meta&BitCompressed == 0, "Cannot set BitCompressed outside valueLog")
			var p valuePointer

			if !l.opt.SyncWrites && len(e.Value) < l.opt.ValueThreshold {
				// No need to write to value log.
				b.Ptrs = append(b.Ptrs, p)
				continue
			}

			p.Fid = uint32(curlf.fid)
			p.Offset = uint64(curlf.offset) + uint64(l.buf.Len())
			p.Len = uint32(entryEncoder.Encode(e, &l.buf))
			b.Ptrs = append(b.Ptrs, p)

			if p.Offset > uint64(LogSize) {
				toDisk()
			}
		}
	}
	toDisk()

	// Acquire mutex locks around this manipulation, so that the reads don't try to use
	// an invalid file descriptor.
}

func (l *valueLog) getFile(fid int32) (*logFile, error) {
	l.RLock()
	defer l.RUnlock()

	idx := sort.Search(len(l.files), func(idx int) bool {
		return l.files[idx].fid >= fid
	})
	if idx == len(l.files) || l.files[idx].fid != fid {
		return nil, Corrupt
	}
	return l.files[idx], nil
}

// Read reads the value log at a given location.
func (l *valueLog) Read(p valuePointer, s *y.Slice) (e Entry, err error) {
	lf, err := l.getFile(int32(p.Fid))
	if err != nil {
		return e, err
	}

	if s == nil {
		s = new(y.Slice)
	}
	buf := s.Resize(int(p.Len))
	if err := lf.read(buf, int64(p.Offset)); err != nil {
		return e, err
	}
	var h header
	buf, _ = h.Decode(buf)
	if h.meta&BitCompressed > 0 {
		// TODO: reuse generated buffer
		y.AssertTrue(uint32(len(buf)) == h.vlen)
		decoded, err := lz4.Decode(nil, buf)
		y.Check(err)

		y.AssertTrue(len(decoded) > int(h.klen))
		h.vlen = uint32(len(decoded)) - h.klen
		buf = decoded
	}
	e.Key = buf[0:h.klen]
	e.Meta = h.meta
	e.casCounter = h.casCounter
	e.CASCounterCheck = h.casCounterCheck
	e.Value = buf[h.klen : h.klen+h.vlen]
	return e, nil
}

func (l *valueLog) runGCInLoop(lc *y.LevelCloser) {
	defer lc.Done()
	if l.opt.ValueGCThreshold == 0.0 {
		fmt.Println("l.opt.ValueGCThreshold = 0.0. Exiting runGCInLoop")
		return
	}

	tick := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-lc.HasBeenClosed():
			fmt.Println("has been closed")
			return
		case <-tick.C:
			l.doRunGC()
		}
	}
}

func (l *valueLog) pickLog() *logFile {
	l.RLock()
	defer l.RUnlock()
	if len(l.files) <= 1 {
		fmt.Println("Need at least 2 value log files to run GC.")
		return nil
	}
	// This file shouldn't be being written to.
	lfi := rand.Intn(len(l.files))
	if lfi > 0 {
		lfi = rand.Intn(lfi) // Another level of rand to favor smaller fids.
	}
	return l.files[lfi]
}

func (vlog *valueLog) doRunGC() {
	lf := vlog.pickLog()
	if lf == nil {
		return
	}

	type reason struct {
		total   float64
		keep    float64
		discard float64
	}

	var r reason
	var window float64 = 100.0
	count := 0

	fmt.Printf("Picked fid: %d for GC\n", lf.fid)
	// Pick a random start point for the log.
	skipFirstM := float64(rand.Int63n(LogSize/int64(M))) - window
	var skipped float64
	fmt.Printf("Skipping first %5.2f MB\n", skipFirstM)

	start := time.Now()
	y.AssertTrue(vlog.kv != nil)
	err := lf.iterate(0, func(e Entry) bool {
		esz := float64(len(e.Key)+len(e.Value)+1+4) / (1 << 20) // in MBs. +4 for the CAS stuff.
		skipped += esz
		if skipped < skipFirstM {
			return true
		}

		count++
		if count%100 == 0 {
			time.Sleep(time.Millisecond)
		}
		r.total += esz
		if r.total > window {
			return false
		}
		if time.Since(start) > 10*time.Second {
			return false
		}

		vs := vlog.kv.get(e.Key)
		if (vs.Meta & BitDelete) > 0 {
			// Key has been deleted. Discard.
			r.discard += esz
			return true
		}
		if (vs.Meta & BitValuePointer) == 0 {
			// Value is stored alongside key. Discard.
			r.discard += esz
			return true
		}

		// Value is still present in value log.
		y.AssertTrue(len(vs.Value) > 0)
		var vp valuePointer
		vp.Decode(vs.Value)

		if int32(vp.Fid) > lf.fid {
			// Value is present in a later log. Discard.
			r.discard += esz
			return true
		}
		if int64(vp.Offset) > e.Offset {
			// Value is present in a later offset, but in the same log.
			r.discard += esz
			return true
		}
		if int32(vp.Fid) == lf.fid && int64(vp.Offset) == e.Offset {
			// This is still the active entry. This would need to be rewritten.
			r.keep += esz

		} else {
			fmt.Printf("Reason=%+v\n", r)
			ne, err := vlog.Read(vp, nil)
			y.Check(err)
			ne.Offset = int64(vp.Offset)
			if ne.casCounter == e.casCounter {
				ne.print("Latest Entry in LSM")
				e.print("Latest Entry in Log")
				y.Fatalf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.", vp, vs.Meta)
			}
		}
		return true
	})

	y.Checkf(err, "While iterating for RunGC.")
	fmt.Printf("Fid: %d Data status=%+v\n", lf.fid, r)

	if r.total < 10.0 || r.keep >= vlog.opt.ValueGCThreshold*r.total {
		fmt.Printf("Skipping GC on fid: %d\n\n", lf.fid)
		return
	}

	fmt.Printf("=====> REWRITING VLOG %d\n", lf.fid)
	vlog.rewrite(lf)
	fmt.Println("REWRITE DONE")
	vlog.elog.Printf("Done rewriting.")
}
