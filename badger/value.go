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
	"sync/atomic"
	"time"

	"golang.org/x/net/trace"

	"github.com/bkaradzic/go-lz4"
	"github.com/dgraph-io/badger/y"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete             = 1       // Set if the key has been deleted.
	BitValuePointer       = 2       // Set if the value is NOT stored directly next to key.
	BitCompressed         = 4       // Set if the value is compressed in the value log.
	LogSize         int64 = 1 << 30 // ~1GB
	M               int   = 1 << 20
)

var Corrupt error = errors.New("Unable to find log. Potential data corruption.")

type logFile struct {
	sync.RWMutex
	fd     *os.File
	fid    int32
	offset int64
	size   int64
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

	fi, err := lf.fd.Stat()
	lf.size = fi.Size()
}

type logEntry func(e Entry) bool

func iterateEntries(reader bufio.Reader, fn logEntry) error {

}

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

	// we could extract entry reader from here
	hbuf := make([]byte, 8)
	var h header
	var count int
	k := make([]byte, 1<<10)
	v := make([]byte, 1<<20)

	ensureCapacity := func() {
		if cap(k) < int(h.klen) {
			k = make([]byte, 2*h.klen)
		}
		if cap(v) < int(h.vlen) {
			v = make([]byte, 2*h.vlen)
		}
	}

	var e Entry
	decodeEntry := func() (err error) {
		e.Key = k[:h.klen]
		e.Value = v[:h.vlen]

		if err = read(reader, e.Key); err != nil {
			return err
		}
		if e.Meta, err = reader.ReadByte(); err != nil {
			return err
		}
		if err = read(reader, e.Value); err != nil {
			return err
		}
		return
	}

	recordOffset := offset
OUTER:
	for {
		if err = read(reader, hbuf); err == io.EOF {
			break
		}
		h.Decode(hbuf)
		// fmt.Printf("[%d] Header read: %+v\n", count, h)

		kl := int(h.klen)

		if kl > 0 {
			vl := int(h.vlen)

			ensureCapacity()

			e.Offset = recordOffset

			err = decodeEntry()
			if err != nil {
				return err
			}
			if !fn(e) {
				break
			}
			count++
			recordOffset += int64(8 + kl + vl + 1)
		} else { // key length == 0 => beginning of compressed block
			bl := int(h.vlen)
			block := make([]byte, bl)
			if err = read(reader, block); err != nil {
				return err
			}
			decoded, err := lz4.Decode(nil, block)
			if err != nil {
				return err
			}

			blockOffset := recordOffset + 8
			for i := 0; i < bl; i++ {
				h.Decode(decoded)

				kl = int(h.klen)
				vl := int(h.vlen)

				ensureCapacity()

				// hack: we can't have an offset for
				// compressed entry... what is this offset for?
				e.Offset = blockOffset + int64(i)

				err = decodeEntry()
				if err != nil {
					return err
				}
				if !fn(e) {
					break OUTER
				}
				count++
			}
			recordOffset = blockOffset + int64(bl)
		}
	}
	return nil
}

func (vlog *valueLog) rewrite(f *logFile) {
	maxFid := atomic.LoadInt32(&vlog.maxFid)
	y.AssertTruef(f.fid < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)

	elog := trace.NewEventLog("badger", "vlog-rewrite")
	defer elog.Finish()
	elog.Printf("Rewriting fid: %d", f.fid)
	fmt.Println("rewrite called")
	ctx := context.Background()

	b := &block{
		Wg: sync.WaitGroup{},
	}
	blocks := make([]*block, 0, 10)
	blocks = append(blocks, b)

	y.AssertTrue(vlog.kv != nil)
	var count int
	fe := func(e Entry) {
		count++
		if count%10000 == 0 {
			elog.Printf("Processing entry %d", count)
		}

		vptr, meta := vlog.kv.get(ctx, e.Key)

		if (meta & BitDelete) > 0 {
			return
		}
		if (meta & BitValuePointer) == 0 {
			return
		}

		// Value is still present in value log.
		y.AssertTrue(len(vptr) > 0)
		var vp valuePointer
		vp.Decode(vptr)

		if int32(vp.Fid) > f.fid {
			return
		}
		if int64(vp.Offset) > e.Offset {
			return
		}
		if int32(vp.Fid) == f.fid && int64(vp.Offset) == e.Offset {
			// This new entry only contains the key, and a pointer to the value.
			var ne Entry
			y.AssertTruef(e.Meta == 0, "Got meta: %v", e.Meta)
			ne.Meta = e.Meta
			ne.Key = make([]byte, len(e.Key))
			copy(ne.Key, e.Key)
			ne.Value = make([]byte, len(e.Value))
			copy(ne.Value, e.Value)
			b.Entries = append(b.Entries, &ne)
			if len(b.Entries) >= 1000 {
				b = &block{
					Wg: sync.WaitGroup{},
				}
				blocks = append(blocks, b)
			}

		} else {
			y.Fatalf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.", vp, meta)
		}
	}

	f.iterate(0, func(e Entry) bool {
		fe(e)
		return true
	})
	elog.Printf("Processed %d entries in total", count)

	for i, b := range blocks {
		elog.Printf("block %d has %d entries", i, len(b.Entries))
		fmt.Printf("block %d has %d entries\n", i, len(b.Entries))
		b.Wg.Add(1)
		vlog.kv.writeCh <- b
	}
	for i, b := range blocks {
		elog.Printf("block %d done", i)
		fmt.Printf("block %d done\n", i)
		b.Wg.Wait()
	}

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

type Entry struct {
	Key    []byte
	Meta   byte
	Value  []byte
	Offset int64
}

func (e Entry) EncodeTo(buf *bytes.Buffer) {
	var headerEnc [8]byte
	var h header
	h.klen = uint32(len(e.Key))
	h.vlen = uint32(len(e.Value))
	h.Encode(headerEnc[:])

	buf.Write(headerEnc[:])
	buf.Write(e.Key)
	buf.WriteByte(e.Meta)
	buf.Write(e.Value)
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d Offset: %d len(val)=%d\n",
		prefix, e.Key, e.Meta, e.Offset, len(e.Value))
}

// klen == 0 means that it's a start of a compressed block of entries.
// In this case vlen denotes the length of the block.
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
	Fid               uint32
	Len               uint32
	Offset            uint64
	InsideBlockOffset uint16
	Meta              byte
}

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 19)
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint64(b[8:16], p.Offset)
	binary.BigEndian.PutUint16(b[16:18], p.InsideBlockOffset)
	b[18] = p.Meta
	return b[:19]
}

func (p *valuePointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 16)
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint64(b[8:16])
	p.InsideBlockOffset = binary.BigEndian.Uint16(b[16:18])
	p.Meta = b[18]
}

type valueLog struct {
	sync.RWMutex
	opt   Options
	files []*logFile
	// fds    []*os.File
	offset  int64
	elog    trace.EventLog
	dirPath string
	maxFid  int32
	kv      *KV
	closer  *y.Closer
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

		lf := &logFile{fid: int32(fid)}
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
			// lf.fd, err = os.OpenFile(l.fpath(lf.fid), os.O_RDWR|os.O_CREATE, 0666)
			y.Check(err)
			l.maxFid = lf.fid

		} else {
			lf.fd, err = os.OpenFile(l.fpath(lf.fid), os.O_RDONLY, 0666)
			y.Check(err)
		}
	}

	// If no files are found, then create a new file.
	if len(l.files) == 0 {
		lf := &logFile{fid: 0}
		lf.fd, err = y.OpenSyncedFile(l.fpath(lf.fid), l.opt.SyncWrites)
		y.Check(err)
		l.files = append(l.files, lf)
	}
}

func (l *valueLog) Open(kv *KV, opt *Options) {
	l.dirPath = opt.Dir
	l.openOrCreateFiles()
	l.closer = y.NewCloser(1)
	l.opt = *opt
	l.kv = kv
	go l.runGCInLoop()

	l.elog = trace.NewEventLog("Badger", "Valuelog")
}

func (l *valueLog) Close() {
	l.elog.Printf("Stopping garbage collection of values.")
	l.closer.Signal()
	l.closer.Wait()

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

type block struct {
	Entries []*Entry
	Ptrs    []valuePointer
	Wg      sync.WaitGroup
}

// Write writes entries from blocks to the disk and updates the valuePointers
// for this entries.
//
// Write is thread-unsafe by design and should not be called concurrently.
func (l *valueLog) Write(blocks []*block) {
	writer := newWriter(l)
	writer.write(blocks)

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
func (l *valueLog) Read(ctx context.Context, p valuePointer) (e Entry, err error) {
	y.Trace(ctx, "Reading value with pointer: %+v", p)
	defer y.Trace(ctx, "Read done")

	lf, err := l.getFile(int32(p.Fid))
	if err != nil {
		return e, err
	}

	var h header
	buf := make([]byte, p.Len)
	if _, err := lf.fd.ReadAt(buf, int64(p.Offset)); err != nil {
		return e, err
	}

	if p.Meta&BitCompressed != 0 {
		decoded, err := lz4.Decode(nil, buf)
		y.Check(err)

		buf = decoded[p.InsideBlockOffset:]
	}

	buf = h.Decode(buf)
	e.Key = buf[0:h.klen]
	e.Meta = buf[h.klen] | (p.Meta & BitCompressed)
	buf = buf[h.klen+1:]
	e.Value = buf[0:h.vlen]
	return e, nil
}

func (l *valueLog) runGCInLoop() {
	defer l.closer.Done()
	defer func() {
		fmt.Printf("Stopping runGCInLoop. Signal: %v", l.closer.GotSignal())
	}()

	if l.opt.ValueGCThreshold == 0.0 {
		fmt.Println("l.opt.ValueGCThreshold = 0.0")
		return
	}

	tick := time.NewTicker(time.Minute)
	for {
		select {
		case <-l.closer.HasBeenClosed():
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

	tr := trace.New("badger", "value-gc")
	defer tr.Finish()
	ctx := trace.NewContext(context.Background(), tr)

	var r reason
	var window float64 = 100.0
	count := 0

	y.Trace(ctx, "Picked fid: %d for GC", lf.fid)
	fmt.Printf("Picked fid: %d for GC\n", lf.fid)
	// Pick a random start point for the log.
	skipFirstM := float64(rand.Int63n(LogSize/int64(M))) - window
	var skipped float64
	fmt.Printf("Skipping first %5.2f MB\n", skipFirstM)

	start := time.Now()
	y.AssertTrue(vlog.kv != nil)
	err := lf.iterate(0, func(e Entry) bool {
		esz := float64(len(e.Key)+len(e.Value)+1) / (1 << 20) // in MBs.
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

		vptr, meta := vlog.kv.get(ctx, e.Key)
		if (meta & BitDelete) > 0 {
			// Key has been deleted. Discard.
			r.discard += esz
			return true
		}
		if (meta & BitValuePointer) == 0 {
			// Value is stored alongside key. Discard.
			r.discard += esz
			return true
		}

		// Value is still present in value log.
		y.AssertTrue(len(vptr) > 0)
		var vp valuePointer
		vp.Decode(vptr)

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
			ne, err := vlog.Read(context.Background(), vp)
			y.Check(err)
			ne.Offset = int64(vp.Offset)
			ne.print("Latest Entry in LSM")
			e.print("Latest Entry in Log")
			y.Fatalf("This shouldn't happen. Latest Pointer:%+v. Meta:%v.", vp, meta)
		}
		return true
	})

	y.Checkf(err, "While iterating for RunGC.")
	y.Trace(ctx, "Fid: %d Data status=%+v\n", lf.fid, r)
	fmt.Printf("Fid: %d Data status=%+v\n", lf.fid, r)

	if r.total < 10.0 || r.keep >= vlog.opt.ValueGCThreshold*r.total {
		y.Trace(ctx, "Skipping GC on fid: %d", lf.fid)
		fmt.Printf("Skipping GC on fid: %d\n\n", lf.fid)
		return
	}

	y.Trace(ctx, "Rewriting fid: %d", lf.fid)
	fmt.Printf("=====> REWRITING VLOG %d\n", lf.fid)
	vlog.rewrite(lf)
	fmt.Println("REWRITE DONE")
	vlog.elog.Printf("Done rewriting.")
}
