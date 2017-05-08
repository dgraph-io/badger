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

	"github.com/bkaradzic/go-lz4"
	"github.com/dgraph-io/badger/y"
	"github.com/hashicorp/golang-lru"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	BitDelete              = 1       // Set if the key has been deleted.
	BitValuePointer        = 2       // Set if the value is NOT stored directly next to key.
	BitCompressed   uint16 = 1 << 15 // Set if the value is compressed in the value log.
	LogSize                = 1 << 30 // ~1GB
	M               int    = 1 << 20
)

var Corrupt error = errors.New("Unable to find log. Potential data corruption.")

type logFile struct {
	sync.RWMutex
	path   string
	fd     *os.File
	fid    uint16
	offset uint32
	size   int64
}

// openReadOnly assumes that we have a write lock on logFile.
func (lf *logFile) openReadOnly() {
	var err error
	lf.fd, err = os.OpenFile(lf.path, os.O_RDONLY, 0666)
	if err != nil {
		y.Fatalf("Unable to open file %s: %+v", lf.path, err)
	}

	fi, err := lf.fd.Stat()
	y.Check(err)
	lf.size = fi.Size()
}

func (lf *logFile) read(buf []byte, offset int64) error {
	lf.RLock()
	defer lf.RUnlock()

	_, err := lf.fd.ReadAt(buf, offset)
	// y.AssertTruef(len(buf) == n, "%d != %d", len(buf), n)
	return err
}

func (lf *logFile) doneWriting() {
	lf.Lock()
	defer lf.Unlock()
	y.Check(lf.fd.Close())

	lf.openReadOnly()
}

type logEntry func(e Entry) bool

type entryDecoder struct {
	reader  io.Reader
	header  header
	hbuf    []byte
	keybuf  []byte
	valbuf  []byte
	metabuf []byte
	entry   Entry
}

func newEntryDecoder(reader io.Reader) (ed entryDecoder) {
	ed.reader = reader
	ed.hbuf = make([]byte, 8)
	ed.keybuf = make([]byte, 1<<10)
	ed.valbuf = make([]byte, 1<<20)
	ed.metabuf = make([]byte, 1)
	return
}

// read reads bytes from the reader untill fills buf.
func (ed *entryDecoder) read(buf []byte) error {
	for {
		n, err := ed.reader.Read(buf)
		if err != nil {
			return err
		}
		if n == len(buf) {
			return nil
		}
		buf = buf[n:]
	}
}

// fills the ed.header from ed.hbuf.
func (ed *entryDecoder) readHeader() error {
	err := ed.read(ed.hbuf)
	if err != nil {
		return err
	}
	ed.header.Decode(ed.hbuf)
	return nil
}

// ensures that ed.keybuf and ed.valbuf have
// enough space to read next entry specified by ed.header.
func (ed *entryDecoder) ensureCapacity() {
	if cap(ed.keybuf) < int(ed.header.klen) {
		ed.keybuf = make([]byte, 2*ed.header.klen)
	}
	if cap(ed.valbuf) < int(ed.header.vlen) {
		ed.valbuf = make([]byte, 2*ed.header.vlen)
	}
}

// readEntry reads and decodes entry from ed.reader to
// ed.entry
func (ed *entryDecoder) readEntry() (err error) {
	ed.ensureCapacity()

	ed.entry.Key = ed.keybuf[:ed.header.klen]
	ed.entry.Value = ed.valbuf[:ed.header.vlen]

	if err = ed.read(ed.entry.Key); err != nil {
		return err
	}
	if err = ed.read(ed.metabuf); err != nil {
		return err
	}
	ed.entry.Meta = ed.metabuf[0]

	// TODO: Add casCounter.
	var casBytes [4]byte
	if err = ed.read(casBytes[:]); err != nil {
		return err
	}
	ed.entry.casCounter = binary.BigEndian.Uint16(casBytes[:2])
	ed.entry.CASCounterCheck = binary.BigEndian.Uint16(casBytes[2:])

	if err = ed.read(ed.entry.Value); err != nil {
		return err
	}
	return
}

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (f *logFile) iterate(offset uint32, fn logEntry) error {
	_, err := f.fd.Seek(int64(offset), 0)
	y.Check(err)

	reader := bufio.NewReader(f.fd)
	entryDecoder := newEntryDecoder(reader)

	var count int

	fileOffset := offset

	e := &entryDecoder.entry

	var commpressedBlocksBuffer []byte
OUTER:
	for {
		if err = entryDecoder.readHeader(); err == io.EOF {
			break
		}
		// fmt.Printf("[%d] Header read: %+v\n", count, entryDecoder.header)

		kl := int(entryDecoder.header.klen)

		if kl > 0 {
			vl := int(entryDecoder.header.vlen)

			e.Offset = fileOffset

			err = entryDecoder.readEntry()
			if err != nil {
				return err
			}
			if !fn(*e) {
				break
			}
			count++
			fileOffset += uint32(8 + kl + vl + 1 + 4) // +1 for meta, +4 for CAS stuff.
		} else { // key length == 0 => beginning of compressed block
			bl := entryDecoder.header.vlen
			block := make([]byte, bl)
			if err = entryDecoder.read(block); err != nil {
				return err
			}
			decoded, err := lz4.Decode(commpressedBlocksBuffer, block)
			if err != nil {
				return err
			}

			entryDecoder.reader = bytes.NewReader(decoded)
		INNER:
			for {
				err = entryDecoder.readHeader()
				if err == io.EOF {
					break INNER
				}
				y.Check(err)

				err = entryDecoder.readEntry()
				e.Offset = fileOffset
				if err != nil {
					return err
				}
				if !fn(*e) {
					break OUTER
				}
				count++
			}
			fileOffset = fileOffset + 8 + bl
			entryDecoder.reader = reader
		}
	}
	return nil
}

var entries = make([]*Entry, 0, 1000000)

func (vlog *valueLog) rewrite(f *logFile) {
	maxFid := atomic.LoadUint32(&vlog.maxFid)
	y.AssertTruef(uint32(f.fid) < maxFid, "fid to move: %d. Current max fid: %d", f.fid, maxFid)

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

		if vp.Fid > f.fid {
			return
		}
		if vp.Offset > e.Offset {
			return
		}
		if vp.Fid == f.fid && vp.Offset == e.Offset {
			// This new entry only contains the key, and a pointer to the value.
			var ne Entry
			y.AssertTruef(e.Meta == 0, "Got meta: %v", e.Meta)
			ne.Meta = e.Meta
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
	Offset          uint32
	CASCounterCheck uint16 // If nonzero, we will check if existing casCounter matches.

	// Fields maintained internally.
	casCounter uint16
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

	// TODO: Reduce space used here.
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], e.casCounter)
	buf.Write(b[:])
	binary.BigEndian.PutUint16(b[:], e.CASCounterCheck)
	buf.Write(b[:])

	buf.Write(e.Value)
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d Offset: %d len(val)=%d cas=%d check=%d\n",
		prefix, e.Key, e.Meta, e.Offset, len(e.Value), e.casCounter, e.CASCounterCheck)
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

// Pointer to the entry. The entry is stored in a compressed entries block
// iff first bit of InsideBlockOffset is set to 1.
// If not compressed, the entry is encoded in the file at Offset and has Len (including header).
// If compressed, Offset specifies the beginning of the compressed entries block
// and Len -- length of the block.
// This block contains header (key=0, value=length of the data block) and data.
// InsideBlockOffset indicates the entry position in the uncompressed data.
type valuePointer struct {
	Fid               uint16
	Len               uint32
	Offset            uint32
	InsideBlockOffset uint16
}

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	y.AssertTrue(len(b) >= 12)
	binary.BigEndian.PutUint16(b[:2], p.Fid)
	binary.BigEndian.PutUint32(b[2:6], p.Len)
	binary.BigEndian.PutUint32(b[6:10], p.Offset)
	binary.BigEndian.PutUint16(b[10:12], p.InsideBlockOffset)
	return b[:12]
}

func (p *valuePointer) Decode(b []byte) {
	y.AssertTrue(len(b) >= 12)
	p.Fid = binary.BigEndian.Uint16(b[:2])
	p.Len = binary.BigEndian.Uint32(b[2:6])
	p.Offset = binary.BigEndian.Uint32(b[6:10])
	p.InsideBlockOffset = binary.BigEndian.Uint16(b[10:12])
}

type valueLog struct {
	sync.RWMutex
	files []*logFile
	// fds    []*os.File
	offset                int64
	elog                  trace.EventLog
	dirPath               string
	kv                    *KV
	maxFid                uint32
	opt                   Options
	compressedBlocksCache *lru.Cache
}

func (l *valueLog) fpath(fid uint16) string {
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

		lf := &logFile{fid: uint16(fid), path: l.fpath(uint16(fid))}
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
			l.maxFid = uint32(lf.fid)

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
	var err error
	l.compressedBlocksCache, err = lru.New(opt.BlockCacheSize)
	y.Check(err)

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
	fid := ptr.Fid
	offset := ptr.Offset
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
	last := l.files[len(l.files)-1]
	n, err := last.fd.Seek(0, io.SeekEnd)
	y.AssertTrue(n < 1<<32)
	last.offset = uint32(n)
	y.Checkf(err, "Unable to seek to the end")
}

type request struct {
	Entries []*Entry
	Ptrs    []valuePointer
	Wg      sync.WaitGroup
}

// Write writes entries from blocks to the disk and updates the valuePointers
// for this entries.
//
// Write is thread-unsafe by design and should not be called concurrently.
func (l *valueLog) Write(reqs []*request) {
	writer := newWriter(l)
	writer.write(reqs)

	// Acquire mutex locks around this manipulation, so that the reads don't try to use
	// an invalid file descriptor.
}

func (l *valueLog) getFile(fid uint16) (*logFile, error) {
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

type fileOffset struct {
	fid    uint16
	offset uint32
}

// Read reads the value log at a given location.
func (l *valueLog) Read(p valuePointer, s *y.Slice) (e Entry, err error) {
	lf, err := l.getFile(p.Fid)
	if err != nil {
		return e, err
	}

	if s == nil {
		s = new(y.Slice)
	}
	var h header
	var buf []byte

	if p.InsideBlockOffset&BitCompressed > 0 {
		var decoded []byte

		d, cached := l.compressedBlocksCache.Get(fileOffset{p.Fid, p.Offset})

		if cached {
			// fmt.Println("in cache")
			decoded = d.([]byte)
		} else {
			// fmt.Println("not in cache")
			buf = s.Resize(int(p.Len))
			if err := lf.read(buf, int64(p.Offset)); err != nil {
				return e, err
			}

			buf = h.Decode(buf)
			y.AssertTrue(h.klen == 0)
			y.AssertTruef(h.vlen+8 == p.Len, "%d+8 != %d", h.vlen, p.Len)

			decoded, err = lz4.Decode(nil, buf)
			if err != nil {
				y.Fatalf("failed to decode compressed block vpt: %+v, size: %d", p, len(buf))
			}
			y.Check(err)

			l.compressedBlocksCache.Add(fileOffset{p.Fid, p.Offset}, decoded)
		}
		buf = decoded[(p.InsideBlockOffset - BitCompressed):] // seting first bit to 0
	} else {
		buf = s.Resize(int(p.Len))
		if err := lf.read(buf, int64(p.Offset)); err != nil {
			return e, err
		}
	}

	buf = h.Decode(buf)
	y.AssertTrue(h.klen > 0)
	y.AssertTruef(len(buf) >= int(h.klen+5+h.vlen), "chuj %+v %d, %d %+v", p, len(buf), int(h.klen+5+h.vlen), h)
	e.Key = buf[0:h.klen]
	e.Meta = buf[h.klen]
	e.casCounter = binary.BigEndian.Uint16(buf[h.klen+1 : h.klen+3])
	e.CASCounterCheck = binary.BigEndian.Uint16(buf[h.klen+3 : h.klen+5])
	buf = buf[h.klen+5:]
	e.Value = buf[0:h.vlen]

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

		if vp.Fid > lf.fid {
			// Value is present in a later log. Discard.
			r.discard += esz
			return true
		}
		if vp.Offset > e.Offset {
			// Value is present in a later offset, but in the same log.
			r.discard += esz
			return true
		}
		if vp.Fid == lf.fid && vp.Offset == e.Offset {
			// This is still the active entry. This would need to be rewritten.
			r.keep += esz

		} else {
			fmt.Printf("Reason=%+v\n", r)
			ne, err := vlog.Read(vp, nil)
			y.Check(err)
			ne.Offset = vp.Offset
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
