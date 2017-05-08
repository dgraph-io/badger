package badger

import (
	"errors"
	"sync/atomic"

	"github.com/bkaradzic/go-lz4"

	"bytes"
	"github.com/dgraph-io/badger/y"
)

const (
	optimalLz4BlockSize  = 1<<15 - 1 //64 * (1 << 10) // Needs to be less than CompressedBit
	compressionThreshold = 1.5       // We compress values only if compression provides specified space gain.
)

var (
	errNoSpaceGain = errors.New("no significant space gain")
)

type block struct {
	entries  []*Entry
	offsets  []uint16
	requests []*request
	buf      bytes.Buffer
}

func (b *block) append(e *Entry, r *request) {
	b.entries = append(b.entries, e)
	y.AssertTruef(b.buf.Len() <= int(BitCompressed)-1, "current offset: %d %d", b.buf.Len(), int(BitCompressed))
	b.offsets = append(b.offsets, uint16(b.buf.Len()))
	b.requests = append(b.requests, r)

	e.EncodeTo(&b.buf)
}

func (b *block) reset() {
	b.buf.Reset()

	b.offsets = b.offsets[:0]
	b.entries = b.entries[:0]
	b.requests = b.requests[:0]
}

func (b *block) flushWithoutCompression(dest *bytes.Buffer, lf *logFile) {
	for j, e := range b.entries {
		var p valuePointer
		p.Fid = lf.fid
		p.Len = uint32(8 + len(e.Key) + len(e.Value) + 1 + 4) // +4 for CAS stuff.
		p.Offset = lf.offset + uint32(dest.Len()) + uint32(b.offsets[j])
		y.AssertTrue(p.InsideBlockOffset == 0)

		b.requests[j].Ptrs = append(b.requests[j].Ptrs, p)
	}

	dest.Write(b.buf.Bytes())
	b.reset()
}

type valueLogWriter struct {
	l                  *valueLog
	curlf              *logFile
	compressionEnabled bool
	writeBuf           bytes.Buffer
	compressionBuf     []byte
}

func newWriter(l *valueLog) *valueLogWriter {
	writer := &valueLogWriter{
		l:                  l,
		compressionEnabled: l.opt.ValueCompression,
	}

	l.RLock()
	writer.curlf = l.files[len(l.files)-1]
	l.RUnlock()

	return writer
}

func (w *valueLogWriter) compress(buffer []byte) ([]byte, error) {
	compressed, err := lz4.Encode(w.compressionBuf, buffer)
	if err != nil {
		return nil, err
	}

	if float32(len(compressed))*compressionThreshold > float32(len(buffer)) {
		return nil, errNoSpaceGain
	}

	return compressed, nil
}

func (w *valueLogWriter) newFile() (newlf *logFile) {
	var err error
	w.curlf.doneWriting()

	newfid := uint16(atomic.AddUint32(&w.l.maxFid, 1))
	newlf = &logFile{fid: newfid, path: w.l.fpath(newfid)}
	newlf.fd, err = y.OpenSyncedFile(newlf.path, w.l.opt.SyncWrites)
	y.Check(err)

	w.l.Lock()
	w.l.files = append(w.l.files, newlf)
	w.l.Unlock()

	return
}

// Writes buffer to the logFile and updates curlf.offset.
func (w *valueLogWriter) writeToDisk(buffer []byte) (err error) {
	if len(buffer) == 0 {
		return
	}

	var n int

	n, err = w.curlf.fd.Write(buffer)
	if err != nil {
		return
	}

	w.curlf.offset += uint32(n)

	if w.curlf.offset > LogSize {
		w.curlf = w.newFile()
	}
	return
}

func (w *valueLogWriter) write(blocks []*request) {
	var headerBuffer [8]byte

	var currentBlock block

	flushBuffers := func() {
		currentBlock.flushWithoutCompression(&w.writeBuf, w.curlf)
		err := w.writeToDisk(w.writeBuf.Bytes())
		y.Checkf(err, "unable to write to value log: %v", err)
		w.writeBuf.Reset()
	}

	for i := range blocks {
		b := blocks[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]

			y.AssertTruef(len(e.Key) > 0, "key empty")

			currentBlock.append(e, b)

			if w.compressionEnabled && currentBlock.buf.Len() > optimalLz4BlockSize {
				compressed, err := w.compress(currentBlock.buf.Bytes())

				if err != nil {
					if err == errNoSpaceGain {
						w.l.elog.Printf("Flushing without compression: %v\n", err)

						currentBlock.flushWithoutCompression(&w.writeBuf, w.curlf)
					} else {
						y.Check(err)
					}
				} else {
					w.l.elog.Printf("Adding compressed block.")

					for j := range currentBlock.entries {
						var p valuePointer
						p.Fid = w.curlf.fid
						p.Len = uint32(len(compressed) + 8) // + header size
						p.Offset = w.curlf.offset + uint32(w.writeBuf.Len())
						y.AssertTrue(currentBlock.offsets[j]&BitCompressed == 0)
						p.InsideBlockOffset = currentBlock.offsets[j] ^ BitCompressed

						currentBlock.requests[j].Ptrs = append(currentBlock.requests[j].Ptrs, p)
					}

					h := header{0, uint32(len(compressed))}
					h.Encode(headerBuffer[:])
					w.writeBuf.Write(headerBuffer[:])

					w.writeBuf.Write(compressed)
					currentBlock.reset()

					w.l.elog.Printf("Saved compressed block of size %d bytes from %d bytes.\n",
						len(compressed), w.writeBuf.Len())
				}
			}
			if w.curlf.offset+uint32(w.writeBuf.Len()) > uint32(LogSize) {
				w.l.elog.Printf("Writing to disk entries of total size: %d", w.writeBuf.Len())
				flushBuffers()
				w.l.elog.Printf("Done")
			}
		}
	}
	flushBuffers()
}
