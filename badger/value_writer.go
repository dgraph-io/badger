package badger

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bkaradzic/go-lz4"

	"bytes"
	"github.com/dgraph-io/badger/y"
)

const (
	optimalLz4BlockSize  = 64 * (1 << 10) // 64 KB
	compressionThreshold = 1.0            // We compress values only if compression provides specified space gain.
)

type valueLogWriter struct {
	l                  *valueLog
	curlf              *logFile
	compressionEnabled bool
	writeBuf           bytes.Buffer
	compressionBlock   bytes.Buffer
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

func compress(buffer []byte) ([]byte, error) {
	// TODO(szm): We should reuse the compressed buffer.
	compressed, err := lz4.Encode(nil, buffer)
	if err != nil {
		return nil, err
	}

	if len(compressed)*compressionThreshold > len(buffer) {
		return nil, errors.New("no significant space gain")
	}

	return compressed, nil
}

type entryPosition struct {
	block uint16
	id    uint16
}

func (e entryPosition) String() string {
	return fmt.Sprintf("Block %d, id: %d", e.block, e.id)
}

func next(e entryPosition, blocks []*request) entryPosition {
	if uint16(len(blocks[e.block].Entries)) == e.id-1 {
		// last entry in block
		return entryPosition{e.block + 1, 0}
	}
	return entryPosition{e.block, e.id + 1}
}

func (w *valueLogWriter) newFile() (newlf *logFile) {
	var err error
	w.curlf.doneWriting()

	newlf = &logFile{fid: uint16(atomic.AddUint32(&w.l.maxFid, 1)), offset: 0}
	newlf.fd, err = y.OpenSyncedFile(w.l.fpath(newlf.fid), w.l.opt.SyncWrites)
	y.Check(err)

	w.l.Lock()
	w.l.files = append(w.l.files, newlf)
	w.l.Unlock()

	return
}

// Saves buffer to the logFile and updates curlf.offset.
func (w *valueLogWriter) saveToDisk(buffer []byte) (err error) {
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

// Saves compressed block of entries to curlf and updated appriopriate poitners.
// Resets the buffer and updates curlf.offset.

// Traverses blocks entries between from and to (excluding to) and sets
// BlockOffset and InsideBlockOffset values.
func (w *valueLogWriter) updateCompressedPointers(blocks []*request, from, to entryPosition,
	blockStart, headerSize, blockLen uint32) {
	beforeTo := func(e entryPosition) bool {
		return e.block < to.block || (e.block == to.block && e.id < to.id)
	}

	for pos := from; beforeTo(pos); pos = next(pos, blocks) {
		ptr := &blocks[pos.block].Ptrs[pos.id]

		ptr.InsideBlockOffset = uint16(ptr.Offset-blockStart) | BitCompressed
		ptr.Offset = blockStart
		ptr.Len = blockLen + headerSize
	}
}

func (w *valueLogWriter) write(blocks []*request) {
	headerBuffer := make([]byte, 8)

	var firstCompressionEntry entryPosition

	flushWithoutCompression := func() {
		w.writeBuf.Write(w.compressionBlock.Bytes())
		w.compressionBlock.Reset()
	}

	save := func() {
		flushWithoutCompression()
		err := w.saveToDisk(w.writeBuf.Bytes())
		y.Checkf(err, "unable to write to value log: %v", err)
		w.writeBuf.Reset()
	}

	for i := range blocks {
		b := blocks[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]

			y.AssertTruef(len(e.Key) > 0, "key empty")

			// We set the pointer if entries were not compressed.
			var p valuePointer
			p.Fid = w.curlf.fid
			p.Len = uint32(8 + len(e.Key) + len(e.Value) + 1 + 4) // +4 for CAS stuff.
			p.Offset = w.curlf.offset + uint32(w.writeBuf.Len()+w.compressionBlock.Len())
			b.Ptrs = append(b.Ptrs, p)

			e.EncodeTo(&w.compressionBlock)
			nextEntry := next(entryPosition{uint16(i), uint16(j)}, blocks)

			if w.compressionEnabled && w.compressionBlock.Len() > optimalLz4BlockSize {
				compressed, err := compress(w.compressionBlock.Bytes())

				if err == nil {
					w.l.elog.Printf("Adding compressed block.")
					blockStart := w.curlf.offset + uint32(w.writeBuf.Len())

					var h header
					h.klen = 0
					h.vlen = uint32(len(compressed))
					h.Encode(headerBuffer)
					w.writeBuf.Write(headerBuffer)

					w.writeBuf.Write(compressed)
					fmt.Printf("Copied block of size %d bytes compressed from %d bytes.\n",
						len(compressed), w.compressionBlock.Len())
					w.compressionBlock.Reset()

					w.updateCompressedPointers(blocks, firstCompressionEntry, nextEntry,
						blockStart, 8, uint32(len(compressed)))

					w.l.elog.Printf("Saved compressed block of size %d bytes from %d bytes.\n",
						len(compressed), w.writeBuf.Len())
				} else {
					w.l.elog.Printf("Flushing withour compression %v\n", err)
					flushWithoutCompression()
				}
				firstCompressionEntry = nextEntry
			}
			if p.Offset+p.Len > uint32(LogSize) {
				w.l.elog.Printf("Saving from %+v to %+v of total size: %d",
					firstCompressionEntry, nextEntry, w.writeBuf.Len())
				save()
				w.l.elog.Printf("Done")
			}
		}
	}
	save()
}
