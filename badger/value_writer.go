package badger

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bkaradzic/go-lz4"

	"github.com/dgraph-io/badger/y"
)

const (
	optimalLz4BlockSize  = 64 * (1 << 10) // 64 KB
	compressionThreshold = 1.0            // We compress values only if compression provides specified space gain.
)

type valueLogWriter struct {
	l     *valueLog
	curlf *logFile
}

func newWriter(l *valueLog) *valueLogWriter {
	writer := &valueLogWriter{l: l}

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

func next(e entryPosition, blocks []*block) entryPosition {
	if uint16(len(blocks[e.block].Entries)) == e.id-1 {
		// last entry in block
		return entryPosition{e.block + 1, 0}
	}
	return entryPosition{e.block, e.id + 1}
}

func (w *valueLogWriter) newFile() (newlf *logFile) {
	var err error
	w.curlf.doneWriting()

	newlf = &logFile{fid: atomic.AddInt32(&w.l.maxFid, 1), offset: 0}
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

	w.curlf.offset += int64(n)

	if w.curlf.offset > LogSize {
		w.curlf = w.newFile()
	}
	return
}

// Saves compressed block of entries to curlf and updated appriopriate poitners.
// Resets the buffer and updates curlf.offset.

// Traverses blocks entries between from and to (excluding to) and sets
// BlockOffset and InsideBlockOffset values.
func (w *valueLogWriter) updatePointers(blocks []*block, from, to entryPosition,
	blockStart uint64, blockLen uint32) {
	beforeTo := func(e entryPosition) bool {
		return e.block < to.block || (e.block == to.block && e.id < to.id)
	}

	for pos := from; beforeTo(pos); pos = next(pos, blocks) {
		ptr := &blocks[pos.block].Ptrs[pos.id]

		ptr.InsideBlockOffset = uint16(ptr.Offset - blockStart)
		ptr.Offset = blockStart
		ptr.Len = blockLen
		ptr.Meta = ptr.Meta | BitCompressed
	}
}

func (w *valueLogWriter) write(blocks []*block) {
	var firstBufferEntry entryPosition

	useCompression := true

	for i := range blocks {
		b := blocks[i]
		b.Ptrs = b.Ptrs[:0]
		for j := range b.Entries {
			e := b.Entries[j]

			var p valuePointer
			p.Fid = uint32(w.curlf.fid)
			p.Len = uint32(8 + len(e.Key) + len(e.Value) + 1)
			p.Offset = uint64(w.curlf.offset) + uint64(w.l.buf.Len())
			b.Ptrs = append(b.Ptrs, p)

			e.EncodeTo(&w.l.buf)
			if useCompression && w.l.buf.Len() > optimalLz4BlockSize {
				compressed, err := compress(w.l.buf.Bytes())

				if err == nil {
					w.l.elog.Printf("Writing compressed block.")
					blockStart := uint64(w.curlf.offset)
					err := w.saveToDisk(compressed)
					fmt.Printf("Saved compressed block of size %d bytes from %d bytes.\n", len(compressed), w.l.buf.Len())

					if err != nil {
						y.Fatalf("Unable to write to value log: %v", err)
					}
					w.l.buf.Reset()

					next := next(entryPosition{uint16(i), uint16(j)}, blocks)
					w.updatePointers(blocks, firstBufferEntry, next, blockStart, uint32(len(compressed)))

					w.l.elog.Printf("Saved compressed block of size %d bytes from %d bytes.\n", len(compressed), w.l.buf.Len())

					firstBufferEntry = next
				} else {
					// We are not going to try compression for this file again.
					fmt.Println(err)
					useCompression = false
				}
			}
			if p.Offset > uint64(LogSize) {
				afterLast := next(entryPosition{uint16(i), uint16(j)}, blocks)
				w.l.elog.Printf("Flushing from %+v to %+v of total size: %d",
					firstBufferEntry, afterLast, w.l.buf.Len())
				err := w.saveToDisk(w.l.buf.Bytes())
				y.Checkf(err, "unable to write to value log: %v", err)
				w.l.buf.Reset()
				w.l.elog.Printf("Done")
				useCompression = true
			}
		}
	}
	err := w.saveToDisk(w.l.buf.Bytes())
	y.Checkf(err, "unable to write to value log: %v", err)
	w.l.buf.Reset()
}
