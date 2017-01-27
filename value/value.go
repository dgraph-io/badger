package value

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/dgraph-io/dgraph/x"
	"github.com/pkg/errors"
)

type Log struct {
	x.SafeMutex
	fd *os.File
}

type Entry struct {
	Key   []byte
	Value []byte
}

type header struct {
	active byte
	klen   uint32
	vlen   uint32
}

func (h header) Encode() []byte {
	b := make([]byte, 1+4+4)
	b[0] = byte(0)
	binary.BigEndian.PutUint32(b[1:5], h.klen)
	binary.BigEndian.PutUint32(b[5:9], h.vlen)
	return b
}

func (h *header) Decode(buf []byte) []byte {
	h.active = buf[0]
	h.klen = binary.BigEndian.Uint32(buf[1:5])
	h.vlen = binary.BigEndian.Uint32(buf[5:9])
	return buf[9:]
}

type Pointer struct {
	Len    uint32
	Offset int64
}

func (l *Log) Open(fname string) {
	var err error
	l.fd, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0666)
	x.Check(err)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (l *Log) Write(entries []Entry) ([]Pointer, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	var h header
	ptrs := make([]Pointer, 0, len(entries))

	for _, e := range entries {
		h.active = byte(0)
		h.klen = uint32(len(e.Key))
		h.vlen = uint32(len(e.Value))
		header := h.Encode()

		var p Pointer
		p.Len = uint32(len(header)) + h.klen + h.vlen
		p.Offset = int64(buf.Len())
		ptrs = append(ptrs, p)

		buf.Write(header)
		buf.Write(e.Key)
		buf.Write(e.Value)
	}

	l.Lock()
	defer l.Unlock()

	off, err := l.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return ptrs, errors.Wrap(err, "Unable to seek")
	}
	for i := range ptrs {
		p := &ptrs[i]
		p.Offset += off
	}

	_, err = l.fd.Write(buf.Bytes())
	if err != nil {
		return ptrs, errors.Wrap(err, "Unable to write to file")
	}
	err = syscall.Fdatasync(int(l.fd.Fd()))
	return ptrs, errors.Wrap(err, "Unable to write to file")
}

func (l *Log) Read(offset int64, len uint32, fn func(Entry)) error {
	var e Entry
	buf := make([]byte, len)
	if _, err := l.fd.ReadAt(buf, offset); err != nil {
		return err
	}
	var h header
	buf = h.Decode(buf)
	e.Key = buf[0:h.klen]
	buf = buf[h.klen:]
	e.Value = buf[0:h.vlen]
	fn(e)
	return nil
}
