package y

import "encoding/binary"

type Buffer struct {
	buf    []byte
	offset int
}

func NewBuffer(sz int) *Buffer {
	return &Buffer{
		buf:    Calloc(sz),
		offset: 0,
	}
}

func (b *Buffer) Len() int {
	return b.offset
}

func (b *Buffer) Bytes() []byte {
	return b.buf[0:b.offset]
}

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 64

func (b *Buffer) Grow(n int) {
	// In this case, len and cap are the same.
	if len(b.buf) == 0 && n <= smallBufferSize {
		b.buf = Calloc(smallBufferSize)
		return
	} else if b.buf == nil {
		b.buf = Calloc(n)
		return
	}
	if b.offset+n < len(b.buf) {
		return
	}

	sz := 2*len(b.buf) + n
	newBuf := Calloc(sz)
	copy(newBuf, b.buf[:b.offset])
	Free(b.buf)
	b.buf = newBuf
}

// Allocate is not thread-safe. The byte slice returned MUST be used before further calls to Buffer.
func (b *Buffer) Allocate(n int) []byte {
	b.Grow(n)
	off := b.offset
	b.offset += n
	return b.buf[off:b.offset]
}

func (b *Buffer) WriteLen(sz int) {
	buf := b.Allocate(4)
	binary.BigEndian.PutUint32(buf, uint32(sz))
}

func (b *Buffer) AllocateSlice(sz int) []byte {
	b.Grow(4 + sz)
	b.WriteLen(sz)
	return b.Allocate(sz)
}

func (b *Buffer) SliceOffsets(offsets []int) []int {
	start := 0
	for start < b.offset {
		offsets = append(offsets, start)
		sz := binary.BigEndian.Uint32(b.buf[start:])
		start += 4 + int(sz)
	}
	return offsets
}

func (b *Buffer) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(b.buf[offset:])
	start := offset + 4
	return b.buf[start : start+int(sz)]
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	b.Grow(len(p))
	n = copy(b.buf[b.offset:], p)
	b.offset += n
	return n, nil
}

func (b *Buffer) Reset() {
	b.offset = 0
}

func (b *Buffer) Release() {
	Free(b.buf)
}
