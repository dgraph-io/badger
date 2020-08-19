package y

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
