package table

import (
	"bytes"
	"encoding/binary"
)

type header struct {
	plen uint16
	klen uint16
	vlen uint16
}

func (h header) Encode() []byte {
	b := make([]byte, 6)
	binary.BigEndian.PutUint16(b[0:2], h.plen)
	binary.BigEndian.PutUint16(b[2:4], h.klen)
	binary.BigEndian.PutUint16(b[4:6], h.vlen)
	return b
}

func (h *header) Decode(buf []byte) []byte {
	h.plen = binary.BigEndian.Uint16(buf[0:2])
	h.klen = binary.BigEndian.Uint16(buf[2:4])
	h.vlen = binary.BigEndian.Uint16(buf[4:6])
	return buf[6:]
}

type TableBuilder struct {
	RestartInterval int
	counter         int

	buf      *bytes.Buffer
	lastKey  []byte
	restarts []uint32
}

func (b *TableBuilder) Reset() {
	b.counter = 0
	b.buf.Reset()
	b.lastKey = []byte{}
	b.restarts = b.restarts[:0]
}

func keyDiff(newKey, lastKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(lastKey); i++ {
		if newKey[i] != lastKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *TableBuilder) Add(key, value []byte) {
	if b.counter >= b.RestartInterval {
		b.restarts = append(b.restarts, uint32(b.buf.Len()))
		b.counter = 0
		b.lastKey = []byte{}
	}

	diffKey := keyDiff(key, b.lastKey)
	if len(diffKey) == len(key) {
		b.lastKey = key
	}

	h := header{
		plen: uint16(len(key) - len(diffKey)),
		klen: uint16(len(diffKey)),
		vlen: uint16(len(value)),
	}
	b.buf.Write(h.Encode())
	b.buf.Write(diffKey)
	b.buf.Write(value)
}

func (b *TableBuilder) Len() int {
	return b.buf.Len() + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
}

func (b *TableBuilder) Data() []byte {
	return b.buf.Bytes()
}

func (b *TableBuilder) BlockIndex() []byte {
	// Store the end offset, so we know the length of the final block.
	b.restarts = append(b.restarts, uint32(b.buf.Len()))

	sz := 4*len(b.restarts) + 4
	out := make([]byte, sz)
	buf := out
	for _, r := range b.restarts {
		binary.BigEndian.PutUint32(buf[:4], r)
		buf = buf[4:]
	}
	binary.BigEndian.PutUint32(buf[:4], uint32(len(b.restarts)))
	return out
}
