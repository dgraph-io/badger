package table

import (
	"bytes"
	"encoding/binary"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/x"
)

var tableSize int64 = 50 << 20

type header struct {
	plen int
	klen int
	vlen int
}

func (h header) Encode() []byte {
	b := make([]byte, 6)
	binary.BigEndian.PutUint16(b[0:2], uint16(h.plen))
	binary.BigEndian.PutUint16(b[2:4], uint16(h.klen))
	binary.BigEndian.PutUint16(b[4:6], uint16(h.vlen))
	return b
}

func (h *header) Decode(buf []byte) int {
	h.plen = int(binary.BigEndian.Uint16(buf[0:2]))
	h.klen = int(binary.BigEndian.Uint16(buf[2:4]))
	h.vlen = int(binary.BigEndian.Uint16(buf[4:6]))
	return 6
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
	if b.buf.Cap() < int(tableSize) {
		b.buf.Grow(int(tableSize) - b.buf.Cap())
	}
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

func (b *TableBuilder) Add(key, value []byte) error {
	if len(key)+len(value)+b.length() > int(tableSize) {
		return y.Errorf("Exceeds table size")
	}

	if b.counter >= b.RestartInterval {
		b.restarts = append(b.restarts, uint32(b.buf.Len()))
		b.counter = 0
		b.lastKey = []byte{}
	}

	// diffKey stores the difference of key with lastKey.
	diffKey := keyDiff(key, b.lastKey)
	if len(diffKey) == len(key) {
		b.lastKey = key
	}

	h := header{
		plen: len(key) - len(diffKey),
		klen: len(diffKey),
		vlen: len(value),
	}
	b.buf.Write(h.Encode())
	b.buf.Write(diffKey) // We only need to store the key difference.
	b.buf.Write(value)
	return nil
}

func (b *TableBuilder) length() int {
	return b.buf.Len() + 6 /* empty header */ + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
}

func (b *TableBuilder) Data() []byte {
	return b.buf.Bytes()
}

func (b *TableBuilder) blockIndex() []byte {
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

var emptySlice = make([]byte, 100)

func (b *TableBuilder) Finish() []byte {
	b.Add([]byte{}, []byte{}) // Empty record to indicate the end.

	index := b.blockIndex()
	empty := int(tableSize) - b.buf.Len() - len(index)
	x.AssertTrue(empty >= 0)

	n := empty / len(emptySlice)
	for i := 0; i < n; i++ {
		b.buf.Write(emptySlice)
	}
	empty -= n * len(emptySlice)
	for i := 0; i < empty; i++ {
		b.buf.WriteByte(0)
	}
	b.buf.Write(index)
	x.AssertTrue(b.buf.Len() == int(tableSize))
	return b.buf.Bytes()
}
