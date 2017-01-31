package table

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type keyOffset struct {
	key    []byte
	offset int64
}

type Table struct {
	sync.Mutex

	offset int64
	fd     *os.File

	blockIndex []keyOffset
}

type byKey []keyOffset

func (b byKey) Len() int               { return len(b) }
func (b byKey) Swap(i int, j int)      { b[i], b[j] = b[j], b[i] }
func (b byKey) Less(i int, j int) bool { return bytes.Compare(b[i].key, b[j].key) < 0 }

func NewTable(fd *os.File, offset int64) *Table {
	t := &Table{
		fd:     fd,
		offset: offset,
	}
	return t
}

func (t *Table) ReadIndex() error {
	buf := make([]byte, 4)
	if _, err := t.fd.ReadAt(buf, t.offset+tableSize-4); err != nil {
		return errors.Wrap(err, "While reading block index")
	}
	restartsLen := int(binary.BigEndian.Uint32(buf))

	buf = make([]byte, 4*restartsLen)
	if _, err := t.fd.ReadAt(buf, t.offset+tableSize-int64(len(buf))); err != nil {
		return errors.Wrap(err, "While reading block index")
	}

	offsets := make([]uint32, restartsLen)
	for i := 0; i < restartsLen; i++ {
		offsets = append(offsets, binary.BigEndian.Uint32(buf[:4]))
		buf = buf[4:]
	}

	che := make(chan error, len(offsets))
	for i := range offsets {
		go func(o int64) {
			buf := make([]byte, 6)
			if _, err := t.fd.ReadAt(buf, t.offset+o); err != nil {
				che <- errors.Wrap(err, "While reading first header in block")
				return
			}
			// x.Assertf h.plen == 0
			var h header
			h.Decode(buf)
			buf = make([]byte, h.klen)
			if _, err := t.fd.ReadAt(buf, t.offset+o+6); err != nil {
				che <- errors.Wrap(err, "While reading first key in block")
				return
			}
			ko := keyOffset{
				key:    buf,
				offset: o,
			}
			t.Lock()
			t.blockIndex = append(t.blockIndex, ko)
			t.Unlock()
			che <- nil
		}(int64(offsets[i]))
	}

	for _ = range offsets {
		err := <-che
		if err != nil {
			return err
		}
	}
	sort.Sort(byKey(t.blockIndex))

	return nil
}
