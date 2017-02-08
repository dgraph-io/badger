package db

import (
	//	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

type debugHandler struct {
	out [][]string
}

func (s *debugHandler) Put(key []byte, val []byte) {
	s.out = append(s.out, []string{string(key), string(val)})
}

func (s *debugHandler) Delete(key []byte) {
	s.out = append(s.out, []string{string(key)})
}

func TestWriteBatchBasic(t *testing.T) {
	wb := NewWriteBatch(0)
	wb.Put([]byte("keya"), []byte("vala"))
	require.EqualValues(t, 1, wb.Count())
	wb.Put([]byte("keyb"), []byte("valb"))
	require.EqualValues(t, 2, wb.Count())
	wb.Delete([]byte("keyc"))
	require.EqualValues(t, 3, wb.Count())

	h := new(debugHandler)
	wb.Iterate(h)
	require.EqualValues(t,
		[][]string{
			{"keya", "vala"},
			{"keyb", "valb"},
			{"keyc"},
		},
		h.out)
}

func TestWriteBatchAppend(t *testing.T) {
	wb1 := NewWriteBatch(0)
	wb2 := NewWriteBatch(0)
	wb1.Put([]byte("keya"), []byte("vala"))
	wb2.Put([]byte("keyb"), []byte("valb"))
	wb2.Delete([]byte("keyc"))
	wb1.Append(wb2)
	h := new(debugHandler)
	wb1.Iterate(h)
	require.EqualValues(t,
		[][]string{
			{"keya", "vala"},
			{"keyb", "valb"},
			{"keyc"},
		},
		h.out)
}
