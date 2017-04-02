/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
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
