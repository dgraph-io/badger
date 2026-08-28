/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// buildFooterData lays out an SST footer of the form:
//
//	[index bytes][indexLen: 4 bytes][checksum bytes][checksumLen: 4 bytes]
//
// indexLen and checksumLen are written as BigEndian uint32 (matching
// y.U32ToBytes/y.BytesToU32), independently of the actual index/checksum byte
// lengths, so we can craft footers whose length fields disagree with reality.
func buildFooterData(indexLen uint32, index []byte, checksumLen uint32, checksum []byte) []byte {
	var b bytes.Buffer
	b.Write(index)
	b.Write(y.U32ToBytes(indexLen))
	b.Write(checksum)
	b.Write(y.U32ToBytes(checksumLen))
	return b.Bytes()
}

// newTestTable returns an in-memory Table over data without going through
// OpenTable/OpenInMemoryTable, so initIndex can be called directly.
func newTestTable(data []byte) *Table {
	return &Table{
		MmapFile:  &z.MmapFile{Data: data},
		tableSize: len(data),
		opt:       &Options{},
	}
}

func TestInitIndexFooterValidation(t *testing.T) {
	// A non-empty, well-formed checksum so the checksumLen check and
	// proto.Unmarshal both pass and we can reach the indexLen check.
	validChecksum, err := proto.Marshal(&pb.Checksum{Sum: 1})
	require.NoError(t, err)
	checksumLen := uint32(len(validChecksum))

	// The footer is read by walking backwards from EOF (readPos starts at
	// tableSize), so each length must fit in the bytes remaining *before*
	// readPos — not merely be smaller than the whole table. checksumLen == 0 is
	// legal (a zero checksum marshals to no bytes), so only the bounds are
	// checked for checksumLen; a zero indexLen is always corruption since a
	// table always has at least one block.
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "tableSize<4",
			data:    make([]byte, 2),
			wantErr: "invalid table size in footer. Data corrupted",
		},
		{
			// 8-byte file since checksumLen and indexLen are both uint32.
			// checksumLen field reads 8, exceeding the 4 bytes
			// remaining before readPos.
			name:    "checksumLen=tableSize",
			data:    buildFooterData(0, nil, 8, nil),
			wantErr: "invalid checksum length in footer. Data corrupted",
		},
		{
			name:    "checksumLen=tableSize-1",
			data:    buildFooterData(0, nil, 7, nil),
			wantErr: "invalid checksum length in footer. Data corrupted",
		},
		{
			name:    "checksumLen>tableSize",
			data:    buildFooterData(0, nil, 100, nil),
			wantErr: "invalid checksum length in footer. Data corrupted",
		},
		{
			// checksumLen=0 is legal, so it must fall through to the empty
			// index length related error.
			name:    "checksumLen=0 falls through to indexLen",
			data:    buildFooterData(0, nil, 0, nil),
			wantErr: "invalid index length in footer. Data corrupted",
		},
		{
			name:    "indexLen=0",
			data:    buildFooterData(0, nil, checksumLen, validChecksum),
			wantErr: "invalid index length in footer. Data corrupted",
		},
		{
			// tableSize = [indexLen:4] + [checksum:len(validChecksum)] + [checksumLen:4].
			name: "indexLen=tableSize",
			data: buildFooterData(
				uint32(4+len(validChecksum)+4), nil, checksumLen, validChecksum),
			wantErr: "invalid index length in footer. Data corrupted",
		},
		{
			name:    "indexLen>tableSize",
			data:    buildFooterData(1000, nil, checksumLen, validChecksum),
			wantErr: "invalid index length in footer. Data corrupted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestTable(tc.data).initIndex()
			require.Error(t, err)
			require.Equal(t, tc.wantErr, err.Error())
		})
	}
}

// TestInitBiggestAndSmallestRecoversFlatbuffersPanic verifies the core of the
// issue #2201 fix: when initIndex panics while parsing a corrupted index
// (panic #1, "P1"), initBiggestAndSmallest's recover defer must convert that
// panic into an error instead of re-panicking. The footer here is
// self-consistent — a valid checksum over a garbage index — so every
// validation check passes and the panic happens inside the flatbuffers parse,
// exactly like the original issue (checksum false-pass → GetRootAsTableIndex).
func TestInitBiggestAndSmallestRecoversFlatbuffersPanic(t *testing.T) {
	garbageIndex := bytes.Repeat([]byte{0xFF}, 20)
	checksum, err := proto.Marshal(&pb.Checksum{
		Algo: pb.Checksum_CRC32C,
		Sum:  y.CalculateChecksum(garbageIndex, pb.Checksum_CRC32C),
	})
	require.NoError(t, err)
	data := buildFooterData(
		uint32(len(garbageIndex)), garbageIndex, uint32(len(checksum)), checksum)

	err = newTestTable(data).initBiggestAndSmallest()
	require.Error(t, err)
	require.Contains(t, err.Error(), "initIndex crashed")
}
