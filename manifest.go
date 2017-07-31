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
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/dgraph-io/badger/y"
)

// The MANIFEST file describes the startup state of the db -- all value log files and LSM files,
// and their sizes (except the last value log file), what level they're at, key range info for each
// LSM file.
type manifest struct {
	levels []levelManifest
}

type levelManifest struct {
	tables map[uint64]tableManifest // Maps table id to manifest info
}

type tableManifest struct {
	tableSize         uint64 // Filesize
	smallest, biggest []byte // Smallest and largest keys, just like in Table
}

// manifestFile holds the file pointer (and other info) about the manifest file, which is a log
// file we append to.
type manifestFile struct {
	fp *os.File
}

const (
	tableCreate   = 1
	tableSetLevel = 2
	tableDelete   = 3
)

type tableChange struct {
	id                uint64
	op                byte   // has value tableCreate, tableSetLevel, or tableDelete
	level             uint8  // set if tableCreate, tableSetLevel
	smallest, biggest []byte // set if tableCreate
}

type manifestChange struct {
	tc tableChange
}

type manifestChangeSet struct {
	changes []manifestChange
}

func openManifestFile(path string) (ret *manifestFile, result manifest, err error) {
	fp, err := y.OpenSyncedFile(path, true)
	if err != nil {
		return nil, manifest{}, err
	}

	m, err := replayManifestFile(fp)
	if err != nil {
		_ = fp.Close()
		return nil, manifest{}, err
	}

	return &manifestFile{fp}, m, nil
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}

type countingReader struct {
	wrapped *bufio.Reader
	count   int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

func (r *countingReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

func replayManifestFile(fp *os.File) (ret manifest, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	offset := r.count

	var build manifest
	for {
		offset = r.count
		var changeSet manifestChangeSet
		err := changeSet.Decode(&r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return manifest{}, err
		}
		applyChangeSet(&build, &changeSet)
	}

	// Truncate file so we don't have a half-written entry at the end.
	fp.Truncate(offset)

	_, err = fp.Seek(0, os.SEEK_END)
	return manifest{}, err
}

func applyChangeSet(build *manifest, changeSet *manifestChangeSet) {
	// TODO: Do something.
}

func (mcs *manifestChangeSet) Encode(w *bytes.Buffer) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(len(mcs.changes)))
	w.Write(b[:n])
	for _, change := range mcs.changes {
		change.Encode(w)
	}
}

func (mcs *manifestChangeSet) Decode(r *countingReader) error {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	changes := make([]manifestChange, n)
	for i := uint64(0); i < n; i++ {
		if err := changes[i].Decode(r); err != nil {
			return err
		}
	}
	return nil
}

func (mc *manifestChange) Encode(w *bytes.Buffer) {
	mc.tc.Encode(w)
}

func (mc *manifestChange) Decode(r io.Reader) error {
	return mc.tc.Decode(r)
}

// TODO: How do we know keys have 16 bit size?  We need to encapsulate that somehow.
func encodeKey(w *bytes.Buffer, x []byte) {
	var size [2]byte
	binary.BigEndian.PutUint16(size[:], uint16(len(x)))
	w.Write(size[:])
	w.Write(x)
}

func decodeKey(r io.Reader) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(size[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (tc *tableChange) Encode(w *bytes.Buffer) {
	var bytes [10]byte
	binary.BigEndian.PutUint64(bytes[:8], tc.id)
	bytes[8] = tc.op
	bytes[9] = tc.level
	w.Write(bytes[:])
	encodeKey(w, tc.smallest)
	encodeKey(w, tc.biggest)
}

func (tc *tableChange) Decode(r io.Reader) error {
	var bytes [10]byte
	if _, err := io.ReadFull(r, bytes[:]); err != nil {
		return err
	}
	tc.id = binary.BigEndian.Uint64(bytes[:8])
	tc.op = bytes[8]
	tc.level = bytes[9]
	var err error
	if tc.smallest, err = decodeKey(r); err != nil {
		return err
	}
	if tc.biggest, err = decodeKey(r); err != nil {
		return err
	}
	return nil
}
