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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/x"
)

// The MANIFEST file describes the startup state of the db -- all value log files and LSM files,
// and their sizes (except the last value log file), what level they're at, key range info for each
// LSM file.
type manifest struct {
	levels        []levelManifest
	tables        map[uint64]tableManifest
	valueLogFiles map[uint64]struct{}
}

func createManifest(maxLevels int) manifest {
	levels := make([]levelManifest, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i].tables = make(map[uint64]struct{})
	}
	return manifest{
		levels:        levels,
		tables:        make(map[uint64]tableManifest),
		valueLogFiles: make(map[uint64]struct{}),
	}
}

type levelManifest struct {
	tables map[uint64]struct{} // Set of table id's
}

type tableManifest struct {
	level uint8
}

// manifestFile holds the file pointer (and other info) about the manifest file, which is a log
// file we append to.
type manifestFile struct {
	fp *os.File

	appendLock sync.Mutex
}

const (
	tableCreate = 1
	tableDelete = 2
)

// If we change a table's level, we just do a delete followed by a create.  (In the same changeset,
// so that they're atomically applied, of course.)
type tableChange struct {
	id    uint64
	op    byte  // has value tableCreate, or tableDelete
	level uint8 // set if tableCreate
}

type valueLogChange struct {
	// TODO: create/delete op?
	id uint64
}

type manifestChangeType byte

const (
	manifestTableChange    = 0
	manifestValueLogChange = 1
)

type manifestChange struct {
	tag manifestChangeType
	tc  tableChange
	vlc valueLogChange
}

type manifestChangeSet struct {
	changes []manifestChange
}

func openOrCreateManifestFile(opt *Options) (ret *manifestFile, result manifest, err error) {
	path := filepath.Join(opt.Dir, "MANIFEST")
	fp, err := y.OpenSyncedFile(path, false) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		return nil, manifest{}, err
	}

	m, err := replayManifestFile(opt.MaxLevels, fp)
	if err != nil {
		_ = fp.Close()
		return nil, manifest{}, err
	}

	return &manifestFile{fp: fp}, m, nil
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}

// addChanges writes a batch of changes, atomically, to the file.  By "atomically" that means when
// we replay the MANIFEST file, we'll either replay all the changes or none of them.  (The truth of
// this depends on the filesystem -- some might append garbage data if a system crash happens at
// the wrong time.)
func (mf *manifestFile) addChanges(changes manifestChangeSet) error {
	var buf bytes.Buffer
	changes.Encode(&buf)
	// Maybe we could use O_APPEND instead (on certain file systems)
	mf.appendLock.Lock()
	_, err := mf.fp.Write(buf.Bytes())
	mf.appendLock.Unlock()
	if err != nil {
		return err
	}
	return mf.fp.Sync()
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

func replayManifestFile(maxLevels int, fp *os.File) (ret manifest, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	offset := r.count

	build := createManifest(maxLevels)
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
		if err := applyChangeSet(&build, &changeSet); err != nil {
			return manifest{}, err
		}
	}

	// Truncate file so we don't have a half-written entry at the end.
	fp.Truncate(offset)

	_, err = fp.Seek(0, os.SEEK_END)
	return build, err
}

func applyTableChange(build *manifest, tc *tableChange) error {
	switch tc.op {
	case tableCreate:
		if _, ok := build.tables[tc.id]; ok {
			return x.Errorf("MANIFEST invalid, table %d exists\n", tc.id)
		}
		build.tables[tc.id] = tableManifest{
			level: tc.level}
		build.levels[tc.level].tables[tc.id] = struct{}{}
	case tableDelete:
		tm, ok := build.tables[tc.id]
		if !ok {
			return x.Errorf("MANIFEST removes non-existing table %d\n", tc.id)
		}
		delete(build.levels[tm.level].tables, tc.id)
		delete(build.tables, tc.id)
	default:
		return x.Errorf("MANIFEST file has invalid tableChange op\n")
	}
	return nil
}

func applyValueLogChange(build *manifest, vlc *valueLogChange) error {
	if _, ok := build.valueLogFiles[vlc.id]; ok {
		return x.Errorf("MANIFEST invalid, value log %d exists\n", vlc.id)
	}
	build.valueLogFiles[vlc.id] = struct{}{}
	return nil
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *manifest, changeSet *manifestChangeSet) error {
	for _, change := range changeSet.changes {
		switch change.tag {
		case manifestTableChange:
			if err := applyTableChange(build, &change.tc); err != nil {
				return err
			}
		case manifestValueLogChange:
			if err := applyValueLogChange(build, &change.vlc); err != nil {
				return err
			}
		}
	}
	return nil
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
	mcs.changes = changes
	return nil
}

func (mc *manifestChange) Encode(w *bytes.Buffer) {
	w.WriteByte(byte(mc.tag))
	switch mc.tag {
	case manifestTableChange:
		mc.tc.Encode(w)
	case manifestValueLogChange:
		mc.vlc.Encode(w)
	}
}

func (mc *manifestChange) Decode(r *countingReader) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch b {
	case manifestTableChange:
		mc.tag = manifestTableChange
		return mc.tc.Decode(r)
	case manifestValueLogChange:
		mc.tag = manifestValueLogChange
		return mc.vlc.Decode(r)
	default:
		return fmt.Errorf("invalid manifestChange byte")
	}
}

func (tc *tableChange) Encode(w *bytes.Buffer) {
	var bytes [10]byte
	binary.BigEndian.PutUint64(bytes[0:8], tc.id)
	bytes[8] = tc.op
	bytes[9] = tc.level
	w.Write(bytes[:])
}

func (tc *tableChange) Decode(r io.Reader) error {
	var bytes [10]byte
	if _, err := io.ReadFull(r, bytes[:]); err != nil {
		return err
	}
	tc.id = binary.BigEndian.Uint64(bytes[0:8])
	tc.op = bytes[8]
	tc.level = bytes[9]
	return nil
}

func (vlc *valueLogChange) Encode(w *bytes.Buffer) {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[0:8], vlc.id)
	w.Write(bytes[:])
}

func (vlc *valueLogChange) Decode(r io.Reader) error {
	var bytes [8]byte
	if _, err := io.ReadFull(r, bytes[:]); err != nil {
		return err
	}
	vlc.id = binary.BigEndian.Uint64(bytes[0:8])
	return nil
}
