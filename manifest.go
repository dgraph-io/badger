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
)

// The MANIFEST file describes the startup state of the db -- all value log files and LSM files,
// and their sizes (except the last value log file), what level they're at, key range info for each
// LSM file.
type manifest struct {
	levels []levelManifest
	tables map[uint64]tableManifest

	// Contains total number of creation and deletion changes in the manifest -- used to compute
	// whether it'd be useful to rewrite the manifest.
	creations int
	deletions int
}

func createManifest(maxLevels int) manifest {
	levels := make([]levelManifest, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i].tables = make(map[uint64]struct{})
	}
	return manifest{
		levels: levels,
		tables: make(map[uint64]tableManifest),
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
	fp        *os.File
	directory string
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int

	manifest manifest

	// Guards appends, which includes access to creations/deletions.
	appendLock sync.Mutex
}

const (
	tableCreate = 1
	tableDelete = 2
)

// If we change a table's level, we just do a delete followed by a create.  (In the same changeset,
// so that they're atomically applied, of course.)
type manifestChange struct {
	id    uint64
	op    byte  // has value tableCreate, or tableDelete
	level uint8 // set if tableCreate
}

type manifestChangeSet struct {
	changes []manifestChange
}

const (
	manifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 100000
	manifestDeletionsRatio            = 10
)

func openOrCreateManifestFile(opt *Options) (ret *manifestFile, result manifest, err error) {
	return helpOpenOrCreateManifestFile(opt, manifestDeletionsRewriteThreshold)
}

func helpOpenOrCreateManifestFile(opt *Options, deletionsThreshold int) (ret *manifestFile, result manifest, err error) {
	path := filepath.Join(opt.Dir, manifestFilename)
	fp, err := y.OpenSyncedFile(path, false) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		return nil, manifest{}, err
	}

	m1, m2, err := replayManifestFile(opt.MaxLevels, fp)
	if err != nil {
		_ = fp.Close()
		return nil, manifest{}, err
	}

	return &manifestFile{fp: fp, directory: opt.Dir, manifest: m1}, m2, nil
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
	if err := applyChangeSet(&mf.manifest, &changes); err != nil {
		mf.appendLock.Unlock()
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.deletions > mf.deletionsRewriteThreshold &&
		mf.manifest.deletions > manifestDeletionsRatio*(mf.manifest.creations-mf.manifest.deletions) {
		if err := mf.rewrite(); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	} else {
		if _, err := mf.fp.Write(buf.Bytes()); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	}

	mf.appendLock.Unlock()
	return mf.fp.Sync()
}

// Must be called while appendLock is held.
func (mf *manifestFile) rewrite() error {
	// We explicitly sync.
	rewritePath := filepath.Join(mf.directory, manifestRewriteFilename)
	fp, err := y.OpenTruncFile(rewritePath, false)
	if err != nil {
		return err
	}
	netCreations := len(mf.manifest.tables)
	changes := make([]manifestChange, 0, netCreations)
	for id, tm := range mf.manifest.tables {
		changes = append(changes, manifestChange{
			id:    id,
			op:    tableCreate,
			level: tm.level,
		})
	}
	set := manifestChangeSet{changes: changes}

	var buf bytes.Buffer
	set.Encode(&buf)
	if _, err := fp.Write(buf.Bytes()); err != nil {
		fp.Close()
		return err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return err
	}
	mf.manifest.creations = netCreations
	mf.manifest.deletions = 0
	if err := os.Rename(rewritePath, filepath.Join(mf.directory, manifestFilename)); err != nil {
		fp.Close()
		return err
	}
	mf.fp.Close()
	mf.fp = fp
	if err := syncDir(mf.directory); err != nil {
		return err
	}
	return nil
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

// We need one immutable copy and one mutable copy of the manifest.  Easiest way is to construct
// two of them.
func replayManifestFile(maxLevels int, fp *os.File) (ret1 manifest, ret2 manifest, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	offset := r.count

	build1 := createManifest(maxLevels)
	build2 := createManifest(maxLevels)
	for {
		offset = r.count
		var changeSet manifestChangeSet
		err := changeSet.Decode(&r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return manifest{}, manifest{}, err
		}
		if err := applyChangeSet(&build1, &changeSet); err != nil {
			return manifest{}, manifest{}, err
		}
		if err := applyChangeSet(&build2, &changeSet); err != nil {
			return manifest{}, manifest{}, err
		}
	}

	// Truncate file so we don't have a half-written entry at the end.
	fp.Truncate(offset)

	_, err = fp.Seek(0, os.SEEK_END)
	return build1, build2, err
}

func applyManifestChange(build *manifest, tc *manifestChange) error {
	switch tc.op {
	case tableCreate:
		if _, ok := build.tables[tc.id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.id)
		}
		build.tables[tc.id] = tableManifest{
			level: tc.level}
		build.levels[tc.level].tables[tc.id] = struct{}{}
		build.creations++
	case tableDelete:
		tm, ok := build.tables[tc.id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.id)
		}
		delete(build.levels[tm.level].tables, tc.id)
		delete(build.tables, tc.id)
		build.deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *manifest, changeSet *manifestChangeSet) error {
	for _, change := range changeSet.changes {
		if err := applyManifestChange(build, &change); err != nil {
			return err
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

func (tc *manifestChange) Encode(w *bytes.Buffer) {
	var bytes [10]byte
	binary.BigEndian.PutUint64(bytes[0:8], tc.id)
	bytes[8] = tc.op
	bytes[9] = tc.level
	w.Write(bytes[:])
}

func (tc *manifestChange) Decode(r io.Reader) error {
	var bytes [10]byte
	if _, err := io.ReadFull(r, bytes[:]); err != nil {
		return err
	}
	tc.id = binary.BigEndian.Uint64(bytes[0:8])
	tc.op = bytes[8]
	if tc.op != tableCreate && tc.op != tableDelete {
		return fmt.Errorf("decoded invalid tableChange op")
	}
	tc.level = bytes[9]
	return nil
}

func makeTableCreateChange(id uint64, level int) manifestChange {
	return manifestChange{
		id:    id,
		op:    tableCreate,
		level: uint8(level),
	}
}

func makeTableDeleteChange(id uint64) manifestChange {
	return manifestChange{
		id: id,
		op: tableDelete,
	}
}
