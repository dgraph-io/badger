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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/y"
)

// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're
// at.
//
// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically,
// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
// reconstruct the manifest at startup.

type manifest struct {
	levels []levelManifest
	tables map[uint64]tableManifest

	// Contains total number of creation and deletion changes in the manifest -- used to compute
	// whether it'd be useful to rewrite the manifest.
	creations int
	deletions int
}

func createManifest() manifest {
	levels := make([]levelManifest, 0)
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

	// Guards appends, which includes access to the manifest field.
	appendLock sync.Mutex

	// Used to track the current state of the manifest, used when rewriting.
	manifest manifest
}

const (
	manifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 100000
	manifestDeletionsRatio            = 10
)

func OpenOrCreateManifestFile(dir string) (ret *manifestFile, result manifest, err error) {
	return helpOpenOrCreateManifestFile(dir, manifestDeletionsRewriteThreshold)
}

func helpOpenOrCreateManifestFile(dir string, deletionsThreshold int) (ret *manifestFile, result manifest, err error) {
	path := filepath.Join(dir, manifestFilename)
	fp, err := y.OpenSyncedFile(path, false) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		return nil, manifest{}, err
	}

	m1, m2, truncOffset, err := ReplayManifestFile(fp)

	// Truncate file so we don't have a half-written entry at the end.
	if err := fp.Truncate(truncOffset); err != nil {
		_ = fp.Close()
		return nil, manifest{}, err
	}

	if _, err = fp.Seek(0, os.SEEK_END); err != nil {
		_ = fp.Close()
		return nil, manifest{}, err
	}

	return &manifestFile{fp: fp, directory: dir, manifest: m1}, m2, nil
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}

// addChanges writes a batch of changes, atomically, to the file.  By "atomically" that means when
// we replay the MANIFEST file, we'll either replay all the changes or none of them.  (The truth of
// this depends on the filesystem -- some might append garbage data if a system crash happens at
// the wrong time.)
func (mf *manifestFile) addChanges(changes protos.ManifestChangeSet) error {
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

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
		var lenbuf [4]byte
		binary.BigEndian.PutUint32(lenbuf[:], uint32(len(buf)))
		buf = append(lenbuf[:], buf...)
		if _, err := mf.fp.Write(buf); err != nil {
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
	changes := make([]*protos.ManifestChange, 0, netCreations)
	for id, tm := range mf.manifest.tables {
		changes = append(changes, makeTableCreateChange(id, int(tm.level)))
	}
	set := protos.ManifestChangeSet{Changes: changes}

	buf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return err
	}
	var lenbuf [4]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(buf)))
	if _, err := fp.Write(append(lenbuf[:], buf...)); err != nil {
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

// ReplayManifestFile reads the manifest file and constructs two manifest objects.  (We need one
// immutable copy and one mutable copy of the manifest.  Easiest way is to construct two of them.)
// Also, returns the last offset after a completely read manifest entry -- the file must be
// truncated at that point before further appends are made (if there is a partial entry after
// that).  In normal conditions, truncOffset is the file size.
func ReplayManifestFile(fp *os.File) (ret1 manifest, ret2 manifest, truncOffset int64, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	offset := r.count

	build1 := createManifest()
	build2 := createManifest()
	for {
		offset = r.count
		var lenbuf [4]byte
		_, err := io.ReadFull(&r, lenbuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return manifest{}, manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenbuf[:])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(&r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return manifest{}, manifest{}, 0, err
		}

		var changeSet protos.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return manifest{}, manifest{}, 0, err
		}

		if err := applyChangeSet(&build1, &changeSet); err != nil {
			return manifest{}, manifest{}, 0, err
		}
		if err := applyChangeSet(&build2, &changeSet); err != nil {
			return manifest{}, manifest{}, 0, err
		}
	}

	return build1, build2, offset, err
}

func applyManifestChange(build *manifest, tc *protos.ManifestChange) error {
	switch tc.Op {
	case protos.ManifestChange_CREATE:
		if _, ok := build.tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.tables[tc.Id] = tableManifest{
			level: uint8(tc.Level),
		}
		for len(build.levels) <= int(tc.Level) {
			build.levels = append(build.levels, levelManifest{make(map[uint64]struct{})})
		}
		build.levels[tc.Level].tables[tc.Id] = struct{}{}
		build.creations++
	case protos.ManifestChange_DELETE:
		tm, ok := build.tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.levels[tm.level].tables, tc.Id)
		delete(build.tables, tc.Id)
		build.deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *manifest, changeSet *protos.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func makeTableCreateChange(id uint64, level int) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id:    id,
		Op:    protos.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

func makeTableDeleteChange(id uint64) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id: id,
		Op: protos.ManifestChange_DELETE,
	}
}
