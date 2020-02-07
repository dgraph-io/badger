/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/pkg/errors"
)

const blobFileSuffix = ".blob"

type blobFile struct {
	fid uint32
	fd  *os.File
}

func newBlobFileName(id uint32) string {
	return fmt.Sprintf("%06d", id) + blobFileSuffix
}

func parseBlobFileName(name string) (uint32, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, blobFileSuffix) {
		return 0, false
	}
	name = strings.TrimSuffix(name, blobFileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	y.AssertTrue(id >= 0)
	return uint32(id), true
}

// Finish finishes a blob file and opens the built blob file.
func (bfb *blobFileBuilder) finish() (*blobFile, error) {
	name := filepath.Join(bfb.dir, newBlobFileName(bfb.fid))
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "finish blob file: %d", bfb.fid)
	}
	if _, err := file.Write(bfb.buf.Bytes()); err != nil {
		return nil, errors.Wrapf(err, "failed to write to blob file: %d", bfb.fid)
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	return newBlobFile(file.Name(), bfb.fid)
}

func newBlobFile(path string, fid uint32) (*blobFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &blobFile{
		fid: fid,
		fd:  file,
	}, nil
}

// read reads the value writen at offset stored in the given blobpointer.
func (bf *blobFile) read(vp *valuePointer, s *y.Slice) ([]byte, error) {
	buf := s.Resize(int(vp.Len))
	n, err := bf.fd.ReadAt(buf, int64(vp.Offset))
	if err != nil {
		return nil, err
	}
	if uint32(n) != vp.Len {
		return nil, errors.Errorf("read %d bytes, expectd %d", n, vp.Len)
	}
	return buf, nil
}

type blobFileBuilder struct {
	dir string
	fid uint32
	buf *bytes.Buffer
}

func newBlobFileBuilder(fid uint32, dir string) *blobFileBuilder {
	return &blobFileBuilder{
		dir: dir,
		fid: fid,
		buf: &bytes.Buffer{},
	}
}

func (bfb *blobFileBuilder) addValue(value []byte) (*valuePointer, error) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := bfb.buf.Write(lenBuf[:]); err != nil {
		return nil, err
	}
	// Store offset from the point where the value starts. This is intentional.
	offset := uint32(bfb.buf.Len())
	if _, err := bfb.buf.Write(value); err != nil {
		return nil, err
	}
	return &valuePointer{
		Fid:    bfb.fid,
		Offset: offset,
		Len:    uint32(len(value)),
	}, nil
}

type blobManager struct {
	filesLock sync.RWMutex
	fileList  map[uint32]*blobFile
	maxFileID uint32
	dirPath   string
}

func (bm *blobManager) Open(db *DB, opt Options) error {
	// TODO - Figure out if this should be valueDir or just Dir.
	bm.dirPath = db.opt.Dir
	bm.fileList = make(map[uint32]*blobFile)

	fileInfos, err := ioutil.ReadDir(bm.dirPath)
	if err != nil {
		return errors.Wrapf(err, "Error while opening blob files")
	}
	for _, fileInfo := range fileInfos {
		fid, success := parseBlobFileName(fileInfo.Name())
		// If it is an sst or vlog, skip it.
		if !success {
			continue
		}

		path := filepath.Join(bm.dirPath, fileInfo.Name())
		blobFile, err := newBlobFile(path, fid)
		if err != nil {
			return err
		}
		bm.fileList[fid] = blobFile
	}
	return nil
}

func (bm *blobManager) close() {
	bm.filesLock.Lock()
	for _, f := range bm.fileList {
		y.Check(f.fd.Close())
	}
	bm.fileList = nil
	bm.filesLock.Unlock()
}

func (bm *blobManager) dropAll() error {
	bm.filesLock.Lock()
	for _, f := range bm.fileList {
		y.Check(f.fd.Close())
		if err := os.Remove(f.fd.Name()); err != nil {
			return err
		}
	}
	bm.fileList = make(map[uint32]*blobFile)
	bm.filesLock.Unlock()
	return nil
}
func (bm *blobManager) allocFileID() uint32 {
	return atomic.AddUint32(&bm.maxFileID, 1)
}

func (bm *blobManager) read(ptr []byte, s *y.Slice) ([]byte, error) {
	var vp valuePointer
	vp.Decode(ptr)

	bm.filesLock.RLock()
	bf, ok := bm.fileList[vp.Fid]
	bm.filesLock.RUnlock()

	if !ok {
		return nil, errors.Errorf("blob file %d not found", vp.Fid)
	}
	return bf.read(&vp, s)
}

func (bm *blobManager) addFile(file *blobFile) {
	bm.filesLock.Lock()
	bm.fileList[file.fid] = file
	bm.filesLock.Unlock()
}
