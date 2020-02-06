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
	"unsafe"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/pkg/errors"
)

const blobFileSuffix = ".blob"

type blobFile struct {
	path     string
	fid      uint32
	fd       *os.File
	fileSize uint32

	//mappingSize    uint32
	//mmap           []byte
	//mappingEntries []mappingEntry

	// only accessed by gcHandler
	//totalDiscard uint32
}

type blobFileBuilder struct {
	dir string
	fid uint32
	buf *bytes.Buffer
}

type blobPointer struct {
	logicalAddr
	length uint32
}

func (bp *blobPointer) decode(val []byte) {
	ptr := (*blobPointer)(unsafe.Pointer(&val[0]))
	*bp = *ptr
}

type logicalAddr struct {
	fid    uint32
	offset uint32
}

//type mappingEntry struct {
//	logicalAddr
//	physicalOffset uint32
//}

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

func newBlobFileBuilder(fid uint32, dir string) *blobFileBuilder {
	return &blobFileBuilder{
		dir: dir,
		fid: fid,
		buf: &bytes.Buffer{},
	}
}

func (bfb *blobFileBuilder) addValue(value []byte) (bp []byte, err error) {

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(value)))

	if _, err = bfb.buf.Write(lenBuf[:]); err != nil {
		return nil, err
	}
	// Store offset from the point where the value starts. This is intentional.
	offset := uint32(bfb.buf.Len())
	if _, err = bfb.buf.Write(value); err != nil {
		return nil, err
	}
	bp = make([]byte, 12)
	binary.LittleEndian.PutUint32(bp, bfb.fid)
	binary.LittleEndian.PutUint32(bp[4:], offset)
	binary.LittleEndian.PutUint32(bp[8:], uint32(len(value)))
	return
}

func (bfb *blobFileBuilder) finish() (*blobFile, error) {
	name := filepath.Join(bfb.dir, newBlobFileName(bfb.fid))
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	if _, err := file.Write(bfb.buf.Bytes()); err != nil {
		return nil, err
	}
	_ = file.Close()
	return newBlobFile(file.Name(), bfb.fid, uint32(len(bfb.buf.Bytes())))
}

func newBlobFile(path string, fid, fileSize uint32) (*blobFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	if _, err = file.Seek(0, 2); err != nil {
		return nil, err
	}
	return &blobFile{
		path:     path,
		fid:      fid,
		fd:       file,
		fileSize: fileSize,
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
		blobFile, err := newBlobFile(path, fid, uint32(fileInfo.Size()))
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
		_ = f.fd.Close()
	}
	bm.fileList = nil
	bm.filesLock.Unlock()
}

func (bm *blobManager) dropAll() error {
	bm.filesLock.Lock()
	for _, f := range bm.fileList {
		_ = f.fd.Close()
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
	var bp blobPointer
	bp.decode(ptr)

	bm.filesLock.RLock()
	bf, ok := bm.fileList[bp.fid]
	bm.filesLock.RUnlock()

	if !ok {
		return nil, errors.Errorf("blob file %d not found", bp.fid)
	}
	buf := s.Resize(int(bp.length))
	n, err := bf.fd.ReadAt(buf, int64(bp.offset))
	if err != nil {
		return nil, err
	}
	if uint32(n) != bp.length {
		return nil, errors.New("read error")
	}
	return buf, nil
}

func (bm *blobManager) addFile(file *blobFile) {
	bm.filesLock.Lock()
	bm.fileList[file.fid] = file
	bm.filesLock.Unlock()
}
