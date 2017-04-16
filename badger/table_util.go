package badger

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

const (
	filePrefix = "table_"
)

func newFilename(fileID uint64, dir string) string {
	return filepath.Join(dir, filePrefix+fmt.Sprintf("%010d", fileID))
}

// tempFile returns a unique filename and the uint64.
func (s *KV) newFile() *os.File {
	for {
		id := atomic.AddUint64(&s.maxFileID, 1)
		filename := newFilename(id, s.opt.Dir)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			// File does not exist.
			fd, err := y.OpenSyncedFile(filename)
			y.Check(err)
			return fd
		}
	}
}

// updateLevel is called only when moving table to the next level, when there is no overlap
// with the next level. Here, we update the table metadata.
func updateLevel(t *table.Table, newLevel int) error {
	var metadata [2]byte
	binary.BigEndian.PutUint16(metadata[:], uint16(newLevel))
	if err := t.SetMetadata(metadata[:]); err != nil {
		return err
	}
	return nil
}
