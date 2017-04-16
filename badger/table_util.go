package badger

import (
	"encoding/binary"
	"io/ioutil"
	"sync/atomic"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// reserveFileIDs reserve k fileIDs. Returns pair is a half-interval. For example,
// (3, 10] means 4, 5, ..., 10. It has 7 IDs.
func (s *levelsController) reserveFileIDs(k int) (uint64, uint64) {
	id := atomic.AddUint64(&s.maxFileID, uint64(k))
	return id - uint64(k), id
}

//// newFile returns a unique filename.
//func (s *levelsController) newFile() *os.File {
//	for {
//		id := atomic.AddUint64(&s.maxFileID, 1)
//		filename := table.NewFilename(id, s.kv.opt.Dir)
//		if _, err := os.Stat(filename); os.IsNotExist(err) {
//			// File does not exist.
//			fd, err := y.OpenSyncedFile(filename)
//			y.Check(err)
//			return fd
//		}
//	}
//}

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

func (s *levelsController) reserveCompactID() uint64 {
	return atomic.AddUint64(&s.maxCompactID, 1)
}

func getIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := ioutil.ReadDir(dir)
	y.Check(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID, ok := table.ParseFileID(info.Name())
		if !ok {
			continue
		}
		idMap[fileID] = struct{}{}
	}
	return idMap
}
