package badger

import (
	"encoding/binary"
	"io/ioutil"
	"sync/atomic"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

// reserveFileIDs reserve k fileIDs. Returns pair is a half-interval.
// If we return [3, 6), it means use 3, 4, 5.
func (s *levelsController) reserveFileIDs(k int) (uint64, uint64) {
	id := atomic.AddUint64(&s.maxFileID, uint64(k))
	return id - uint64(k), id
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
