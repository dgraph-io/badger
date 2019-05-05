package badger

import (
	"bytes"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

type StreamWriter struct {
	db       *DB
	throttle *y.Throttle
}

func (sw *StreamWriter) Write(kvs *pb.KVList) {
}

type SortedWriter struct {
	db       *DB
	throttle *y.Throttle

	builder *table.Builder
	lastKey []byte
}

func (sw *StreamWriter) NewSSWriter() *SortedWriter {
	return &SortedWriter{
		db:       sw.db,
		throttle: sw.throttle,
		builder:  table.NewTableBuilder(),
	}
}

var ErrUnsortedKey = errors.New("Keys not in sorted order")

func (w *SortedWriter) Add(e *Entry) error {
	if bytes.Compare(e.Key, w.lastKey) < 0 {
		return ErrUnsortedKey
	}
	sameKey := y.SameKey(e.Key, w.lastKey)
	w.lastKey = y.SafeCopy(w.lastKey, e.Key)

	v := y.ValueStruct{
		Value:     e.Value,
		Meta:      e.meta,
		UserMeta:  e.UserMeta,
		ExpiresAt: e.ExpiresAt,
	}
	if err := w.builder.Add(e.Key, v); err != nil {
		return err
	}
	// Same keys should go into the same SSTable.
	if !sameKey && w.builder.ReachedCapacity(w.db.opt.MaxTableSize) {
		return w.send()
	}
	return nil
}

func (w *SortedWriter) send() error {
	data := w.builder.Finish()
	if err := w.throttle.Do(); err != nil {
		return err
	}
	go func() {
		err := w.createTable(data)
		w.throttle.Done(err)
	}()
	w.builder = table.NewTableBuilder()
	return nil
}

func (w *SortedWriter) Done() error {
	if w.builder.Empty() {
		return nil
	}
	return w.send()
}

func (w *SortedWriter) createTable(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	fileId := w.db.lc.reserveFileID()
	fd, err := y.CreateSyncedFile(table.NewFilename(fileId, w.db.opt.Dir), true)
	if err != nil {
		return err
	}
	if _, err := fd.Write(data); err != nil {
		return err
	}
	tbl, err := table.OpenTable(fd, w.db.opt.TableLoadingMode, nil)
	if err != nil {
		return err
	}
	lc := w.db.lc

	var lhandler *levelHandler
	for _, l := range lc.levels[1:] {
		ratio := float64(l.getTotalSize()) / float64(l.maxTotalSize)
		if ratio < 1.0 {
			lhandler = l
			break
		}
	}
	if lhandler == nil {
		lhandler = lc.levels[len(lc.levels)-1]
	}
	// Now that table can be opened successfully, let's add this to the MANIFEST.
	change := &pb.ManifestChange{
		Id:       tbl.ID(),
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(lhandler.level),
		Checksum: tbl.Checksum,
	}
	if err := w.db.manifest.addChanges([]*pb.ManifestChange{change}); err != nil {
		return err
	}
	if err := lhandler.replaceTables([]*table.Table{}, []*table.Table{tbl}); err != nil {
		return err
	}
	w.db.opt.Infof("Table created: %d at level: %d. Size: %s\n",
		fileId, lhandler.level, humanize.Bytes(uint64(tbl.Size())))
	return nil
}
