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
	writers  map[uint32]*sortedWriter
}

func (db *DB) NewStreamWriter(maxPending int) *StreamWriter {
	return &StreamWriter{
		db:       db,
		throttle: y.NewThrottle(maxPending),
		writers:  make(map[uint32]*sortedWriter),
	}
}

func (sw *StreamWriter) Write(kvs *pb.KVList) error {
	var entries []*Entry
	for _, kv := range kvs.Kv {
		var meta, userMeta byte
		if len(kv.Meta) > 0 {
			meta = kv.Meta[0]
		}
		if len(kv.UserMeta) > 0 {
			userMeta = kv.UserMeta[0]
		}
		e := &Entry{
			Key:       kv.Key,
			Value:     kv.Value,
			UserMeta:  userMeta,
			ExpiresAt: kv.ExpiresAt,
			meta:      meta,
		}
		// If the value can be colocated with the key in LSM tree, we can skip
		// writing the value to value log.
		e.skipVlog = sw.db.shouldWriteValueToLSM(*e)
		entries = append(entries, e)
	}
	req := &request{
		Entries: entries,
	}
	y.AssertTrue(len(kvs.Kv) == len(req.Entries))
	if err := sw.db.vlog.write([]*request{req}); err != nil {
		return err
	}

	for i, kv := range kvs.Kv {
		e := req.Entries[i]
		vptr := req.Ptrs[i]

		writer, ok := sw.writers[kv.StreamId]
		if !ok {
			writer = sw.newWriter()
			sw.writers[kv.StreamId] = writer
		}

		var vs y.ValueStruct
		if e.skipVlog {
			vs = y.ValueStruct{
				Value:     e.Value,
				Meta:      e.meta,
				UserMeta:  e.UserMeta,
				ExpiresAt: e.ExpiresAt,
			}
		} else {
			vbuf := make([]byte, vptrSize)
			vs = y.ValueStruct{
				Value:     vptr.Encode(vbuf),
				Meta:      e.meta | bitValuePointer,
				UserMeta:  e.UserMeta,
				ExpiresAt: e.ExpiresAt,
			}
		}
		if err := writer.Add(e.Key, vs); err != nil {
			return err
		}
	}
	return nil
}

func (sw *StreamWriter) Done() error {
	// TODO: Set the value log head to math.MaxUint32 stream here.
	for _, writer := range sw.writers {
		if err := writer.Done(); err != nil {
			return err
		}
	}
	return sw.throttle.Finish()
}

type sortedWriter struct {
	db       *DB
	throttle *y.Throttle

	builder *table.Builder
	lastKey []byte
}

func (sw *StreamWriter) newWriter() *sortedWriter {
	return &sortedWriter{
		db:       sw.db,
		throttle: sw.throttle,
		builder:  table.NewTableBuilder(),
	}
}

var ErrUnsortedKey = errors.New("Keys not in sorted order")

func (w *sortedWriter) Add(key []byte, vs y.ValueStruct) error {
	if bytes.Compare(key, w.lastKey) < 0 {
		return ErrUnsortedKey
	}
	sameKey := y.SameKey(key, w.lastKey)
	w.lastKey = y.SafeCopy(w.lastKey, key)

	if err := w.builder.Add(key, vs); err != nil {
		return err
	}
	// Same keys should go into the same SSTable.
	if !sameKey && w.builder.ReachedCapacity(w.db.opt.MaxTableSize) {
		return w.send()
	}
	return nil
}

func (w *sortedWriter) send() error {
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

func (w *sortedWriter) Done() error {
	if w.builder.Empty() {
		return nil
	}
	return w.send()
}

func (w *sortedWriter) createTable(data []byte) error {
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
