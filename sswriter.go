/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"math"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

type StreamWriter struct {
	db         *DB
	done       func()
	throttle   *y.Throttle
	head       valuePointer
	maxVersion uint64
	writers    map[uint32]*sortedWriter
}

func (db *DB) NewStreamWriter(maxPending int) *StreamWriter {
	return &StreamWriter{
		db:       db,
		throttle: y.NewThrottle(maxPending),
		writers:  make(map[uint32]*sortedWriter),
	}
}

func (sw *StreamWriter) Prepare() error {
	var err error
	sw.done, err = sw.db.dropAll()
	return err
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
		if sw.maxVersion < kv.Version {
			sw.maxVersion = kv.Version
		}
		e := &Entry{
			Key:       y.KeyWithTs(kv.Key, kv.Version),
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
		if !vptr.IsZero() {
			y.AssertTrue(sw.head.Less(vptr))
			sw.head = vptr
		}

		writer, ok := sw.writers[kv.StreamId]
		if !ok {
			writer = sw.newWriter(kv.StreamId)
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
	defer sw.done()
	for _, writer := range sw.writers {
		if err := writer.Done(); err != nil {
			return err
		}
	}

	// Encode and write the value log head into a new table.
	data := make([]byte, vptrSize)
	sw.head.Encode(data)
	headWriter := sw.newWriter(math.MaxUint32)
	if err := headWriter.Add(
		y.KeyWithTs(head, sw.maxVersion),
		y.ValueStruct{Value: data}); err != nil {
		return err
	}
	if err := headWriter.Done(); err != nil {
		return err
	}

	if !sw.db.opt.managedTxns {
		sw.db.orc.nextTxnTs = sw.maxVersion
		sw.db.orc.txnMark.Done(sw.db.orc.nextTxnTs)
		sw.db.orc.readMark.Done(sw.db.orc.nextTxnTs)
		sw.db.orc.nextTxnTs++
	}

	// Wait for all files to be written.
	if err := sw.throttle.Finish(); err != nil {
		return err
	}

	// Now sync the directories, so all the files are registered.
	if err := syncDir(sw.db.opt.Dir); err != nil {
		return err
	}
	if sw.db.opt.ValueDir != sw.db.opt.Dir {
		return syncDir(sw.db.opt.ValueDir)
	}
	return nil
}

type sortedWriter struct {
	db       *DB
	throttle *y.Throttle

	builder  *table.Builder
	lastKey  []byte
	streamId uint32
}

func (sw *StreamWriter) newWriter(streamId uint32) *sortedWriter {
	return &sortedWriter{
		db:       sw.db,
		streamId: streamId,
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
	if err := w.throttle.Do(); err != nil {
		return err
	}
	go func(builder *table.Builder) {
		data := builder.Finish()
		err := w.createTable(data)
		w.throttle.Done(err)
	}(w.builder)
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
	w.db.opt.Infof("Table created: %d at level: %d for stream: %d. Size: %s\n",
		fileId, lhandler.level, w.streamId, humanize.Bytes(uint64(tbl.Size())))
	return nil
}
