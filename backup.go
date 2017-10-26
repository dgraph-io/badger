package badger

import (
	"encoding/binary"
	"io"

	"github.com/dgraph-io/badger/y"

	"github.com/dgraph-io/badger/protos"
)

func writeTo(entry *protos.KVPair, w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, uint64(entry.Size()))
	if err != nil {
		return err
	}
	buf, err := entry.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (db *KV) Backup(w io.Writer) error {
	opts := DefaultIteratorOptions
	it := db.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var val []byte
		err := item.Value(func(v []byte) error {
			val = y.Safecopy(val, v)
			return nil
		})
		if err != nil {
			return err
		}

		entry := &protos.KVPair{
			Key:      y.Safecopy([]byte{}, item.Key()),
			Value:    val,
			UserMeta: y.Safecopy([]byte{}, []byte{item.UserMeta()}),
		}

		// Write entries to disk
		if err := writeTo(entry, w); err != nil {
			return err
		}
	}
	return nil
}
