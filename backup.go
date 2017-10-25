package badger

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync"

	"github.com/dgraph-io/badger/y"

	"github.com/dgraph-io/badger/protos"
)

func writeTo(entry *protos.KVPair, w io.Writer) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(entry.Size()))
	_, err := w.Write(buf[0:n])
	if err != nil {
		return err
	}
	buf, err = entry.Marshal()
	if err != nil {
		return err
	}
	if _, err = w.Write(buf); err != nil {
		return err
	}
	return nil
}

// Dump dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than the specified version. It returns a
// timestamp indicating when the entries were dumped which can be passed into a
// later invocation to generate an incremental dump, of entries that have been
// added/modified since the last invocation of DB.Dump()
//
// This can be used to backup the data in a database at a given point in time.
func (db *DB) Dump(ts uint64, w io.Writer) (uint64, error) {
	var tsNew uint64
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.Version() < ts {
				// Ignore versions less than given timestamp
				continue
			}
			val, err := item.Value()
			if err != nil {
				return err
			}

			entry := &protos.KVPair{
				Key:      y.Safecopy([]byte{}, item.Key()),
				Value:    y.Safecopy([]byte{}, val),
				UserMeta: y.Safecopy([]byte{}, []byte{item.UserMeta()}),
				Version:  item.Version(),
			}

			// Write entries to disk
			if err := writeTo(entry, w); err != nil {
				return err
			}
		}
		ts = txn.readTs
		return nil
	})
	return tsNew, err
}

// Load reads a protobuf-encoded list of all entries from a reader and writes
// them to the database. This can be used to restore the database from a backup
// made by calling DB.Dump().
//
// DB.Load() should be called on a database that is not running any other
// concurrent transactions while it is running.
func (db *DB) Load(r io.Reader) error {
	br := bufio.NewReaderSize(r, 16<<10)
	unmarshalBuf := make([]byte, 1<<10)
	var entries []*entry
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// func to check for pending error before sending off a batch for writing
	batchSetAsyncIfNoErr := func(entries []*entry) error {
		select {
		case err := <-errChan:
			return err
		default:
			wg.Add(1)
			return db.batchSetAsync(entries, func(err error) {
				defer wg.Done()
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			})
		}
	}

	for {
		buf, err := br.Peek(binary.MaxVarintLen64)
		if err == io.EOF {
			break
		} else if len(buf) > 0 && len(buf) < binary.MaxVarintLen64 && err != nil {
			// Ignore
		} else if err != nil {
			return err
		}

		sz, n := binary.Uvarint(buf)
		if sz <= 0 {
			return ErrInvalidDump
		}

		_, err = br.Discard(n)
		if err != nil {
			return err
		}

		for cap(unmarshalBuf) < int(sz) {
			unmarshalBuf = make([]byte, sz)
		}

		e := &protos.KVPair{}
		_, err = io.ReadFull(br, unmarshalBuf[:sz])
		if err != nil {
			return err
		}
		err = e.Unmarshal(unmarshalBuf[:sz])
		if err != nil {
			return err
		}
		entries = append(entries, &entry{
			Key:      y.KeyWithTs(e.Key, e.Version),
			Value:    e.Value,
			UserMeta: e.UserMeta[0],
		})

		if len(entries) == 1000 {
			if err := batchSetAsyncIfNoErr(entries); err != nil {
				return err
			}
			entries = entries[:0]
		}
	}

	if len(entries) > 0 {
		if err := batchSetAsyncIfNoErr(entries); err != nil {
			return err
		}
	}
	return nil
}
