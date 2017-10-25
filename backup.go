package badger

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync"

	"github.com/dgraph-io/badger/y"

	"github.com/dgraph-io/badger/protos"
)

func writeToDisk(entries []*protos.KVPair, w io.Writer) error {
	buf := make([]byte, binary.MaxVarintLen64+entries[0].Size())
	for _, entry := range entries {
		if len(buf) < entry.Size()+binary.MaxVarintLen64 {
			buf = make([]byte, entry.Size())
		}
		b := buf
		n := binary.PutUvarint(b, uint64(entry.Size()))
		_, err := w.Write(b[0:n])
		if err != nil {
			return err
		}
		b = b[n:]
		n, err = entry.MarshalTo(b)
		if err != nil {
			return err
		}
		if _, err = w.Write(b[0:n]); err != nil {
			return err
		}
	}
	return nil
}

// Dump dumps a protobuf-encoded list of all entries in the database into the
// given writer.
//
// This can be used to backup the data in a database at a given point in time.
func (db *DB) Dump(w io.Writer) error {
	return db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)

		var entries []*protos.KVPair
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.Value()
			if err != nil {
				return err
			}

			entries = append(entries, &protos.KVPair{
				Key:      y.Safecopy([]byte{}, item.Key()),
				Value:    y.Safecopy([]byte{}, val),
				UserMeta: y.Safecopy([]byte{}, []byte{item.UserMeta()}),
				Version:  item.Version(),
			})

			count++
			if count == 1000 {
				// Write entries to disk
				if err := writeToDisk(entries, w); err != nil {
					return err
				}
				count = 0
			}
		}
		if count > 0 { // write remaining entries
			return writeToDisk(entries, w)
		}
		return nil
	})
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
	var count int
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
		} else if err != nil {
			return err
		}

		sz, n := binary.Uvarint(buf)
		if sz <= 0 {
			return ErrInvalidBackupData
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
			// TODO do we have to set the Meta field ourselves
		})
		count++

		if count == 1000 {
			if err := batchSetAsyncIfNoErr(entries); err != nil {
				return err
			}
			count = 0
			entries = []*entry{}
		}
	}

	if count > 0 {
		if err := batchSetAsyncIfNoErr(entries); err != nil {
			return err
		}
	}
	return nil
}
