package badger

import (
	"bytes"
	"sync"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/pkg/errors"
)

type BatchSet struct {
	sync.Mutex

	db       *DB
	err      error
	count    int64
	size     int64
	entries  []*Entry
	throttle *y.Throttle
}

func (db *DB) NewBatchSet() (*BatchSet, error) {
	if !db.opt.managedTxns {
		return nil, errors.New("Batchset can only be used in managed mode")
	}
	return newBatchSet(db), nil
}

func newBatchSet(db *DB) *BatchSet {
	return &BatchSet{
		db:       db,
		entries:  make([]*Entry, 0, 1000),
		throttle: y.NewThrottle(16),
	}
}

func (bs *BatchSet) SetAt(k, v []byte, meta byte, version uint64) error {
	return bs.SetEntryAt(&Entry{Key: k, Value: v, UserMeta: meta}, version)
}

func (bs *BatchSet) SetEntryAt(e *Entry, version uint64) error {
	bs.Lock()
	defer bs.Unlock()

	const maxKeySize = 65000

	switch {
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > bs.db.opt.ValueLogFileSize:
		return exceedsSize("Value", bs.db.opt.ValueLogFileSize, e.Value)
	case bs.db.opt.InMemory && len(e.Value) > bs.db.opt.ValueThreshold:
		return exceedsSize("Value", int64(bs.db.opt.ValueThreshold), e.Value)
	}

	e.Key = y.KeyWithTs(e.Key, version)
	err := bs.add(e)
	switch err {
	case nil:
		return nil
	case ErrTxnTooBig:
		if err := bs.throttle.Do(); err != nil {
			return err
		}
		err = bs.BatchSetListAsync(bs.entries, func(err error) {
			defer bs.throttle.Done(err)
			if err != nil {
				bs.err = err
				return
			}
		})
		bs.reset()
		bs.add(e)
		return err
	default:
		panic(err)
		return err

	}
	return nil
}

func (bs *BatchSet) add(e *Entry) error {
	err := bs.checkSize(e)
	if err != nil {
		panic(err)
		return err
	}
	bs.entries = append(bs.entries, e)
	return nil
}

func (bs *BatchSet) reset() {
	bs.entries = make([]*Entry, 0, 1000)
	bs.count = 0
	bs.size = 0
}

func (bs *BatchSet) Flush() error {
	bs.Lock()
	defer bs.Unlock()

	if err := bs.throttle.Do(); err != nil {
		return err
	}
	if err := bs.BatchSetListAsync(bs.entries, func(err error) {
		defer bs.throttle.Done(err)
		if err != nil {
			bs.err = err
			return
		}
	}); err != nil {
		return err
	}

	if err := bs.throttle.Finish(); err != nil {
		panic(err)
		return err
	}

	return bs.err
}

// BatchSet applies a list of badger.Entry. If a request level error occurs it
// will be returned.
func (bs *BatchSet) BatchSetList(entries []*Entry) error {
	req, err := bs.db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

// BatchSetAsync is the asynchronous version of BatchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//   err := kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//   }
func (bs *BatchSet) BatchSetListAsync(entries []*Entry, f func(error)) error {
	req, err := bs.db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		err := req.Wait()
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

func (bs *BatchSet) checkSize(e *Entry) error {
	count := bs.count + 1
	// Extra bytes for the version in key.
	size := bs.size + int64(e.estimateSize(bs.db.opt.ValueThreshold)) + 10
	if count >= bs.db.opt.maxBatchCount || size >= bs.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	bs.count, bs.size = count, size
	return nil
}
