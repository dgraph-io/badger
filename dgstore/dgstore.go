// +build dgbadger

package dgstore

import (
	"github.com/dgraph-io/dgraph/dgs"

	"github.com/dgraph-io/badger/badger"
)

type Store struct {
	*badger.KV

	itOpt badger.IteratorOptions
}

// NewKV returns a new KV object. Compact levels are created as well.
func NewStore(opt *badger.Options, itOpt *badger.IteratorOptions) *Store {
	out := &Store{badger.NewKV(opt), *itOpt}
	return out
}

func (s *Store) Get(key []byte) ([]byte, func(), error) {
	// Currently our Get does not return any error...
	// The CASCounter is exposed yet.
	val, _ := s.KV.Get(key)
	return val, func() {}, nil
}

func (s *Store) GetStats() string {
	return "NoStats"
}

func (s *Store) SetOne(key, val []byte) error {
	return s.KV.Set(key, val)
}

func (s *Store) NewIterator(reversed bool) dgs.Iterator {
	opt := s.itOpt
	opt.Reversed = reversed
	return s.KV.NewIterator(opt)
}

type WriteBatch struct {
	entries []*badger.Entry
}

func (s *Store) NewWriteBatch() dgs.WriteBatch {
	return &WriteBatch{}
}

func (s *WriteBatch) Clear()     { s.entries = s.entries[:0] }
func (s *WriteBatch) Count() int { return len(s.entries) }

func (s *WriteBatch) Delete(key []byte) {
	s.entries = append(s.entries, &badger.Entry{
		Key:  key,
		Meta: badger.BitDelete,
	})
}

func (s *WriteBatch) SetOne(key, val []byte) {
	s.entries = append(s.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
}

func (s *WriteBatch) Destroy() {} // Consider sync.Pool or freelist.

func (s *Store) WriteBatch(wb dgs.WriteBatch) error {
	return s.BatchSet(wb.(*WriteBatch).entries)
}
