/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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

package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/y"
)

// BuildKVFromBackup creates a new badger instance from a backup.  opt.Dir and opt.ValueDir must
// not exist.  `source` supplies BackupItems (in increasing key order, hopefully) until it returns
// a nil slice (or an error).
func BuildKVFromBackup(opt *badger.Options, source func() ([]protos.BackupItem, error)) (err error) {
	// First create the directories we're restoring our backup into.
	if err := os.Mkdir(opt.Dir, 0755); err != nil {
		return err
	}
	if opt.Dir != opt.ValueDir {
		if err := os.Mkdir(opt.ValueDir, 0755); err != nil {
			return err
		}
	}

	kv, err := badger.NewKV(opt)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := kv.Close(); err == nil {
			err = closeErr
		}
	}()

	size := int64(0)
	batch := []*badger.Entry{}
	for {
		items, err := source()
		if err != nil {
			return err
		}
		if items == nil {
			break
		}
		for _, item := range items {
			if item.HasValue {
				e := &badger.Entry{
					Key:      item.Key,
					Value:    item.Value,
					UserMeta: uint8(item.UserMeta),
				}
				size += int64(opt.EstimateSize(e))
				batch = append(batch, e)

				if size >= (64 << 20) {
					if err := kv.BatchSet(batch); err != nil {
						return err
					}
					size = 0
					batch = []*badger.Entry{}
				}
			}
		}
	}
	if len(batch) > 0 {
		if err := kv.BatchSet(batch); err != nil {
			return err
		}
	}

	return nil
}

// Implements the y.Iterator interface.
type backupFileIterator struct {
	fp     *os.File
	reader *bufio.Reader
	key    []byte
	value  y.ValueStruct
	err    error
}

func makeBackupFileIterator(path string, id uint64, bufferSize int) (*backupFileIterator, error) {
	fp, err := os.Open(filepath.Join(path, badger.BackupFileName(id)))
	if err != nil {
		return nil, err
	}
	return &backupFileIterator{fp: fp}, nil
}

func (bit *backupFileIterator) Rewind() {
	if bit.err != nil {
		bit.err = nil
	}
	_, err := bit.fp.Seek(0, os.SEEK_SET)
	if err != nil {
		bit.err = err
		bit.key = nil
		bit.value = y.ValueStruct{}
		return
	}
	bit.reader = bufio.NewReader(bit.fp)
	bit.Next()
}

func (bit *backupFileIterator) Key() []byte {
	return bit.key
}

func (bit *backupFileIterator) Value() y.ValueStruct {
	return bit.value
}

func (bit *backupFileIterator) Next() {
	if bit.err != nil {
		return
	}
	sz, err := binary.ReadUvarint(bit.reader)
	if err != nil {
		bit.err = err
		bit.key = nil
		bit.value = y.ValueStruct{}
		return
	}

	data := make([]byte, sz)
	if _, err := io.ReadFull(bit.reader, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		bit.err = err
		bit.key = nil
		bit.value = y.ValueStruct{}
		return
	}

	var item protos.BackupItem
	if err := item.Unmarshal(data); err != nil {
		bit.err = err
		bit.key = nil
		bit.value = y.ValueStruct{}
		return
	}
	bit.key = item.Key
	var meta byte
	if !item.HasValue {
		meta = badger.BitDelete
	}
	bit.value = y.ValueStruct{
		// This is typically a vptr but in our case it's a value -- we're just using MergeIterator.
		Value:      item.Value,
		Meta:       meta,
		UserMeta:   uint8(item.UserMeta),
		CASCounter: item.Counter,
	}
}

func (bit *backupFileIterator) Valid() bool {
	return bit.key != nil
}

func (bit *backupFileIterator) Close() error {
	err := bit.err
	if err == io.EOF {
		err = nil
	}
	if closeErr := bit.fp.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (bit *backupFileIterator) Seek([]byte) {
	panic("Seek not implemented")
}

// RetrieveBackup streams all changes from the backup store in increasing key order to consumer.
func RetrieveBackup(path string, consumer func(protos.BackupItem) error) (err error) {
	status, err := badger.ReadBackupStatus(path)
	if err != nil {
		return err
	}

	ids := []uint64{}
	for _, backup := range status.Backups {
		ids = append(ids, backup.BackupID)
	}

	// Sort by descending ID (because MergeIterator expects order by increasing age)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	const memUsage = 2 << 30
	bufferSize := memUsage / len(ids)

	fileIters := []y.Iterator{}
	defer func() {
		for _, iter := range fileIters {
			_ = iter.Close()
		}
	}()

	for _, id := range ids {
		iter, err := makeBackupFileIterator(path, id, bufferSize)
		if err != nil {
			return err
		}
		fileIters = append(fileIters, iter)
	}

	// In MergeIterator, keys returned by the iterators closer to the beginning of the array take
	// precedence.
	mergeIter := y.NewMergeIterator(fileIters, false)
	fileIters = nil
	defer func() {
		if closeErr := mergeIter.Close(); err == nil {
			err = closeErr
		}
	}()

	for mergeIter.Rewind(); mergeIter.Valid(); mergeIter.Next() {
		key := mergeIter.Key()
		value := mergeIter.Value()
		if value.Meta == 0 { // if value.Meta != BitDelete
			err := consumer(protos.BackupItem{
				Key:      key,
				Counter:  value.CASCounter,
				HasValue: true,
				UserMeta: uint32(value.UserMeta),
				Value:    value.Value,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
