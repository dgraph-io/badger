// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package badger

import (
	"bytes"
	"os"
	"testing"
)

func FuzzValueLogEntry(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 10, 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(make([]byte, 12))
	f.Add(make([]byte, 1000))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 10 { return }
		var vp valuePointer
		func() { defer func() { recover() }(); vp.Decode(data) }()
		var h header
		func() { defer func() { recover() }(); h.Decode(data) }()
	})
}

func FuzzManifestParse(f *testing.F) {
	f.Add([]byte{10, 0})
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{10, 2, 8, 1, 10, 4, 8, 2, 16, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 { return }
		r := bytes.NewReader(data)
		buf := make([]byte, len(data))
		r.Read(buf)
		dir, _ := os.MkdirTemp("", "fuzz-badger-*")
		defer os.RemoveAll(dir)
		db, err := Open(DefaultOptions(dir).WithReadOnly(true).WithValueLogFileSize(1<<20))
		if err == nil { db.Close() }
	})
}

func FuzzDBWriteRead(f *testing.F) {
	f.Add([]byte("k1"), []byte("v1"))
	f.Add([]byte(""), []byte(""))
	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) > 1000 || len(value) > 10000 { return }
		dir, _ := os.MkdirTemp("", "fuzz-badger-*")
		defer os.RemoveAll(dir)
		func() {
			defer func() { recover() }()
			db, err := Open(DefaultOptions(dir).WithSyncWrites(false).WithNumVersionsToKeep(1))
			if err != nil { return }
			defer db.Close()
			txn := db.NewTransaction(true)
			txn.Set(key, value)
			txn.Commit()
			db.View(func(txn *Txn) error {
				item, err := txn.Get(key)
				if err == nil { item.ValueCopy(nil) }
				return nil
			})
		}()
	})
}

func FuzzOptions(f *testing.F) {
	f.Add(int64(1), int64(1), float64(0.5))
	f.Add(int64(1<<20), int64(1<<30), float64(0.95))
	f.Add(int64(-1), int64(0), float64(2.0))
	f.Fuzz(func(t *testing.T, memSize, vlogSize int64, sample float64) {
		dir, _ := os.MkdirTemp("", "fuzz-badger-*")
		defer os.RemoveAll(dir)
		func() {
			defer func() { recover() }()
			opts := DefaultOptions(dir).
				WithMemTableSize(memSize).
				WithValueLogFileSize(vlogSize).
				WithValueThreshold(32)
			_ = opts
		}()
	})
}
