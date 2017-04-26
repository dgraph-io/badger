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

package badger

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("", "")
	y.Check(err)

	kv := NewKV(getTestOptions(dir))
	defer kv.Close()
	log := kv.vlog

	entry := &Entry{
		Key:   []byte("samplekey"),
		Value: []byte("sampleval"),
		Meta:  123,
	}
	b := new(block)
	b.Entries = []*Entry{entry}

	log.Write([]*block{b})
	require.Len(t, b.Ptrs, 1)
	fmt.Printf("Pointer written: %+v", b.Ptrs[0])

	var readEntries []Entry
	e, err := log.Read(ctx, b.Ptrs[0])
	require.NoError(t, err)
	readEntries = append(readEntries, e)
	require.EqualValues(t, []Entry{
		{
			Key:   []byte("samplekey"),
			Value: []byte("sampleval"),
			Meta:  123,
		},
	}, readEntries)
}

func repeat(c byte, size int) (b []byte) {
	b = make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = c
	}
	return
}

func TestCompression(t *testing.T) {
	ctx := context.Background()
	dir, err := ioutil.TempDir("", "")
	y.Check(err)

	kv := NewKV(getTestOptions(dir))
	defer kv.Close()
	log := kv.vlog

	manyA := repeat('a', (1 << 10)) // ~ 1KB

	fmt.Println(len(manyA))

	manyB := repeat('b', 65*(1<<10)) // ~ 65KB
	fmt.Println(len(manyB))
	entry1 := &Entry{
		Key:   []byte("key1"),
		Value: manyA,
		Meta:  BitValuePointer,
	}
	entry2 := &Entry{
		Key:   []byte("key2"),
		Value: manyB,
		Meta:  BitValuePointer,
	}
	b := new(block)
	b.Entries = []*Entry{entry1, entry2}

	log.Write([]*block{b})
	require.Len(t, b.Ptrs, 2)
	fmt.Printf("Pointer written: %+v, %+v\n", b.Ptrs[0], b.Ptrs[1])

	require.NotZero(t, b.Ptrs[0].Meta&BitCompressed)
	require.EqualValues(t, 0, b.Ptrs[0].Offset)
	require.EqualValues(t, 0, b.Ptrs[0].InsideBlockOffset)

	require.NotZero(t, b.Ptrs[1].Meta&BitCompressed)
	require.NotZero(t, b.Ptrs[1].InsideBlockOffset)
	require.EqualValues(t, 0, b.Ptrs[1].Offset)

	require.EqualValues(t, b.Ptrs[0].Len, b.Ptrs[1].Len)
	require.True(t, b.Ptrs[0].Len < 400) // Check whether the block is small

	var readEntries []Entry
	e, err := log.Read(ctx, b.Ptrs[0])
	require.NoError(t, err)
	readEntries = append(readEntries, e)
	e, err = log.Read(ctx, b.Ptrs[1])
	require.NoError(t, err)
	readEntries = append(readEntries, e)

	require.EqualValues(t, readEntries[0].Value, manyA)
	require.EqualValues(t, readEntries[1].Value, manyB)
}

func TestGC(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	db := NewKV(getTestOptions(dir))
	defer db.Close()

	sz := 16 << 20
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v)
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	ctx := context.Background()
	require.NoError(t, db.Write(ctx, entries))

	for i := 0; i < 45; i++ {
		db.Delete(ctx, []byte(fmt.Sprintf("key%d", i)))
	}

	db.vlog.RLock()
	lf := db.vlog.files[0]
	db.vlog.RUnlock()

	// lf.iterate(0, func(e Entry) {
	// 	e.print("lf")
	// })

	db.vlog.rewrite(lf)
	for i := 45; i < 100; i++ {
		val := db.Get(ctx, []byte(fmt.Sprintf("key%d", i)))
		require.NotNil(t, val)
		require.True(t, len(val) == sz, "Size found: %d", len(val))
	}
}

func BenchmarkReadWrite(b *testing.B) {
	ctx := context.Background()
	rwRatio := []float32{
		0.1, 0.2, 0.5, 1.0,
	}
	valueSize := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}

	for _, vsz := range valueSize {
		for _, rw := range rwRatio {
			b.Run(fmt.Sprintf("%3.1f,%04d", rw, vsz), func(b *testing.B) {
				var vl valueLog
				vl.Open(nil, getTestOptions("vlog"))
				defer os.Remove("vlog")
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					e := new(Entry)
					e.Key = make([]byte, 16)
					e.Value = make([]byte, vsz)
					bl := new(block)
					bl.Entries = []*Entry{e}

					var ptrs []valuePointer

					vl.Write([]*block{bl})
					ptrs = append(ptrs, bl.Ptrs...)

					f := rand.Float32()
					if f < rw {
						vl.Write([]*block{bl})
						ptrs = append(ptrs, bl.Ptrs...)

					} else {
						ln := len(ptrs)
						if ln == 0 {
							b.Fatalf("Zero length of ptrs")
						}
						idx := rand.Intn(ln)
						e, err := vl.Read(ctx, ptrs[idx])
						if err != nil {
							b.Fatalf("Benchmark Read:", err)
						}
						if len(e.Key) != 16 {
							b.Fatalf("Key is invalid")
						}
						if len(e.Value) != vsz {
							b.Fatalf("Value is invalid")
						}
					}
				}
			})
		}
	}
}
