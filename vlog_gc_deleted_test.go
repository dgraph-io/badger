/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"sort"
	"testing"
)

// A historical value selected by GC must stay hidden by its pre-existing
// tombstone even if compaction runs before the rewrite's write-back.
func TestVlogGCDeletedBeforeRewrite(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions(dir).WithLogger(nil).WithNumCompactors(0).WithBlockCacheSize(32 << 20).WithIndexCacheSize(16 << 20)
	open := func() *DB {
		d, e := Open(opt)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	d := open()
	value := bytes.Repeat([]byte{42}, 2<<20)
	if e := d.Update(func(tx *Txn) error { return tx.Set([]byte("c-orphan"), value) }); e != nil {
		t.Fatal(e)
	}
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	d = open()
	if e := d.Update(func(tx *Txn) error { return tx.Delete([]byte("c-orphan")) }); e != nil {
		t.Fatal(e)
	}
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	d = open()
	defer d.Close()
	e := d.View(func(tx *Txn) error { _, e := tx.Get([]byte("c-orphan")); return e })
	if e != ErrKeyNotFound {
		t.Fatalf("before GC expected deleted: %v", e)
	}
	t.Logf("before GC: deleted, maxVersion=%d", d.MaxVersion())
	var fids []int
	for f := range d.vlog.filesMap {
		if f < d.vlog.maxFid {
			fids = append(fids, int(f))
		}
	}
	sort.Ints(fids)
	called := false
	d.vlogGCPauseHook = func() {
		db := d
		called = true
		t.Logf("GC selected old value of already deleted key; protected=%v", db.gcActive.Load())
		if e := db.lc.doCompact(0, compactionPriority{level: 0, score: 1.71}); e != nil {
			t.Fatal(e)
		}
		if e := db.Flatten(1); e != nil {
			t.Fatal(e)
		}
		t.Log("forced legal compaction between GC read and write-back")
		d.vlogGCPauseHook = nil
	}
	defer func() { d.vlogGCPauseHook = nil }()
	for _, fid := range fids {
		if e := d.vlog.rewrite(d.vlog.filesMap[uint32(fid)]); e != nil {
			t.Fatal(e)
		}
	}
	if !called {
		t.Fatal("GC did not select the old value")
	}
	e = d.View(func(tx *Txn) error {
		i, e := tx.Get([]byte("c-orphan"))
		if e != nil {
			return e
		}
		v, e := i.ValueCopy(nil)
		if e == nil {
			t.Logf("after GC: resurrected version=%d valueBytes=%d", i.Version(), len(v))
		}
		return e
	})
	if e != ErrKeyNotFound {
		t.Fatalf("deleted key resurrected after GC: %v", e)
	}
}
