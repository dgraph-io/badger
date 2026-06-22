/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// bitDelete mirrors badger's tombstone meta bit. The exact value is irrelevant
// to the table layer (the version lives in the key regardless); we only use it
// to assert that delete entries widen the block version range.
const testBitDelete = 1 << 0

type vkv struct {
	key     string
	version uint64
	value   string
	delete  bool
}

// buildVersionTable builds a table from (key,version,value) tuples. Keys are
// encoded with their version via y.KeyWithTs and sorted in badger order so that
// blocks are populated deterministically. A tiny block size forces many blocks.
func buildVersionTable(t *testing.T, kvs []vkv, blockSize int) *Table {
	opts := Options{
		Compression:        options.ZSTD,
		BlockSize:          blockSize,
		BloomFalsePositive: 0.01,
		TableSize:          30 << 20,
	}
	b := NewTableBuilder(opts)
	defer b.Close()

	encoded := make([][]byte, len(kvs))
	idx := make([]int, len(kvs))
	for i := range kvs {
		encoded[i] = y.KeyWithTs([]byte(kvs[i].key), kvs[i].version)
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool {
		return y.CompareKeys(encoded[idx[a]], encoded[idx[b]]) < 0
	})

	for _, i := range idx {
		meta := byte('A')
		if kvs[i].delete {
			meta = testBitDelete
		}
		b.Add(encoded[i], y.ValueStruct{Value: []byte(kvs[i].value), Meta: meta}, 0)
	}

	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err)
	return tbl
}

// scanAll returns all (key-with-ts) keys an iterator yields, applying an
// optional version window filter [lower,upper] at the per-entry level. This is
// the authoritative reference the pruned scan must match exactly.
func referenceKeys(kvs []vkv, lower, upper uint64) []string {
	var out []string
	for _, kv := range kvs {
		if kv.version >= lower && kv.version <= upper {
			out = append(out, string(y.KeyWithTs([]byte(kv.key), kv.version)))
		}
	}
	sort.Slice(out, func(a, b int) bool {
		return y.CompareKeys([]byte(out[a]), []byte(out[b])) < 0
	})
	return out
}

// iterateKeys collects keys from a table iterator that fall within [lower,upper]
// (the per-entry filter the real merge iterator applies). When bound is true the
// iterator also has SetVersionBounds applied (storage-level block skipping).
func iterateKeys(it *Iterator, lower, upper uint64) []string {
	var out []string
	for it.Rewind(); it.Valid(); it.Next() {
		v := y.ParseTs(it.Key())
		if v < lower || v > upper {
			continue
		}
		out = append(out, string(it.Key()))
	}
	return out
}

// buildOldFormatTable builds a table whose index omits the appended
// version-range fields, reproducing the on-disk format written by code that
// predates this feature.
func buildOldFormatTable(t *testing.T) *Table {
	opts := Options{
		Compression:        options.ZSTD,
		BlockSize:          512,
		BloomFalsePositive: 0.01,
		TableSize:          30 << 20,
	}
	b := NewTableBuilder(opts)
	b.omitVersionRange = true
	defer b.Close()

	type pair struct{ k, v string }
	pairs := make([]pair, 50)
	for i := 0; i < 50; i++ {
		pairs[i] = pair{k: fmt.Sprintf("key%04d", i), v: "v"}
	}
	sort.Slice(pairs, func(a, c int) bool { return pairs[a].k < pairs[c].k })
	for i, p := range pairs {
		b.Add(y.KeyWithTs([]byte(p.k), uint64(10+i)), y.ValueStruct{Value: []byte(p.v), Meta: 'A'}, 0)
	}
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err)
	return tbl
}

// TestBlockVersionRangeMetadata verifies the builder records a conservative
// per-block [min,max] over every key's version, including tombstones, and a
// table-wide min/max.
func TestBlockVersionRangeMetadata(t *testing.T) {
	// Two version "bands": low keys at versions 1..5, high keys at 100..105,
	// plus a delete at a high version to assert it widens the range.
	var kvs []vkv
	for i := 0; i < 40; i++ {
		kvs = append(kvs, vkv{key: fmt.Sprintf("low%04d", i), version: uint64(1 + i%5), value: "v"})
	}
	for i := 0; i < 40; i++ {
		del := i == 7 // a tombstone at a high version
		kvs = append(kvs, vkv{key: fmt.Sprintf("zzz%04d", i), version: uint64(100 + i%6), value: "v", delete: del})
	}

	tbl := buildVersionTable(t, kvs, 1024) // tiny blocks => many blocks
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	require.True(t, tbl.HasVersionRange(), "new table must advertise version range")
	require.EqualValues(t, 1, tbl.MinVersion())
	require.EqualValues(t, 105, tbl.MaxVersion())

	n := tbl.offsetsLength()
	require.Greater(t, n, 1, "test needs multiple blocks; lower BlockSize")

	// Every block's recorded range must contain every version of every key in
	// that block (conservative over-approximation, deletes included).
	for i := 0; i < n; i++ {
		var ko fb.BlockOffset
		require.True(t, tbl.offsets(&ko, i))
		require.True(t, ko.VersionRangePresent(), "block %d missing version range", i)
		bmin, bmax := ko.MinVersion(), ko.MaxVersion()
		require.LessOrEqual(t, bmin, bmax)

		// Walk the actual entries in this block and confirm containment.
		it := tbl.NewIterator(0)
		it.bpos = i
		block, err := tbl.block(i, true)
		require.NoError(t, err)
		it.bi.setBlock(block)
		for it.bi.seekToFirst(); it.bi.Valid(); it.bi.next() {
			v := y.ParseTs(it.bi.key)
			require.GreaterOrEqual(t, v, bmin, "block %d min too high for key version", i)
			require.LessOrEqual(t, v, bmax, "block %d max too low for key version", i)
		}
		require.NoError(t, it.Close())
	}
}

// TestVersionBoundedScanIdenticalResults asserts a bounded scan with block
// skipping returns IDENTICAL results to an unbounded scan filtered per-entry,
// across randomized multi-version data including deletes, for many windows.
func TestVersionBoundedScanIdenticalResults(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var kvs []vkv
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key%05d", i)
		nver := 1 + rng.Intn(4)
		for j := 0; j < nver; j++ {
			ver := uint64(1 + rng.Intn(200))
			kvs = append(kvs, vkv{
				key:     k,
				version: ver,
				value:   fmt.Sprintf("v%d", ver),
				delete:  rng.Intn(5) == 0,
			})
		}
	}

	tbl := buildVersionTable(t, kvs, 512)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	windows := [][2]uint64{
		{1, math.MaxUint64}, // full range
		{50, 150},
		{1, 30},
		{180, 250}, // partially above data
		{300, 400}, // entirely above data => empty
		{100, 100}, // single version
	}

	for _, w := range windows {
		lower, upper := w[0], w[1]
		t.Run(fmt.Sprintf("window_%d_%d", lower, upper), func(t *testing.T) {
			want := referenceKeys(kvs, lower, upper)

			// Unbounded iterator, per-entry filtered (no storage skipping).
			itUnbounded := tbl.NewIterator(0)
			gotUnbounded := iterateKeys(itUnbounded, lower, upper)
			require.NoError(t, itUnbounded.Close())

			// Bounded iterator WITH block skipping enabled.
			itBounded := tbl.NewIterator(0)
			itBounded.SetVersionBounds(lower, upper)
			gotBounded := iterateKeys(itBounded, lower, upper)
			require.NoError(t, itBounded.Close())

			require.Equal(t, want, gotUnbounded, "unbounded scan mismatch")
			require.Equal(t, want, gotBounded, "bounded scan must equal unbounded filtered scan")
		})
	}
}

// TestBlocksProvablyPruned asserts that blocks whose version range lies entirely
// outside the window are actually skipped (not loaded). We build distinct version
// bands so that a window selecting only the high band must skip the low-band
// blocks, and verify via the storage-level predicate that those blocks are pruned.
func TestBlocksProvablyPruned(t *testing.T) {
	var kvs []vkv
	// Low band: keys aaa* at versions 1..3.
	for i := 0; i < 60; i++ {
		kvs = append(kvs, vkv{key: fmt.Sprintf("aaa%04d", i), version: uint64(1 + i%3), value: "v"})
	}
	// High band: keys zzz* at versions 500..503 (incl. a delete).
	for i := 0; i < 60; i++ {
		kvs = append(kvs, vkv{key: fmt.Sprintf("zzz%04d", i), version: uint64(500 + i%4), value: "v", delete: i == 3})
	}

	tbl := buildVersionTable(t, kvs, 512)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	n := tbl.offsetsLength()
	require.Greater(t, n, 2)

	// Window selecting only the high band.
	lower, upper := uint64(490), uint64(510)
	it := tbl.NewIterator(0)
	it.SetVersionBounds(lower, upper)
	defer it.Close()

	prunable, kept := 0, 0
	for i := 0; i < n; i++ {
		if it.blockOutsideWindow(i) {
			prunable++
		} else {
			kept++
		}
	}
	require.Greater(t, prunable, 0, "expected at least one low-band block to be prunable")
	require.Greater(t, kept, 0, "expected at least one high-band block to be kept")

	// The bounded scan must return exactly the high-band entries in the window.
	got := iterateKeys(it, lower, upper)
	want := referenceKeys(kvs, lower, upper)
	require.Equal(t, want, got)
	// Sanity: all returned keys are high-band.
	for _, k := range got {
		require.Equal(t, "zzz", string(y.ParseKey([]byte(k)))[:3])
	}
}

// TestVersionZeroBlockNotSkipped is a regression test for a lower-bound bug:
// on an UntilTs-only scan (window lower bound 0), a block whose entire version
// range is [0,0] must NOT be skipped, because version-0 keys are in-window.
//
// Note: when a block's min and max are both 0, the appended flatbuffer fields are
// omitted (value==default), so VersionRangePresent() is false and the block is
// conservatively never pruned. Either way the version-0 data must be returned.
func TestVersionZeroBlockNotSkipped(t *testing.T) {
	var kvs []vkv
	// A band of version-0 keys ...
	for i := 0; i < 40; i++ {
		kvs = append(kvs, vkv{key: fmt.Sprintf("aaa%04d", i), version: 0, value: "v"})
	}
	// ... and a band of higher-version keys to force multiple blocks/bands.
	for i := 0; i < 40; i++ {
		kvs = append(kvs, vkv{key: fmt.Sprintf("zzz%04d", i), version: uint64(500 + i%4), value: "v"})
	}
	tbl := buildVersionTable(t, kvs, 512)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	// Window lower=0 (UntilTs-only scan semantics), upper=100.
	it := tbl.NewIterator(0)
	it.SetVersionBounds(0, 100)
	defer it.Close()

	// The version-0 keys are in-window; the bounded scan must surface every one
	// of them (and none of the high-version keys).
	want := referenceKeys(kvs, 0, 100)
	require.NotEmpty(t, want)
	got := iterateKeys(it, 0, 100)
	require.Equal(t, want, got, "version-0 block must not be skipped on a lower=0 window")
}

// TestOldFormatTableNeverSkipped simulates a table written by older code that
// lacks version-range metadata: it must open fine, report HasVersionRange=false,
// and never be pruned by the block/table skip predicates.
func TestOldFormatTableNeverSkipped(t *testing.T) {
	tbl := buildOldFormatTable(t)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	require.False(t, tbl.HasVersionRange(), "old-format table must not advertise a version range")
	// Table-level skip must never fire for an old table, even for a window that
	// could not possibly intersect any plausible range.
	require.False(t, tbl.OutsideVersionRange(1000, 2000))
	require.False(t, tbl.OutsideVersionRange(0, 0))

	// Block-level skip must never fire either.
	it := tbl.NewIterator(0)
	defer it.Close()
	it.SetVersionBounds(1000, 2000)
	for i := 0; i < tbl.offsetsLength(); i++ {
		var ko fb.BlockOffset
		require.True(t, tbl.offsets(&ko, i))
		require.False(t, ko.VersionRangePresent(), "old block must not advertise a version range")
		require.False(t, it.blockOutsideWindow(i), "old-format block must never be skipped")
	}

	// And iteration still yields every key (skipping is disabled for old tables).
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	require.Equal(t, 50, count)
}
