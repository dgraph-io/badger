/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/dgraph-io/ristretto/v2/z"
)

// pendingEntries stores the writes buffered by a transaction before commit.
//
// It replaces the previous map[string]*Entry. The previous representation paid
// one heap allocation per write because Go copies the key into a freshly
// allocated string on every `m[string(key)] = e` insert (the assignment-key
// conversion is NOT elided by the compiler, only the lookup conversion is).
//
// Instead we key on a 64-bit fingerprint (z.MemHash) of the user key and store
// the entries directly. Because a 64-bit hash can collide, each bucket holds a
// small slice of entries that is disambiguated with a byte-wise key comparison.
// In the overwhelmingly common no-collision case a bucket holds a single entry
// and lookups/inserts are O(1) with zero allocation.
//
// Semantics preserved vs the old map:
//   - read-your-own-writes: get() matches on exact user-key bytes.
//   - last-write-wins for the same (key, version): set() overwrites in place.
//   - managed multi-version: set() reports the displaced entry when the new
//     entry has a different version, so the caller can move it to
//     duplicateWrites.
//   - iteration: rangeEntries() visits every live entry exactly once (in
//     unspecified order, exactly like Go map iteration was).
type pendingEntries struct {
	m map[uint64][]*Entry
	// n is the number of distinct live keys held.
	n int
	// hash is the key-fingerprint function. It is a field purely so tests can
	// force collisions; production always uses z.MemHash.
	hash func([]byte) uint64
}

func newPendingEntries() *pendingEntries {
	return &pendingEntries{
		m:    make(map[uint64][]*Entry),
		hash: z.MemHash,
	}
}

// A nil *pendingEntries is valid and behaves as an empty, read-only store.
// This mirrors the previous map[string]*Entry field which was left nil for
// read-only transactions and relied on nil-map reads being safe.

func (p *pendingEntries) len() int {
	if p == nil {
		return 0
	}
	return p.n
}

// get returns the live entry for key, if present.
func (p *pendingEntries) get(key []byte) (*Entry, bool) {
	if p == nil {
		return nil, false
	}
	bucket := p.m[p.hash(key)]
	for _, e := range bucket {
		if bytes.Equal(e.Key, key) {
			return e, true
		}
	}
	return nil, false
}

// set inserts or replaces the entry for e.Key.
//
// It returns the previously stored entry for the same key (if any) and a
// `duplicate` flag that is true only when an entry already existed AND it had a
// different version than e. This mirrors the original modify() logic:
//
//	if old, ok := pendingWrites[string(e.Key)]; ok && old.version != e.version {
//	    duplicateWrites = append(duplicateWrites, old)
//	}
//	pendingWrites[string(e.Key)] = e
func (p *pendingEntries) set(e *Entry) (old *Entry, duplicate bool) {
	fp := p.hash(e.Key)
	bucket := p.m[fp]
	for i, cur := range bucket {
		if bytes.Equal(cur.Key, e.Key) {
			old = cur
			bucket[i] = e
			return old, old.version != e.version
		}
	}
	// New key for this bucket.
	p.m[fp] = append(bucket, e)
	p.n++
	return nil, false
}

// rangeEntries calls fn for every live entry exactly once.
func (p *pendingEntries) rangeEntries(fn func(*Entry)) {
	if p == nil {
		return
	}
	for _, bucket := range p.m {
		for _, e := range bucket {
			fn(e)
		}
	}
}

// keyArena is a single contiguous buffer from which key+timestamp byte slices
// are carved at commit time. It collapses N per-key y.KeyWithTs allocations
// (one make([]byte, len(key)+8) per entry) into a single allocation per commit.
//
// Safety: the arena allocates fresh storage and copies the user key into it; it
// never mutates or aliases the caller-provided Entry.Key bytes. The produced
// slices are sub-slices of the arena with cap clamped to len, so appending to
// one slice cannot scribble over the next. The arena is referenced by the
// Entry.Key slices handed to the write channel and therefore lives, via those
// references, for as long as the asynchronous write needs it.
type keyArena struct {
	buf []byte
	off int
}

func newKeyArena(size int) *keyArena {
	return &keyArena{buf: make([]byte, size)}
}

// keyWithTs writes key followed by the encoded timestamp into the arena and
// returns the resulting slice. It is byte-for-byte identical to y.KeyWithTs.
//
// If the arena is exhausted (which should not happen when sized correctly) it
// falls back to a standalone allocation so correctness never depends on the
// size estimate.
func (a *keyArena) keyWithTs(key []byte, ts uint64) []byte {
	need := len(key) + 8
	if a.off+need > len(a.buf) {
		out := make([]byte, need)
		copy(out, key)
		binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
		return out
	}
	out := a.buf[a.off : a.off+need : a.off+need]
	a.off += need
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}
