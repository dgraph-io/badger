/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"slices"
	"sort"

	"github.com/dgraph-io/badger/v4/y"
)

// RangeStrictness controls how the Right end of a Range behaves.
type RangeStrictness uint8

// String implementation
func (s RangeStrictness) String() string {
	switch s {
	case RangeStrictnessInexact:
		return "inexact"
	case RangeStrictnessExact:
		return "exact"
	case RangeStrictnessExclusive:
		return "exclusive"
	default:
		return "invalid"
	}
}

const (
	// RangeStrictnessInexact both ends are inclusive, Right end includes prefix matches.
	// Default uninitialized range mode, allows zero-value Range{} to act non-bounding selector.
	RangeStrictnessInexact RangeStrictness = y.RangeStrictnessInexact
	// RangeStrictnessExact Right end is inclusive, but prefix matches are excluded.
	RangeStrictnessExact RangeStrictness = y.RangeStrictnessExact
	// RangeStrictnessExclusive Right and is exclusive and prefix matches are excluded.
	RangeStrictnessExclusive RangeStrictness = y.RangeStrictnessExclusive
)

// Range expresses key range that may be exclusive, inclusive or prefix-matched
type Range struct {
	Left       []byte
	Right      []byte
	Strictness RangeStrictness
}

// String stringer implementation
func (r Range) String() string {
	var end string

	switch r.Strictness {
	case RangeStrictnessExclusive:
		end = ")"
	case RangeStrictnessExact:
		end = "]"
	case RangeStrictnessInexact:
		end = "]+"
	default:
		end = "?"
	}

	return fmt.Sprintf("[%x, %x%s", r.Left, r.Right, end)
}

// RangeSize a Range with estimated size of the keys attached
type RangeSize struct {
	Range
	Size int64 // used by db.Ranges
}

// Empty indicates range that will never match anything
func (r Range) Empty() bool {
	return y.RangeEmpty(uint8(r.Strictness), r.Left, r.Right)
}

// Infinite returns true if range is unbounded.
// Only inexact range with empty Right or maximum possible Right is truly unbounded.
func (r Range) Infinite() bool {
	return y.RangeInfinite(uint8(r.Strictness), r.Left, r.Right)
}

// Compare tests relation of two ranges.
// Returns -1 if range A is strictly left of range B.
// Returns 0 if range A overlaps with range B.
// Returns +1 if range A is strictly right of range B.
func (r Range) Compare(other Range) int {
	return y.RangeCompareRange(uint8(r.Strictness), uint8(other.Strictness), r.Left, r.Right, other.Left, other.Right)
}

// CompareKey compares key with a range.
// Returns <0 if key is strictly less than left bound of a range.
// Returns 0 if key is within the bounds of a range.
// Returns >0 if key is strictly greater than right bound of a range.
// Note that magnitude may exceed 1.
func (r Range) CompareKey(key []byte) int {
	return y.KeyCompareRange(uint8(r.Strictness), r.Left, r.Right, key)
}

// Extend returns range that encompasses both ranges, including any area in-between in case of non-intersecting ranges.
func (r Range) Extend(other Range) Range {
	if bytes.Compare(other.Left, r.Left) < 0 {
		r.Left = other.Left
	}

	switch r.Strictness {
	case RangeStrictnessInexact:
		reqlen := min(len(other.Right), len(r.Right))
		rm := r.Right[:reqlen]
		om := other.Right[:reqlen]

		if bytes.Compare(om, rm) > 0 {
			r.Right = om
			r.Strictness = other.Strictness
		} else {
			r.Right = rm
		}
	case RangeStrictnessExact, RangeStrictnessExclusive:
		if bytes.Compare(other.Right, r.Right) > 0 {
			r.Right = other.Right
		}

		r.Strictness = min(r.Strictness, other.Strictness)
	}

	return r
}

// Prefix returns new range constrained by a new prefix, preserving any bytes longer than the prefix and
// the original strictness.
func (r Range) Prefix(prefix []byte) Range {
	if !bytes.HasPrefix(r.Left, prefix) {
		if len(r.Left) >= len(prefix) {
			r.Left = slices.Clone(r.Left)
			copy(r.Left, prefix)
		} else {
			r.Left = prefix
		}
	}

	if !bytes.HasPrefix(r.Right, prefix) {
		if len(r.Right) >= len(prefix) {
			r.Right = slices.Clone(r.Right)
			copy(r.Right, prefix)
		} else {
			r.Right = prefix
		}
	}

	return r
}

// RangesCompact returns smallest possible equivalent set of ranges
func RangesCompact(ranges ...Range) (res []Range) {
	if len(ranges) == 0 {
		return ranges
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].Left, ranges[j].Left) < 0
	})

	cur := ranges[0]

	for _, r := range ranges[1:] {
		if cur.Compare(r) == 0 {
			cur = cur.Extend(r)
			if cur.Infinite() {
				return []Range{{}}
			}

			continue
		}

		res = append(res, cur)

		cur = r
	}

	res = append(res, cur)

	return
}
