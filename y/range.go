/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import "bytes"

const (
	// RangeStrictnessInexact see badger.RangeStrictnessInexact for documentation
	RangeStrictnessInexact = iota
	// RangeStrictnessExact see badger.RangeStrictnessExact for documentation
	RangeStrictnessExact
	// RangeStrictnessExclusive see badger.RangeStrictnessExclusive for documentation
	RangeStrictnessExclusive
)

// RangeEmpty see badger.Range#Empty for documentation
func RangeEmpty(strictness uint8, left, right []byte) bool {
	switch strictness {
	case RangeStrictnessInexact:
		if len(left) > len(right) {
			left = left[:len(right)]
		}

		fallthrough
	case RangeStrictnessExact:
		return bytes.Compare(right, left) < 0
	case RangeStrictnessExclusive:
		return bytes.Compare(right, left) <= 0
	default:
		AssertTruef(false, "invalid strictness: %d", strictness)

		return true
	}
}

// RangeInfinite see badger.Range#Infinite for documentation
func RangeInfinite(strictness uint8, left, right []byte) bool {
	if strictness != RangeStrictnessInexact || len(left) != 0 {
		return false
	}

	if len(right) == 0 {
		return true
	}

	// edge-case, when right is specified, but set to a maximum possible value,
	// which still selects everything in inexact mode.
	for _, b := range right {
		if b != 0xff {
			return false
		}
	}

	return true
}

// RangeCompareRange see badger.Range#Compare for documentation
func RangeCompareRange(strictnessA, strictnessB uint8, leftA, rightA, leftB, rightB []byte) int {
	bl := KeyCompareRange(strictnessA, leftA, rightA, leftB)
	if bl > 0 {
		return -1
	}

	if bl == 0 {
		return 0
	}

	return KeyCompareRange(strictnessB, leftB, rightB, leftA)
}

// KeyCompareRange see badger.Range#CompareKey for documentation
func KeyCompareRange(strictness uint8, left, right, key []byte) int {
	if len(left) > 0 && bytes.Compare(key, left) < 0 {
		return -1
	}

	switch strictness {
	case RangeStrictnessInexact:
		if len(key) > len(right) {
			key = key[:len(right)]
		}

		fallthrough
	case RangeStrictnessExact:
		return max(0, bytes.Compare(key, right))
	case RangeStrictnessExclusive:
		return bytes.Compare(key, right) + 1
	}

	AssertTruef(false, "invalid strictness: %d", strictness)

	return 0
}
