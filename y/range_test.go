/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test RangeEmpty behaviour
func TestRangeEmpty(t *testing.T) {
	require.False(t, RangeEmpty(RangeStrictnessInexact, nil, nil))
	require.False(t, RangeEmpty(RangeStrictnessExact, nil, nil))
	require.True(t, RangeEmpty(RangeStrictnessExclusive, nil, nil))

	require.False(t, RangeEmpty(RangeStrictnessInexact, []byte{0}, []byte{0}))
	require.False(t, RangeEmpty(RangeStrictnessExact, []byte{0}, []byte{0}))
	require.True(t, RangeEmpty(RangeStrictnessExclusive, []byte{0}, []byte{0}))
	require.False(t, RangeEmpty(RangeStrictnessExclusive, []byte{0}, []byte{1}))

	require.False(t, RangeEmpty(RangeStrictnessInexact, []byte{0, 5}, []byte{0}))
}

// Test RangeInfinite behaviour
func TestRangeInfinite(t *testing.T) {
	require.True(t, RangeInfinite(RangeStrictnessInexact, nil, nil))
	require.True(t, RangeInfinite(RangeStrictnessInexact, nil, []byte{0xff, 0xff}))

	require.False(t, RangeInfinite(RangeStrictnessInexact, []byte{0x00}, nil))
	require.False(t, RangeInfinite(RangeStrictnessInexact, []byte{0x00}, []byte{0xff, 0xff}))
	require.False(t, RangeInfinite(RangeStrictnessInexact, nil, []byte{0xff, 0xff, 0xf0}))

	require.False(t, RangeInfinite(RangeStrictnessExact, nil, nil))
	require.False(t, RangeInfinite(RangeStrictnessExclusive, nil, nil))
}

// Test KeyCompareRange behaviour
func TestKeyCompareRange(t *testing.T) {
	t.Run("infinite range", func(t *testing.T) {
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, nil, nil, nil))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, nil, nil, []byte("abc")))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, nil, nil, []byte{0xff}))
	})

	t.Run("inexact range", func(t *testing.T) {
		require.Greater(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, nil))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{64}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{100}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{128}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{200}))

		require.Greater(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{50, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{64, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{100, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{128, 50}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessInexact, []byte{64}, []byte{128}, []byte{129, 50}))
	})

	t.Run("exact range", func(t *testing.T) {
		require.Greater(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, nil))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{64}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{100}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{128}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{200}))

		require.Greater(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{50, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{64, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{100, 50}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{128, 50}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExact, []byte{64}, []byte{128}, []byte{129, 50}))
	})

	t.Run("exclusive range", func(t *testing.T) {
		require.Greater(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, nil))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{64}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{100}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{128}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{200}))

		require.Greater(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{50, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{64, 50}))
		require.Equal(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{100, 50}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{128, 50}))
		require.Less(t, 0, KeyCompareRange(RangeStrictnessExclusive, []byte{64}, []byte{128}, []byte{129, 50}))
	})
}

// Test RangeCompareRange behaviour
func TestRangeCompareRange(t *testing.T) {
	check := func(
		t *testing.T,
		expected int,
		strictnessA, strictnessB uint8,
		leftA, rightA, leftB, rightB []byte,
	) {
		t.Helper()

		switch expected {
		case 0:
			require.Equal(t, 0, RangeCompareRange(strictnessA, strictnessB, leftA, rightA, leftB, rightB))
			require.Equal(t, 0, RangeCompareRange(strictnessB, strictnessA, leftB, rightB, leftA, rightA))

			if strictnessB == RangeStrictnessInexact {
				require.Equal(
					t,
					0,
					RangeCompareRange(strictnessA, strictnessB, append(leftA, 1), append(rightA, 1), leftB, rightB),
				)
				require.Equal(
					t,
					0,
					RangeCompareRange(strictnessB, strictnessA, leftB, rightB, append(leftA, 1), append(rightA, 1)),
				)
			}

			if strictnessA == RangeStrictnessInexact {
				require.Equal(
					t,
					0,
					RangeCompareRange(strictnessA, strictnessB, leftA, rightA, append(leftB, 1), append(rightB, 1)),
				)
				require.Equal(
					t,
					0,
					RangeCompareRange(strictnessB, strictnessA, append(leftB, 1), append(rightB, 1), leftA, rightA),
				)
			}
		case 1:
			require.Less(t, 0, RangeCompareRange(strictnessA, strictnessB, leftA, rightA, leftB, rightB))
			require.Greater(t, 0, RangeCompareRange(strictnessB, strictnessA, leftB, rightB, leftA, rightA))
		case -1:
			require.Greater(t, 0, RangeCompareRange(strictnessA, strictnessB, leftA, rightA, leftB, rightB))
			require.Less(t, 0, RangeCompareRange(strictnessB, strictnessA, leftB, rightB, leftA, rightA))
		}
	}

	b0 := []byte{0}
	b10 := []byte{10}
	b10p1 := []byte{10, 1}
	b20 := []byte{20}
	b30 := []byte{30}
	b40 := []byte{40}
	b40p1 := []byte{40, 1}
	b50 := []byte{50}
	b60 := []byte{60}

	t.Run("infinite range", func(t *testing.T) {
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, nil, nil, nil, nil)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, nil, nil, b0, b0)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExclusive, nil, nil, b0, b0)
	})

	t.Run("inexact range", func(t *testing.T) {
		check(t, -1, RangeStrictnessInexact, RangeStrictnessInexact, b0, b10, b20, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b40, b50, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessInexact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessInexact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessInexact, RangeStrictnessExact, b0, b10, b20, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExact, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessExact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessExact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessInexact, RangeStrictnessExclusive, b0, b10, b20, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExclusive, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExclusive, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExclusive, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessInexact, RangeStrictnessExclusive, b20, b30, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessExclusive, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessExclusive, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessInexact, RangeStrictnessExclusive, b50, b60, b10, b40)
	})

	t.Run("exact range", func(t *testing.T) {
		check(t, -1, RangeStrictnessExact, RangeStrictnessInexact, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExact, RangeStrictnessInexact, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessInexact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessInexact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessInexact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessInexact, b40, b50, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessInexact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessInexact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessExact, RangeStrictnessExact, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExact, RangeStrictnessExact, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExact, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessExact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessExact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessExact, RangeStrictnessExclusive, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExact, RangeStrictnessExclusive, b0, b10, b10p1, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExclusive, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExclusive, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExact, RangeStrictnessExclusive, b20, b30, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessExclusive, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessExclusive, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExact, RangeStrictnessExclusive, b50, b60, b10, b40)
	})

	t.Run("exclusive range", func(t *testing.T) {
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessInexact, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessInexact, b0, b10, b10p1, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessInexact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessInexact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessInexact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessInexact, b40, b50, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessInexact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessInexact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExact, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExact, b0, b10, b10p1, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExact, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessExact, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessExact, b20, b30, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessExact, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessExact, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessExact, b50, b60, b10, b40)

		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExclusive, b0, b10, b20, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExclusive, b0, b10, b10p1, b40)
		check(t, -1, RangeStrictnessExclusive, RangeStrictnessExclusive, b0, b10, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessExclusive, b10, b20, b10, b40)
		check(t, 0, RangeStrictnessExclusive, RangeStrictnessExclusive, b20, b30, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessExclusive, b40, b50, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessExclusive, b40p1, b50, b10, b40)
		check(t, 1, RangeStrictnessExclusive, RangeStrictnessExclusive, b50, b60, b10, b40)
	})
}
