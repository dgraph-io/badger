/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRange_Prefix tests Range.Prefix behaviour
func TestRange_Prefix(t *testing.T) {
	prefixed := Range{}.Prefix([]byte("abc"))

	require.Equal(t, []byte("abc"), prefixed.Left)
	require.Equal(t, []byte("abc"), prefixed.Right)
	require.Equal(t, RangeStrictnessInexact, prefixed.Strictness)

	prefixed = Range{Strictness: RangeStrictnessExact}.Prefix([]byte("abc"))

	require.Equal(t, []byte("abc"), prefixed.Left)
	require.Equal(t, []byte("abc"), prefixed.Right)
	require.Equal(t, RangeStrictnessExact, prefixed.Strictness)

	prefixed = Range{Left: []byte("aa"), Right: []byte("bb")}.Prefix([]byte("abc"))

	require.Equal(t, []byte("abc"), prefixed.Left)
	require.Equal(t, []byte("abc"), prefixed.Right)
	require.Equal(t, RangeStrictnessInexact, prefixed.Strictness)

	prefixed = Range{Left: []byte("aaaa"), Right: []byte("bbbb")}.Prefix([]byte("abc"))

	require.Equal(t, []byte("abca"), prefixed.Left)
	require.Equal(t, []byte("abcb"), prefixed.Right)
	require.Equal(t, RangeStrictnessInexact, prefixed.Strictness)
}

// TestRange_Extend tests Range.Extend behaviour
func TestRange_Extend(t *testing.T) {
	r := Range{Left: []byte{10}, Right: []byte{20}}.Extend(Range{Left: []byte{15}, Right: []byte{40}})

	require.Equal(t, uint8(10), r.Left[0])
	require.Equal(t, uint8(40), r.Right[0])
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20}}.Extend(Range{Left: []byte{30}, Right: []byte{40}})

	require.Equal(t, uint8(10), r.Left[0])
	require.Equal(t, uint8(40), r.Right[0])
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20, 30}}.Extend(Range{Left: []byte{10}, Right: []byte{20, 40}})

	require.Equal(t, []byte{10}, r.Left)
	require.Equal(t, []byte{20, 40}, r.Right)
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20, 30}}.Extend(Range{Left: []byte{10}, Right: []byte{20}})

	require.Equal(t, []byte{10}, r.Left)
	require.Equal(t, []byte{20}, r.Right)
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20, 30}}.Extend(Range{Left: []byte{10}, Right: []byte{20, 0xff}})

	require.Equal(t, []byte{10}, r.Left)
	require.Equal(t, []byte{20, 0xff}, r.Right)
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: nil, Right: []byte{10}}.Extend(Range{Left: []byte{0xf0}, Right: nil})

	require.True(t, r.Infinite())

	r = Range{Strictness: RangeStrictnessExclusive}.Extend(Range{Strictness: RangeStrictnessExact})

	require.Equal(t, RangeStrictnessExact, r.Strictness)

	r = Range{Strictness: RangeStrictnessExact}.Extend(Range{Strictness: RangeStrictnessExclusive})

	require.Equal(t, RangeStrictnessExact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20, 30}}.Extend(Range{Left: []byte{10}, Right: []byte{10, 40}})

	require.Equal(t, []byte{10}, r.Left)
	require.Equal(t, []byte{20, 30}, r.Right)
	require.Equal(t, RangeStrictnessInexact, r.Strictness)

	r = Range{Left: []byte{10}, Right: []byte{20}}.Extend(Range{Left: []byte{20}, Right: []byte{20, 25}})

	require.Equal(t, []byte{10}, r.Left)
	require.Equal(t, []byte{20}, r.Right)
	require.Equal(t, RangeStrictnessInexact, r.Strictness)
}

// TestRangesCompact tests RangesCompact behaviour
func TestRangesCompact(t *testing.T) {
	res := RangesCompact(
		Range{Left: []byte{10}, Right: []byte{20}},
		Range{Left: []byte{20}, Right: []byte{30}},
		Range{Left: []byte{30}, Right: []byte{40}},
		Range{Left: []byte{50}, Right: []byte{60}},
		Range{Left: []byte{55}, Right: []byte{70}},
	)

	require.Equal(t, []Range{{Left: []byte{10}, Right: []byte{40}}, {Left: []byte{50}, Right: []byte{70}}}, res)

	res = RangesCompact(
		Range{Left: []byte{10}, Right: []byte{20}},
		Range{Left: []byte{20}, Right: []byte{30}},
		Range{Left: []byte{30}, Right: []byte{40}},
		Range{Left: []byte{50}, Right: []byte{60}},
		Range{Left: []byte{55}, Right: []byte{70}},
		Range{},
	)

	require.Equal(t, []Range{{}}, res)

	res = RangesCompact(
		Range{Left: []byte{10}, Right: []byte{30}},
		Range{Left: []byte{31}, Right: []byte{60}},
		Range{Left: []byte{0}, Right: []byte{100}},
	)

	require.Equal(t, []Range{{Left: []byte{0}, Right: []byte{100}}}, res)
}
