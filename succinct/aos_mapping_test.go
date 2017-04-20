package succinct

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
	"testing"
)

func BananaAoSMapping() *AoSMapping {
	next := []uint32{4, 0, 5, 6, 3, 1, 2}
	return &AoSMapping{
		alpha:     2,
		next:      NewSliceWrapper(next),
		aoS2Input: []uint32{3, 0, 2, 1},
		bPos:      roaring.BitmapOf(0, 4, 5, 6),
		input2AoS: []uint32{1, 3, 2, 0},
	}
}

func TestAoS2InputLookups(t *testing.T) {
	mapping := BananaAoSMapping()
	length := mapping.next.Length()

	expectedAoS2Input := [7]uint32{6, 5, 3, 1, 0, 4, 2}

	var aoS2Input [7]uint32
	for i := uint32(0); i < length; i++ {
		aoS2Input[i] = mapping.LookupAoS2Input(i)
	}
	require.EqualValues(t, expectedAoS2Input, aoS2Input)
}

func TestInput2AoSLookups(t *testing.T) {
	mapping := BananaAoSMapping()
	length := mapping.next.Length()

	expectedInput2AoS := [7]uint32{4, 3, 6, 2, 5, 1, 0}

	var input2AoS [7]uint32
	for i := uint32(0); i < length; i++ {
		input2AoS[i] = mapping.LookupInput2AoS(i)
	}
	require.EqualValues(t, expectedInput2AoS, input2AoS)
}
