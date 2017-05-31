package succinct

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	bananaAoS2Input = []uint32{6, 5, 3, 1, 0, 4, 2}
	bananaInput2AoS = []uint32{4, 3, 6, 2, 5, 1, 0}
	bananaNext      = NewSliceWrapper([]uint32{4, 0, 5, 6, 3, 1, 2})

	bananaSampledBitmap    = roaring.BitmapOf(0, 4, 5, 6)
	bananaSampledAoS2Input = []uint32{3, 0, 2, 1}
	bananaSampledInput2AoS = []uint32{1, 3, 2, 0}
)

func BananaAoSMapping() *AoSMapping {
	return &AoSMapping{
		alpha:     2,
		next:      bananaNext,
		aoS2Input: bananaSampledAoS2Input,
		bPos:      bananaSampledBitmap,
		input2AoS: bananaSampledInput2AoS,
	}
}

func TestAoS2InputLookups(t *testing.T) {
	mapping := BananaAoSMapping()
	length := mapping.next.Length()

	aoS2Input := make([]uint32, 7)
	for i := uint32(0); i < length; i++ {
		aoS2Input[i] = mapping.LookupAoS2Input(i)
	}
	require.EqualValues(t, bananaAoS2Input, aoS2Input)
}

func TestInput2AoSLookups(t *testing.T) {
	mapping := BananaAoSMapping()
	length := mapping.next.Length()

	input2AoS := make([]uint32, 7)
	for i := uint32(0); i < length; i++ {
		input2AoS[i] = mapping.LookupInput2AoS(i)
	}
	require.EqualValues(t, bananaInput2AoS, input2AoS)
}

func TestNewMapping(t *testing.T) {
	mapping := NewMapping(bananaAoS2Input, nil, 2)

	require.EqualValues(t, bananaSampledAoS2Input, mapping.aoS2Input)
	require.EqualValues(t, bananaSampledInput2AoS, mapping.input2AoS)

	require.EqualValues(t, bananaSampledBitmap, mapping.bPos)
}
