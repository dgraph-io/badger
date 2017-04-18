package succinct

import (
	"github.com/stretchr/testify/require"
	"testing"
)

/*
banana AoS:

0 a
1 ana
2 anana
3 banana
4 na
5 nana
*/

// Creates "banana" aos compressed representation.
func BananaCompressedAoS() *compressedAoS {
	characters := []byte{'a', 'b', 'n'}
	charIndexes := []int{0, 3, 4}

	nextChar := []int{endOfStringMark, 4, 5, 2, 0, 1}

	return &compressedAoS{
		characters:  characters,
		charIndexes: charIndexes,
		nextCharIdx: nextChar,
	}
}

func BananaStore() *SuccinctStore {
	aos := BananaCompressedAoS()

	input2AoS := []int{3, 2, 5, 1, 4, 0}
	aoS2Input := []int{5, 3, 1, 0, 4, 2}

	return &SuccinctStore{
		aos:       aos,
		input2AoS: input2AoS,
		aoS2Input: aoS2Input,
	}
}

func TestLookupAOS(t *testing.T) {
	aos := BananaCompressedAoS()

	require.EqualValues(t, []byte("a"), aos.lookupAoS(0, 1))

	require.EqualValues(t, []byte("a"), aos.lookupAoS(0, 2))

	require.EqualValues(t, []byte("ba"), aos.lookupAoS(3, 2))

	require.EqualValues(t, []byte("banan"), aos.lookupAoS(3, 5))

	require.EqualValues(t, []byte("nana"), aos.lookupAoS(5, 4))
}

func TestExtract(t *testing.T) {
	bs := BananaStore()

	require.EqualValues(t, []byte("b"), bs.Extract(0, 1))

	require.EqualValues(t, []byte("banana"), bs.Extract(0, 6))

	require.EqualValues(t, []byte("anana"), bs.Extract(1, 5))

	require.EqualValues(t, []byte("a"), bs.Extract(5, 1))
}

func TestSearch(t *testing.T) {
	bs := BananaStore()

	require.EqualValues(t, []int{0}, bs.Search([]byte("ban")))

	require.EqualValues(t, []int{3, 1}, bs.Search([]byte("an")))

	require.EqualValues(t, []int{5, 3, 1}, bs.Search([]byte("a")))

	require.EqualValues(t, []int{0}, bs.Search([]byte("banana")))
}
