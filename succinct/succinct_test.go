/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package succinct

import (
	"testing"

	"github.com/stretchr/testify/require"
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
func bananaCompressedAoS() *compressedAoS {
	characters := []byte{'a', 'b', 'n'}
	charIndexes := []int{0, 3, 4}

	nextChar := []int{endOfStringMark, 4, 5, 2, 0, 1}

	return &compressedAoS{
		characters:  characters,
		charIndexes: charIndexes,
		nextCharIdx: nextChar,
	}
}

func bananaStore() *SuccinctStore {
	aos := bananaCompressedAoS()

	input2AoS := []int{3, 2, 5, 1, 4, 0}
	aoS2Input := []int{5, 3, 1, 0, 4, 2}

	return &SuccinctStore{
		aos:       aos,
		input2AoS: input2AoS,
		aoS2Input: aoS2Input,
	}
}

func TestLookupAOS(t *testing.T) {
	aos := bananaCompressedAoS()

	require.EqualValues(t, []byte("a"), aos.lookupAoS(0, 1))
	require.EqualValues(t, []byte("a"), aos.lookupAoS(0, 2))
	require.EqualValues(t, []byte("ba"), aos.lookupAoS(3, 2))
	require.EqualValues(t, []byte("banan"), aos.lookupAoS(3, 5))
	require.EqualValues(t, []byte("nana"), aos.lookupAoS(5, 4))
}

func TestExtract(t *testing.T) {
	bs := bananaStore()

	require.EqualValues(t, []byte("b"), bs.Extract(0, 1))
	require.EqualValues(t, []byte("banana"), bs.Extract(0, 6))
	require.EqualValues(t, []byte("anana"), bs.Extract(1, 5))
	require.EqualValues(t, []byte("a"), bs.Extract(5, 1))
}

func TestSearch(t *testing.T) {
	bs := bananaStore()

	require.EqualValues(t, []int{0}, bs.Search([]byte("ban")))
	require.EqualValues(t, []int{3, 1}, bs.Search([]byte("an")))
	require.EqualValues(t, []int{5, 3, 1}, bs.Search([]byte("a")))
	require.EqualValues(t, []int{0}, bs.Search([]byte("banana")))
}
