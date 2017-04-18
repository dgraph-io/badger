package succinct

import (
	"bytes"
	"sort"
)

// Store is a Succinct like store
type Store interface {
	Extract(offset int, len int) []byte
	Search(str string) []int

	// we leave append, count, rangesearch and wildcardsearch for now
}

const (
	endOfStringMark = -1
)

type compressedAoS struct {
	// Succinct representation of AoS stores:
	// (a) all unique characters in the input file in sorted order; and
	// (b) for each character, first AoS index with suffix starting with that character.
	characters  []byte
	charIndexes []int

	// We leave compresion of nextCharIdx for later
	nextCharIdx []int
}

// Returns number of bytes len1 and prefix of lenght len1 < len
// from the suffix on idx pos in AoS
func (aos *compressedAoS) lookupAoS(idx int, length int) []byte {
	var buffer bytes.Buffer
	var idx1 int
	idx1 = idx
	for i := 0; i < length; i++ {
		if idx1 == endOfStringMark {
			break
		}

		foundIdx := sort.Search(len(aos.charIndexes), func(id int) bool {
			return aos.charIndexes[id] >= idx1
		})

		if foundIdx == len(aos.charIndexes) { // idx1 not found
			foundIdx = foundIdx - 1
		}

		// If there is no idx1 in charIndexes, we want it's lower bound.
		if aos.charIndexes[foundIdx] > idx1 {
			foundIdx = foundIdx - 1
		}

		buffer.WriteByte(aos.characters[foundIdx])
		idx1 = aos.lookupNPA(idx1)
	}
	return buffer.Bytes()
}

func (aos *compressedAoS) lookupNPA(idx int) int {
	return aos.nextCharIdx[idx]
}

type SuccinctStore struct {
	aos *compressedAoS

	// Compression of this tables left for later.
	input2AoS []int
	aoS2Input []int
}

// Length returns the number of characters in the store.
func (store *SuccinctStore) Length() int {
	return len(store.input2AoS)
}

func (store *SuccinctStore) Extract(offset int, len int) []byte {
	return store.aos.lookupAoS(store.input2AoS[offset], len)
}

func (store *SuccinctStore) Search(str []byte) []int {
	length := store.Length()
	firstOccurence := sort.Search(length, func(id int) bool {
		return bytes.Compare(store.aos.lookupAoS(id, len(str)), str) >= 0
	})

	if firstOccurence == length {
		return make([]int, 0)
	}

	afterLastOccurence := sort.Search(length, func(id int) bool {
		return bytes.Compare(store.aos.lookupAoS(id, len(str)), str) > 0
	})

	resultLength := afterLastOccurence - firstOccurence
	res := make([]int, resultLength)
	for i := firstOccurence; i < afterLastOccurence; i++ {
		res[i-firstOccurence] = store.aoS2Input[i]
	}
	return res
}
