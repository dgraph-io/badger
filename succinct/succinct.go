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

	// We leave compression of nextCharIdx for later
	nextCharIdx []int
}

// Returns number of bytes len1 and prefix of length len1 < len
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
		// If there is no idx1 in charIndexes, we want it's lower bound.
		if foundIdx == len(aos.charIndexes) || aos.charIndexes[foundIdx] > idx1 {
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

	resultLength := sort.Search(length-firstOccurence, func(id int) bool {
		return bytes.Compare(store.aos.lookupAoS(id+firstOccurence, len(str)), str) > 0
	})

	occurrences := make([]int, resultLength)
	for i := firstOccurence; i < firstOccurence+resultLength; i++ {
		occurrences[i-firstOccurence] = store.aoS2Input[i]
	}
	return occurrences
}
