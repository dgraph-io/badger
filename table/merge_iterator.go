/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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

package table

import (
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	small mergeIteratorChild
	big   mergeIteratorChild

	// When the two iterators have the same value, the value in the second iterator is ignored.
	second  y.Iterator
	reverse bool
}

type mergeIteratorChild struct {
	valid bool
	key   []byte
	iter  y.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (child *mergeIteratorChild) setIterator(iter y.Iterator) {
	child.iter = iter
	child.merge, _ = iter.(*MergeIterator)
	child.concat, _ = iter.(*ConcatIterator)
}

func (child *mergeIteratorChild) setKey() {
	if child.merge != nil {
		child.valid = child.merge.small.valid
		if child.valid {
			child.key = child.merge.small.key
		}
	} else if child.concat != nil {
		child.valid = child.concat.Valid()
		if child.valid {
			child.key = child.concat.Key()
		}
	} else {
		child.valid = child.iter.Valid()
		if child.valid {
			child.key = child.iter.Key()
		}
	}
}

func (mt *MergeIterator) fixSmallerBigger() {
	if !mt.big.valid {
		return
	}
	for mt.small.valid {
		cmp := y.CompareKeys(mt.small.key, mt.big.key)
		// Both the keys are equal.
		if cmp == 0 {
			// Ignore the value in second iterator.
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.small.iter {
				mt.small.setKey()
				secondValid = mt.small.valid
			} else {
				mt.big.setKey()
				secondValid = mt.big.valid
			}
			if !secondValid {
				// Swap small and big only if second points to
				// the small one and the big is valid.
				if mt.second == mt.small.iter && mt.big.valid {
					mt.swap()
				}
				return
			}
			continue
		}
		if mt.reverse {
			if cmp < 0 {
				mt.swap()
			}
		} else {
			if cmp > 0 {
				mt.swap()
			}
		}
		return
	}
	mt.swap()
}

func (mt *MergeIterator) swap() {
	mt.small, mt.big = mt.big, mt.small
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	if mt.small.merge != nil {
		mt.small.merge.Next()
	} else if mt.small.concat != nil {
		mt.small.concat.Next()
	} else {
		mt.small.iter.Next()
	}
	mt.small.setKey()
	mt.fixSmallerBigger()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.small.iter.Rewind()
	mt.small.setKey()
	mt.big.iter.Rewind()
	mt.big.setKey()
	mt.fixSmallerBigger()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.small.iter.Seek(key)
	mt.small.setKey()
	mt.big.iter.Seek(key)
	mt.big.setKey()
	mt.fixSmallerBigger()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.small.valid
}

// Key returns the key associated with the current iterator.
func (mt *MergeIterator) Key() []byte {
	return mt.small.key
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() y.ValueStruct {
	return mt.small.iter.Value()
}

// Close implements y.Iterator.
func (mt *MergeIterator) Close() error {
	err1 := mt.small.iter.Close()
	err2 := mt.big.iter.Close()
	if err1 != nil {
		return errors.Wrap(err1, "MergeIterator")
	}
	return errors.Wrap(err2, "MergeIterator")
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []y.Iterator, reverse bool) y.Iterator {
	if len(iters) == 0 {
		return nil
	} else if len(iters) == 1 {
		return iters[0]
	} else if len(iters) == 2 {
		mi := &MergeIterator{
			second:  iters[1],
			reverse: reverse,
		}
		mi.small.setIterator(iters[0])
		mi.big.setIterator(iters[1])
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]y.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
