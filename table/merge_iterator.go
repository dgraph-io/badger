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
	smaller mergeIteratorChild
	bigger  mergeIteratorChild

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
		child.valid = child.merge.smaller.valid
		if child.valid {
			child.key = child.merge.smaller.key
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
	if !mt.bigger.valid {
		return
	}
	for mt.smaller.valid {
		cmp := y.CompareKeys(mt.smaller.key, mt.bigger.key)
		// Both the keys are equal.
		if cmp == 0 {
			// Ignore the value in second iterator.
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.smaller.iter {
				mt.smaller.setKey()
				secondValid = mt.smaller.valid
			} else {
				mt.bigger.setKey()
				secondValid = mt.bigger.valid
			}
			if !secondValid {
				// Swap smaller and bigger only if second points to the smaller one and the bigger is valid.
				if mt.second == mt.smaller.iter && mt.bigger.valid {
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
	mt.smaller, mt.bigger = mt.bigger, mt.smaller
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	if mt.smaller.merge != nil {
		mt.smaller.merge.Next()
	} else if mt.smaller.concat != nil {
		mt.smaller.concat.Next()
	} else {
		mt.smaller.iter.Next()
	}
	mt.smaller.setKey()
	mt.fixSmallerBigger()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.smaller.iter.Rewind()
	mt.smaller.setKey()
	mt.bigger.iter.Rewind()
	mt.bigger.setKey()
	mt.fixSmallerBigger()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.smaller.iter.Seek(key)
	mt.smaller.setKey()
	mt.bigger.iter.Seek(key)
	mt.bigger.setKey()
	mt.fixSmallerBigger()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.smaller.valid
}

// Key returns the key associated with the current iterator
func (mt *MergeIterator) Key() []byte {
	return mt.smaller.key
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() y.ValueStruct {
	return mt.smaller.iter.Value()
}

// Close implements y.Iterator
func (mt *MergeIterator) Close() error {
	err1 := mt.smaller.iter.Close()
	err2 := mt.bigger.iter.Close()
	if err1 != nil {
		return errors.Wrap(err1, "MergeIterator")
	}
	return errors.Wrap(err2, "MergeIterator")
}

// NewMergeIterator creates a merge iterator
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
		mi.smaller.setIterator(iters[0])
		mi.bigger.setIterator(iters[1])
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]y.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
