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
	"fmt"

	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	left  node
	right node

	small *node

	// When the two iterators have the same value, the value in the second iterator is ignored.
	// On level 0, we can have multiple iterators with the same key. In this case we want to
	// use value of the iterator that was added first to the merge iterator. Second keeps track of the
	// iterator that was added second so that we can resolve the same key conflict.
	second  y.Iterator
	reverse bool
}

type node struct {
	valid bool
	key   []byte
	iter  y.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (child *node) setIterator(iter y.Iterator) {
	child.iter = iter
	child.merge, _ = iter.(*MergeIterator)
	child.concat, _ = iter.(*ConcatIterator)
}

func (child *node) setKey() {
	if child.merge != nil {
		child.valid = child.merge.left.valid
		if child.valid {
			child.key = child.merge.left.key
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

func (mt *MergeIterator) fix() {
	if !mt.bigger().valid {
		return
	}
	for mt.small.valid {
		cmp := y.CompareKeys(mt.small.key, mt.bigger().key)
		// Both the keys are equal.
		if cmp == 0 {
			// Key conflict. Ignore the value in second iterator.
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.small.iter {
				mt.small.setKey()
				secondValid = mt.small.valid
			} else if mt.second == mt.bigger().iter {
				mt.bigger().setKey()
				secondValid = mt.bigger().valid
			} else {
				fmt.Println(mt.second)
				panic("////")
			}
			if !secondValid {
				// Swap small and big only if second points to
				// the small one and the big is valid.
				if mt.second == mt.small.iter && mt.bigger().valid {
					// mt.small = &mt.right
					mt.swapSmall()
				}
				return
			}
			continue
		}
		if mt.reverse {
			if cmp < 0 {
				mt.swapSmall()
			}
		} else {
			if cmp > 0 {
				mt.swapSmall()
			}
		}
		return
	}
	mt.swapSmall()
}

func (mt *MergeIterator) bigger() *node {
	if mt.small == &mt.left {
		return &mt.right
	}
	return &mt.left
}

func (mt *MergeIterator) swapSmall() {
	if mt.small == &mt.left {
		mt.small = &mt.right
		return
	}
	if mt.small == &mt.right {
		mt.small = &mt.left
		return
	}
	fmt.Println("mt.small is nil ", mt.small == nil)
	panic(".....")
	// mt.left, mt.right = mt.right, mt.left
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
	mt.fix()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.left.iter.Rewind()
	mt.left.setKey()
	mt.right.iter.Rewind()
	mt.right.setKey()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.left.iter.Seek(key)
	mt.left.setKey()
	mt.right.iter.Seek(key)
	mt.right.setKey()
	mt.fix()
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
	err1 := mt.left.iter.Close()
	err2 := mt.right.iter.Close()
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
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// mi.small.setIterator(iters[0])
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]y.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
