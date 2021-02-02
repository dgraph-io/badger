// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "fmt"

// InternalIterator iterates over a DB's key/value pairs in key order. Unlike
// the Iterator interface, the returned keys are InternalKeys composed of the
// user-key, a sequence number and a key kind. In forward iteration, key/value
// pairs for identical user-keys are returned in descending sequence order. In
// reverse iteration, key/value pairs for identical user-keys are returned in
// ascending sequence order.
//
// InternalIterators provide 5 absolute positioning methods and 2 relative
// positioning methods. The absolute positioning methods are:
//
// - SeekGE
// - SeekPrefixGE
// - SeekLT
// - First
// - Last
//
// The relative positioning methods are:
//
// - Next
// - Prev
//
// The relative positioning methods can be used in conjunction with any of the
// absolute positioning methods with one exception: SeekPrefixGE does not
// support reverse iteration via Prev. It is undefined to call relative
// positioning methods without ever calling an absolute positioning method.
//
// InternalIterators can optionally implement a prefix iteration mode. This
// mode is entered by calling SeekPrefixGE and exited by any other absolute
// positioning method (SeekGE, SeekLT, First, Last). When in prefix iteration
// mode, a call to Next will advance to the next key which has the same
// "prefix" as the one supplied to SeekPrefixGE. Note that "prefix" in this
// context is not a strict byte prefix, but defined by byte equality for the
// result of the Comparer.Split method. An InternalIterator is not required to
// support prefix iteration mode, and can implement SeekPrefixGE by forwarding
// to SeekGE.
//
// Bounds, [lower, upper), can be set on iterators, either using the
// SetBounds() function in the interface, or in implementation specific ways
// during iterator creation. The forward positioning routines (SeekGE, First,
// and Next) only check the upper bound. The reverse positioning routines
// (SeekLT, Last, and Prev) only check the lower bound. It is up to the caller
// to ensure that the forward positioning routines respect the lower bound and
// the reverse positioning routines respect the upper bound (i.e. calling
// SeekGE instead of First if there is a lower bound, and SeekLT instead of
// Last if there is an upper bound). This imposition is done in order to
// elevate that enforcement to the caller (generally pebble.Iterator or
// pebble.mergingIter) rather than having it duplicated in every
// InternalIterator implementation. Additionally, the caller needs to ensure
// that SeekGE/SeekPrefixGE are not called with a key > the upper bound, and
// SeekLT is not called with a key < the lower bound.
// InternalIterator implementations are required to respect the iterator
// bounds, never returning records outside of the bounds with one exception:
// an iterator may generate synthetic RANGEDEL marker records. See
// levelIter.syntheticBoundary for the sole existing example of this behavior.
// Specifically, levelIter can return synthetic keys whose user key is equal
// to the lower/upper bound.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not goroutine-safe, but it is safe to use multiple iterators
// concurrently, either in separate goroutines or switching between the
// iterators in a single goroutine.
//
// It is also safe to use an iterator concurrently with modifying its
// underlying DB, if that DB permits modification. However, the resultant
// key/value pairs are not guaranteed to be a consistent snapshot of that DB
// at a particular point in time.
//
// InternalIterators accumulate errors encountered during operation, exposing
// them through the Error method. All of the absolute positioning methods
// reset any accumulated error before positioning. Relative positioning
// methods return without advancing if the iterator has accumulated an error.
type InternalIterator interface {
	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key. Returns the key and value if the iterator
	// is pointing at a valid entry, and (nil, nil) otherwise. Note that SeekGE
	// only checks the upper bound. It is up to the caller to ensure that key
	// is greater than or equal to the lower bound.
	SeekGE(key []byte) (*InternalKey, []byte)

	// SeekPrefixGE moves the iterator to the first key/value pair whose key is
	// greater than or equal to the given key. Returns the key and value if the
	// iterator is pointing at a valid entry, and (nil, nil) otherwise. Note that
	// SeekPrefixGE only checks the upper bound. It is up to the caller to ensure
	// that key is greater than or equal to the lower bound.
	//
	// The prefix argument is used by some InternalIterator implementations (e.g.
	// sstable.Reader) to avoid expensive operations. A user-defined Split
	// function must be supplied to the Comparer for the DB. The supplied prefix
	// will be the prefix of the given key returned by that Split function. If
	// the iterator is able to determine that no key with the prefix exists, it
	// can return (nil,nil). Unlike SeekGE, this is not an indication that
	// iteration is exhausted.
	//
	// Note that the iterator may return keys not matching the prefix. It is up
	// to the caller to check if the prefix matches.
	//
	// Calling SeekPrefixGE places the receiver into prefix iteration mode. Once
	// in this mode, reverse iteration may not be supported and will return an
	// error. Note that pebble/Iterator.SeekPrefixGE has this same restriction on
	// not supporting reverse iteration in prefix iteration mode until a
	// different positioning routine (SeekGE, SeekLT, First or Last) switches the
	// iterator out of prefix iteration.
	SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte)

	// SeekLT moves the iterator to the last key/value pair whose key is less
	// than the given key. Returns the key and value if the iterator is pointing
	// at a valid entry, and (nil, nil) otherwise. Note that SeekLT only checks
	// the lower bound. It is up to the caller to ensure that key is less than
	// the upper bound.
	SeekLT(key []byte) (*InternalKey, []byte)

	// First moves the iterator the the first key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that First only checks the upper bound. It is up to the
	// caller to ensure that First() is not called when there is a lower bound,
	// and instead call SeekGE(lower).
	First() (*InternalKey, []byte)

	// Last moves the iterator the the last key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Last only checks the lower bound. It is up to the
	// caller to ensure that Last() is not called when there is an upper bound,
	// and instead call SeekLT(upper).
	Last() (*InternalKey, []byte)

	// Next moves the iterator to the next key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Next only checks the upper bound. It is up to the
	// caller to ensure that key is greater than or equal to the lower bound.
	//
	// It is valid to call Next when the iterator is positioned before the first
	// key/value pair due to either a prior call to SeekLT or Prev which returned
	// (nil, nil). It is not allowed to call Next when the previous call to SeekGE,
	// SeekPrefixGE or Next returned (nil, nil).
	Next() (*InternalKey, []byte)

	// Prev moves the iterator to the previous key/value pair. Returns the key
	// and value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Prev only checks the lower bound. It is up to the
	// caller to ensure that key is less than the upper bound.
	//
	// It is valid to call Prev when the iterator is positioned after the last
	// key/value pair due to either a prior call to SeekGE or Next which returned
	// (nil, nil). It is not allowed to call Prev when the previous call to SeekLT
	// or Prev returned (nil, nil).
	Prev() (*InternalKey, []byte)

	// Error returns any accumulated error.
	Error() error

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error

	// SetBounds sets the lower and upper bounds for the iterator. Note that the
	// result of Next and Prev will be undefined until the iterator has been
	// repositioned with SeekGE, SeekPrefixGE, SeekLT, First, or Last.
	SetBounds(lower, upper []byte)

	fmt.Stringer
}
