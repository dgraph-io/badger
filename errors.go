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

package badger

import (
	"encoding/hex"

	"github.com/pkg/errors"
)

// ErrInvalidDir is returned when Badger cannot find the directory
// from where it is supposed to load the key-value store.
var ErrInvalidDir = errors.New("Invalid Dir, directory does not exist")

// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
// range.
var ErrValueLogSize = errors.New("Invalid ValueLogFileSize, must be between 1MB and 2GB")

// ErrKeyNotFound is returned when key isn't found on a txn.Get.
var ErrKeyNotFound = errors.New("Key not found")

// ErrConflict is returned when a transaction conflicts with another transaction. This can happen if
// the read rows had been updated concurrently by another transaction.
var ErrConflict = errors.New("Transaction Conflict. Please retry.")

// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
var ErrReadOnlyTxn = errors.New("No sets or deletes are allowed in a read-only transaction.")

// ErrEmptyKey is returned if an empty key is passed on an update function.
var ErrEmptyKey = errors.New("Key cannot be empty.")

const maxKeySize = 1 << 20

func exceedsMaxKeySizeError(key []byte) error {
	return errors.Errorf("Key with size %d exceeded %dMB limit. Key:\n%s",
		len(key), maxKeySize<<20, hex.Dump(key[:1<<10]))
}

func exceedsMaxValueSizeError(value []byte, maxValueSize int64) error {
	return errors.Errorf("Value with size %d exceeded ValueLogFileSize (%dMB). Key:\n%s",
		len(value), maxValueSize<<20, hex.Dump(value[:1<<10]))
}

var (
	// ErrRetry is returned when a log file containing the value is not found.
	// This usually indicates that it may have been garbage collected, and the
	// operation needs to be retried.
	ErrRetry = errors.New("Unable to find log file. Please retry")

	// ErrCasMismatch is returned when a CompareAndSet operation has failed due
	// to a counter mismatch.
	ErrCasMismatch = errors.New("CompareAndSet failed due to counter mismatch")

	// ErrKeyExists is returned by SetIfAbsent metadata bit is set, but the
	// key already exists in the store.
	ErrKeyExists = errors.New("SetIfAbsent failed since key already exists")

	// ErrThresholdZero is returned if threshold is set to zero, and value log GC is called.
	// In such a case, GC can't be run.
	ErrThresholdZero = errors.New(
		"Value log GC can't run because threshold is set to zero")

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New(
		"Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after KV::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
)
