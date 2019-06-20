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
	"github.com/dgraph-io/badger/v2/options"
)

// Options are params for creating DB object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
//
// Each option X is documented on the WithX method.
type Options struct {
	// Required options.

	Dir      string
	ValueDir string

	// Usually modified options.

	SyncWrites          bool
	TableLoadingMode    options.FileLoadingMode
	ValueLogLoadingMode options.FileLoadingMode
	NumVersionsToKeep   int
	ReadOnly            bool
	Truncate            bool
	Logger              Logger

	// Fine tuning options.

	MaxTableSize        int64
	LevelSizeMultiplier int
	MaxLevels           int
	ValueThreshold      int
	NumMemtables        int

	NumLevelZeroTables      int
	NumLevelZeroTablesStall int

	LevelOneSize       int64
	ValueLogFileSize   int64
	ValueLogMaxEntries uint32

	NumCompactors     int
	CompactL0OnClose  bool
	LogRotatesToFlush int32

	// Transaction start and commit timestamps are managed by end-user.
	// This is only useful for databases built on top of Badger (like Dgraph).
	// Not recommended for most users.
	managedTxns bool

	// 4. Flags for testing purposes
	// ------------------------------
	maxBatchCount int64 // max entries in batch
	maxBatchSize  int64 // max batch size in bytes

}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs with the WithX methods.
func DefaultOptions(path string) Options {
	return Options{
		LevelOneSize:        256 << 20,
		LevelSizeMultiplier: 10,
		TableLoadingMode:    options.MemoryMap,
		ValueLogLoadingMode: options.MemoryMap,
		// table.MemoryMap to mmap() the tables.
		// table.Nothing to not preload the tables.
		MaxLevels:               7,
		MaxTableSize:            64 << 20,
		NumCompactors:           2, // Compactions can be expensive. Only run 2.
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 10,
		NumMemtables:            5,
		SyncWrites:              true,
		NumVersionsToKeep:       1,
		CompactL0OnClose:        true,
		// Nothing to read/write value log using standard File I/O
		// MemoryMap to mmap() the value log files
		// (2^30 - 1)*2 when mmapping < 2^31 - 1, max int32.
		// -1 so 2*ValueLogFileSize won't overflow on 32-bit systems.
		ValueLogFileSize: 1<<30 - 1,

		ValueLogMaxEntries: 1000000,
		ValueThreshold:     32,
		Truncate:           false,
		Logger:             defaultLogger,
		LogRotatesToFlush:  2,
	}
}

// LSMOnlyOptions follows from DefaultOptions, but sets a higher ValueThreshold
// so values would be colocated with the LSM tree, with value log largely acting
// as a write-ahead log only. These options would reduce the disk usage of value
// log, and make Badger act more like a typical LSM tree.
func LSMOnlyOptions(path string) Options {
	// Max value length which fits in uint16.
	// Let's not set any other options, because they can cause issues with the
	// size of key-value a user can pass to Badger. For e.g., if we set
	// ValueLogFileSize to 64MB, a user can't pass a value more than that.
	// Setting it to ValueLogMaxEntries to 1000, can generate too many files.
	// These options are better configured on a usage basis, than broadly here.
	// The ValueThreshold is the most important setting a user needs to do to
	// achieve a heavier usage of LSM tree.
	// NOTE: If a user does not want to set 64KB as the ValueThreshold because
	// of performance reasons, 1KB would be a good option too, allowing
	// values smaller than 1KB to be colocated with the keys in the LSM tree.
	return DefaultOptions(path).WithValueThreshold(65500)
}

// WithDir modifies returns a new Options value where Dir has been set to the given value.
// The original Options value is never modified.
//
// Dir is the path of the directory where key data will be stored in.
// If it doesn't exist, Badger will try to create it for you.
// This is set automatically to be the path given to `DefaultOptions`.
func (opt Options) WithDir(val string) Options {
	opt.Dir = val
	return opt
}

// WithValueDir modifies returns a new Options value where ValueDir has been set to the given value.
// The original Options value is never modified.
//
// ValueDir is the path of the directory where value data will be stored in.
// If it doesn't exist, Badger will try to create it for you.
// This is set automatically to be the path given to `DefaultOptions`.
func (opt Options) WithValueDir(val string) Options {
	opt.ValueDir = val
	return opt
}

// WithSyncWrites modifies returns a new Options value where SyncWrites has been set to the given value.
// The original Options value is never modified.
//
// When SyncWrites is true all writes are synced to disk. Setting this to false would achieve better
// performance, but may cause data loss in case of crash.
// The default value of SyncWrites is true.
func (opt Options) WithSyncWrites(val bool) Options {
	opt.SyncWrites = val
	return opt
}

// WithTableLoadingMode modifies returns a new Options value where TableLoadingMode has been set to the given value.
// The original Options value is never modified.
//
// TableLoadingMode indicates which file loading mode should be used for the LSM tree data files.
// The default value of TableLoadingMode is options.MemoryMap.
func (opt Options) WithTableLoadingMode(val options.FileLoadingMode) Options {
	opt.TableLoadingMode = val
	return opt
}

// WithValueLogLoadingMode modifies returns a new Options value where ValueLogLoadingMode has been set to the given value.
// The original Options value is never modified.
//
// ValueLogLoadingMode indicates which file loading mode should be used for the value log data files.
// The default value of ValueLogLoadingMode is options.MemoryMap.
func (opt Options) WithValueLogLoadingMode(val options.FileLoadingMode) Options {
	opt.ValueLogLoadingMode = val
	return opt
}

// WithNumVersionsToKeep modifies returns a new Options value where NumVersionsToKeep has been set to the given value.
// The original Options value is never modified.
//
// NumVersionsToKeep sets how many versions to keep per key at most.
// The default value of NumVersionsToKeep is 1.
func (opt Options) WithNumVersionsToKeep(val int) Options {
	opt.NumVersionsToKeep = val
	return opt
}

// WithReadOnly modifies returns a new Options value where ReadOnly has been set to the given value.
// The original Options value is never modified.
//
// When ReadOnly is true the DB will be opened on read-only mode.
// Multiple processes can open the same Badger DB.
// Note: if the DB being opened had crashed before and has vlog data to be replayed,
// ReadOnly will cause Open to fail with an appropriate message.
// The default value of ReadOnly is false.
func (opt Options) WithReadOnly(val bool) Options {
	opt.ReadOnly = val
	return opt
}

// WithTruncate modifies returns a new Options value where Truncate has been set to the given value.
// The original Options value is never modified.
//
// Truncate indicates whether value log files should be truncated to delete corrupt data, if any.
// This option is ignored when ReadOnly is true.
// The default value of Truncate is false.
func (opt Options) WithTruncate(val bool) Options {
	opt.Truncate = val
	return opt
}

// WithLogger modifies returns a new Options value where Logger has been set to the given value.
// The original Options value is never modified.
//
// Logger provides a way to configure what logger each value of badger.DB uses.
// The default value of Logger writes to stderr using the log package from the Go standard library.
func (opt Options) WithLogger(val Logger) Options {
	opt.Logger = val
	return opt
}

// WithMaxTableSize modifies returns a new Options value where MaxTableSize has been set to the given value.
// The original Options value is never modified.
//
// MaxTableSize sets the maximum size in bytes for each LSM table or file.
// The default value of MaxTableSize is 64MB.
func (opt Options) WithMaxTableSize(val int64) Options {
	opt.MaxTableSize = val
	return opt
}

// WithLevelSizeMultiplier modifies returns a new Options value where LevelSizeMultiplier has been set to the given value.
// The original Options value is never modified.
//
// LevelSizeMultiplier sets the ratio between the maximum sizes of contiguous levels in the LSM.
// Once a level grows to be larger than this ratio allowed, the compaction process will be triggered.
// The default value of LevelSizeMultiplier is 10.
func (opt Options) WithLevelSizeMultiplier(val int) Options {
	opt.LevelSizeMultiplier = val
	return opt
}

// WithMaxLevels modifies returns a new Options value where MaxLevels has been set to the given value.
// The original Options value is never modified.
//
// Maximum number of levels of compaction allowed in the LSM.
// The default value of MaxLevels is 7.
func (opt Options) WithMaxLevels(val int) Options {
	opt.MaxLevels = val
	return opt
}

// WithValueThreshold modifies returns a new Options value where ValueThreshold has been set to the given value.
// The original Options value is never modified.
//
// ValueThreshold sets the threshold used to decide whether a value is stored directly in the LSM tree or
// separatedly in the log value files.
// The default value of ValueThreshold is 32, but LSMOnlyOptions sets it to 65500.
func (opt Options) WithValueThreshold(val int) Options {
	opt.ValueThreshold = val
	return opt
}

// WithNumMemtables modifies returns a new Options value where NumMemtables has been set to the given value.
// The original Options value is never modified.
//
// NumMemtables sets the maximum number of tables to keep in memory before stalling.
// The default value of NumMemtables is 5.
func (opt Options) WithNumMemtables(val int) Options {
	opt.NumMemtables = val
	return opt
}

// WithNumLevelZeroTables modifies returns a new Options value where NumLevelZeroTables has been set to the given value.
// The original Options value is never modified.
//
// NumLevelZeroTables sets the maximum number of Level 0 tables before compaction starts.
// The default value of NumLevelZeroTables is 5.
func (opt Options) WithNumLevelZeroTables(val int) Options {
	opt.NumLevelZeroTables = val
	return opt
}

// WithNumLevelZeroTablesStall modifies returns a new Options value where NumLevelZeroTablesStall has been set to the given value.
// The original Options value is never modified.
//
// NumLevelZeroTablesStall sets the number of Level 0 tables that once reached causes the DB to stall until compaction succeeds.
// The default value of NumLevelZeroTablesStall is 10.
func (opt Options) WithNumLevelZeroTablesStall(val int) Options {
	opt.NumLevelZeroTablesStall = val
	return opt
}

// WithLevelOneSize modifies returns a new Options value where LevelOneSize has been set to the given value.
// The original Options value is never modified.
//
// LevelOneSize sets the maximum total size for Level 1.
// The default value of LevelOneSize is 20MB.
func (opt Options) WithLevelOneSize(val int64) Options {
	opt.LevelOneSize = val
	return opt
}

// WithValueLogFileSize modifies returns a new Options value where ValueLogFileSize has been set to the given value.
// The original Options value is never modified.
//
// ValueLogFileSize sets the maximum size of a single value log file.
// The default value of ValueLogFileSize is 1GB.
func (opt Options) WithValueLogFileSize(val int64) Options {
	opt.ValueLogFileSize = val
	return opt
}

// WithValueLogMaxEntries modifies returns a new Options value where ValueLogMaxEntries has been set to the given value.
// The original Options value is never modified.
//
// ValueLogMaxEntries sets the maximum number of entries a value log file can hold approximately.
// A actual size limit of a value log file is the minimum of ValueLogFileSize and ValueLogMaxEntries.
// The default value of ValueLogMaxEntries is one million (1000000).
func (opt Options) WithValueLogMaxEntries(val uint32) Options {
	opt.ValueLogMaxEntries = val
	return opt
}

// WithNumCompactors modifies returns a new Options value where NumCompactors has been set to the given value.
// The original Options value is never modified.
//
// NumCompactors sets the number of compaction workers to run concurrently.
// Setting this to zero stops compactions, which could eventually cause writes to block forever.
// The default value of NumCompactors is 2.
func (opt Options) WithNumCompactors(val int) Options {
	opt.NumCompactors = val
	return opt
}

// WithCompactL0OnClose modifies returns a new Options value where CompactL0OnClose has been set to the given value.
// The original Options value is never modified.
//
// CompactL0OnClose determines whether Level 0 should be compacted before closing the DB.
// This ensures that both reads and writes are efficient when the DB is opened later.
// The default value of CompactL0OnClose is true.
func (opt Options) WithCompactL0OnClose(val bool) Options {
	opt.CompactL0OnClose = val
	return opt
}

// WithLogRotatesToFlush modifies returns a new Options value where LogRotatesToFlush has been set to the given value.
// The original Options value is never modified.
//
// LogRotatesToFlush sets the number of value log file rotates after which the Memtables are flushed to disk.
// This is useful in write loads with fewer keys and larger values. This work load
// would fill up the value logs quickly, while not filling up the Memtables. Thus, on a crash
// and restart, the value log head could cause the replay of a good number of value log files
// which can slow things on start.
// The default value of LogRotatesToFlush is 2.
func (opt Options) WithLogRotatesToFlush(val int32) Options {
	opt.LogRotatesToFlush = val
	return opt
}
