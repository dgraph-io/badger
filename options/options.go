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

package options

import (
	"math"

	"github.com/dgraph-io/badger/enums"
)

// Option provides a way to modify the values in Options.
// The options are defined unter github.com/dgraph-io/badger/options to
// avoid namespace pollution of the badger API.
type Option func(Options) Options

// NOTE: Keep the comments in the following to 75 chars width, so they
// format nicely in godoc.

// Options are params for creating DB object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. If it doesn't exist, Badger will
	// try to create it for you.
	Dir string
	// Directory to store the value log in. Can be the same as Dir. If it
	// doesn't exist, Badger will try to create it for you.
	ValueDir string

	// 2. Frequently modified flags
	// -----------------------------
	// Sync all writes to disk. Setting this to false would achieve better
	// performance, but may cause data to be lost.
	SyncWrites bool

	// How should LSM tree be accessed.
	TableLoadingMode enums.FileLoadingMode

	// How should value log be accessed.
	ValueLogLoadingMode enums.FileLoadingMode

	// How many versions to keep per key.
	NumVersionsToKeep int

	// Open the DB as read-only. With this set, multiple processes can
	// open the same Badger DB. Note: if the DB being opened had crashed
	// before and has vlog data to be replayed, ReadOnly will cause Open
	// to fail with an appropriate message.
	ReadOnly bool

	// Truncate value log to delete corrupt data, if any. Would not truncate if ReadOnly is set.
	Truncate bool

	// DB-specific logger which will override the global logger.
	Logger

	// 3. Flags that user might want to review
	// ----------------------------------------
	// The following affect all levels of LSM tree.
	MaxTableSize        int64 // Each table (or file) is at most this size.
	LevelSizeMultiplier int   // Equals SizeOf(Li+1)/SizeOf(Li).
	MaxLevels           int   // Maximum number of levels of compaction.
	// If value size >= this threshold, only store value offsets in tree.
	ValueThreshold int
	// Maximum number of tables to keep in memory, before stalling.
	NumMemtables int
	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int

	// If we hit this number of Level 0 tables, we will stall until L0 is
	// compacted away.
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	LevelOneSize int64

	// Size of single value log file.
	ValueLogFileSize int64

	// Max number of entries a value log file can hold (approximately). A value log file would be
	// determined by the smaller of its file size and max entries.
	ValueLogMaxEntries uint32

	// Number of compaction workers to run concurrently. Setting this to zero would stop compactions
	// to happen within LSM tree. If set to zero, writes could block forever.
	NumCompactors int

	// When closing the DB, force compact Level 0. This ensures that both reads and writes are
	// efficient when the DB is opened later.
	CompactL0OnClose bool

	// After this many number of value log file rotates, there would be a force flushing of memtable
	// to disk. This is useful in write loads with fewer keys and larger values. This work load
	// would fill up the value logs quickly, while not filling up the Memtables. Thus, on a crash
	// and restart, the value log head could cause the replay of a good number of value log files
	// which can slow things on start.
	LogRotatesToFlush int32

	// Transaction start and commit timestamps are managed by end-user.
	// This is only useful for databases built on top of Badger (like Dgraph).
	// Not recommended for most users, you should instead use badger.OpenManaged.
	ManagedTxns bool
}

// Logger provides an abstraction over logs.
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

// WithEmptyOptions provides a way to delete all the default values.
func WithEmptyOptions() Option {
	return func(opt Options) Options { return Options{} }
}

// WithTreeDir sets the directory to store the LSM tree data in.
// If it doesn't exist, Badger will create it for you.
func WithTreeDir(val string) Option {
	return func(opt Options) Options { opt.Dir = val; return opt }
}

// WithValueDir sets the directory to store the value log in.
// If it doesn't exist, Badger will try to create it for you.
// It can be the same as WithTreeDir.
func WithValueDir(val string) Option {
	return func(opt Options) Options { opt.ValueDir = val; return opt }
}

// WithSyncWrites set whether all writes to should be synced to disk.
// Setting this to false would achieve better performance, but may cause data to be lost.
func WithSyncWrites(val bool) Option {
	return func(opt Options) Options { opt.SyncWrites = val; return opt }
}

// WithTableLoadingMode sets how the LSM tree should be accessed.
func WithTableLoadingMode(val enums.FileLoadingMode) Option {
	return func(opt Options) Options { opt.TableLoadingMode = val; return opt }
}

// WithValueLogLoadingMode sets how the value log should be accessed.
func WithValueLogLoadingMode(val enums.FileLoadingMode) Option {
	return func(opt Options) Options { opt.ValueLogLoadingMode = val; return opt }
}

// WithMaxVersionsToKeep sets how many versions at most should be kept per key.
// If the value is -1 all version will be kept.
func WithMaxVersionsToKeep(val int) Option {
	if val < 1 {
		val = math.MaxInt32
	}
	return func(opt Options) Options { opt.NumVersionsToKeep = val; return opt }
}

// WithReadOnly sets whether the DB should be opened in read-only mode.
// With this set, multiple processes can open the same Badger DB.
//
// Note: if the DB being opened had crashed before and has vlog data to be replayed,
// ReadOnly will cause Open to fail with an appropriate message.
func WithReadOnly(val bool) Option {
	return func(opt Options) Options { opt.ReadOnly = val; return opt }
}

// WithTruncate sets whether the value log should be truncated to delete corrupt data, if any.
// This options is ignored if the DB is opened in read-only mode.
func WithTruncate(val bool) Option {
	return func(opt Options) Options { opt.Truncate = val; return opt }
}

// WithLogger sets the DB-specific logger which overrides the global logger.
func WithLogger(val Logger) Option {
	return func(opt Options) Options { opt.Logger = val; return opt }
}

// TODO: is WithMaxTableSize really in bytes?

// WithMaxTableSize sets the maximum size in bytes for all tables at all levels of the LSM tree.
func WithMaxTableSize(val int64) Option {
	return func(opt Options) Options { opt.MaxTableSize = val; return opt }
}

// WithLevelSizeMultiplier sets the size ratio in between contiguous levels of the LSM tree.
func WithLevelSizeMultiplier(val int) Option {
	return func(opt Options) Options { opt.LevelSizeMultiplier = val; return opt }
}

// WithMaxLevels sets the maximum number of levels of compaction in the LSM tree.
func WithMaxLevels(val int) Option {
	return func(opt Options) Options { opt.MaxLevels = val; return opt }
}

// TODO: is the description below correct?

// WithValueThreshold sets the size threshold below which values will be stored offsets in the tree.
func WithValueThreshold(val int) Option {
	return func(opt Options) Options { opt.ValueThreshold = val; return opt }
}

// TODO: what does it mean to stall?

// WithMaxMemtables sets the maximum number of tables to keep in memory, before stalling.
func WithMaxMemtables(val int) Option {
	return func(opt Options) Options { opt.NumMemtables = val; return opt }
}

// WithMaxLevelZeroTables sets the maximum number of Level 0 tables before we start compacting.
func WithMaxLevelZeroTables(val int) Option {
	return func(opt Options) Options { opt.NumLevelZeroTables = val; return opt }
}

// WithMaxLevelZeroTablesStall sets the numberof Level 0 tables at which the DB stalls until
// Level 0 is compacted away.
func WithMaxLevelZeroTablesStall(val int) Option {
	return func(opt Options) Options { opt.NumLevelZeroTablesStall = val; return opt }
}

// TODO: is it in bytes, number of tables?

// WithLevelOneSize sets the maximum size for Level 1 in bytes.
func WithLevelOneSize(val int64) Option {
	return func(opt Options) Options { opt.LevelOneSize = val; return opt }
}

// WithValueLogFileSize sets the size of a single value log file in bytes.
func WithValueLogFileSize(val int64) Option {
	return func(opt Options) Options { opt.ValueLogFileSize = val; return opt }
}

// WithValueLogMaxEntries sets the maximum number of entries a value log file can hold.
// The actual number of entries would be smaller if the maximum file size limit is reached first.
func WithValueLogMaxEntries(val uint32) Option {
	return func(opt Options) Options { opt.ValueLogMaxEntries = val; return opt }
}

// WithNumCompactors sets the number of compaction workers to run concurrently.
// Setting this to zero stops compactions to happen within LSM tree,
// which could eventually cause writes to block forever.
func WithNumCompactors(val int) Option {
	return func(opt Options) Options { opt.NumCompactors = val; return opt }
}

// WithCompactLevelZeroOnClose sets whether to force compaction of Level Zero before closing the DB.
// This ensures that both reads and writes are efficient when the DB is opened later.
func WithCompactLevelZeroOnClose(val bool) Option {
	return func(opt Options) Options { opt.CompactL0OnClose = val; return opt }
}

// WithLogRotatesToFlush sets the number of value log file rotations after which the memtable
// will be forced to flush to disk.
//
// This is useful in write loads with fewer keys and larger values. This work load
// would fill up the value logs quickly, while not filling up the Memtables. Thus, on a crash
// and restart, the value log head could cause the replay of a good number of value log files
// which can slow things down on start.
func WithLogRotatesToFlush(val int32) Option {
	return func(opt Options) Options { opt.LogRotatesToFlush = val; return opt }
}

// WithLSMOnly sets a higher ValueThreshold so values would be colocated with the
// LSM tree, with value log largely actingas a write-ahead log only.
// These options would reduce the disk usage of value log, and make Badger act
// more like a typical LSM tree.
func WithLSMOnly() Option {
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
	return WithValueThreshold(6650)
}
