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
	"github.com/dgraph-io/badger/enums"
	"github.com/dgraph-io/badger/options"
)

// defaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var defaultOptions = options.Options{
	LevelOneSize:        256 << 20,
	LevelSizeMultiplier: 10,
	TableLoadingMode:    enums.MemoryMap,
	ValueLogLoadingMode: enums.MemoryMap,
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
