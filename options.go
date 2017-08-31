package badger

import (
	"time"

	"github.com/dgraph-io/badger/options"
)

// Options are params for creating DB object.
type Options struct {
	Dir      string // Directory to store the data in. Should exist and be writable.
	ValueDir string // Directory to store the value log in. Can be the same as Dir.
	// Should exist and be writable.

	// The following affect all levels of LSM tree.
	MaxTableSize        int64                   // Each table (or file) is at most this size.
	LevelSizeMultiplier int                     // Equals SizeOf(Li+1)/SizeOf(Li).
	MaxLevels           int                     // Maximum number of levels of compaction.
	ValueThreshold      int                     // If value size >= this threshold, only store value offsets in tree.
	TableLoadingMode    options.FileLoadingMode // How should LSM tree be accessed.

	NumMemtables int // Maximum number of tables to keep in memory, before stalling.

	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int
	// If we hit this number of Level 0 tables, we will stall until L0 is compacted away.
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	LevelOneSize int64

	// Control how value log is accessed (using file r/w or mmap)
	ValueLogLoadingMode options.FileLoadingMode

	// Run value log garbage collection if we can reclaim at least this much space. This is a ratio.
	ValueGCThreshold float64
	// How often to run value log garbage collector.
	ValueGCRunInterval time.Duration

	// Size of single value log file.
	ValueLogFileSize int64

	// Sync all writes to disk. Setting this to true would slow down data loading significantly.
	SyncWrites bool

	// Number of compaction workers to run concurrently.
	NumCompactors int

	// Flags for testing purposes.
	DoNotCompact bool // Stops LSM tree from compactions.

	maxBatchSize int64 // max batch size in bytes
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	DoNotCompact:        false,
	LevelOneSize:        256 << 20,
	LevelSizeMultiplier: 10,
	TableLoadingMode:    options.LoadToRAM,
	// table.MemoryMap to mmap() the tables.
	// table.Nothing to not preload the tables.
	MaxLevels:               7,
	MaxTableSize:            64 << 20,
	NumCompactors:           3,
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	NumMemtables:            5,
	SyncWrites:              false,
	ValueLogLoadingMode:     options.FileIO,
	// Nothing to read/write value log using standard File I/O
	// MemoryMap to mmap() the value log files
	ValueGCRunInterval: 10 * time.Minute,
	ValueGCThreshold:   0.5, // Set to zero to not run GC.
	ValueLogFileSize:   1 << 30,
	ValueThreshold:     20,
}
