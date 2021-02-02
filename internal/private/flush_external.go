package private

import "github.com/dgraph-io/badger/v3/internal/manifest"

// FlushExternalTable is a hook for linking files into L0 without assigning a
// global sequence number, mimicking a flush.
//
// Calls to flush a sstable may fail if the file's sequence numbers are not
// greater than the current commit pipeline's sequence number. On success the
// commit pipeline's published sequence number will be moved to the file's
// highest sequence number.
//
// This function is wrapped in a safer, more ergonomic API in the
// internal/replay package. Clients should use the replay package rather than
// calling this private hook directly.
var FlushExternalTable func(interface{}, string, *manifest.FileMetadata) error

// RatchetSeqNum is a hook for allocating and publishing sequence numbers up
// to a specific absolute value.
//
// This function is used by the internal/replay package to ensure replayed
// operations receive the same absolute sequence number.
var RatchetSeqNum func(interface{}, uint64)
