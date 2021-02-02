package manifest

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Compare exports the base.Compare type.
type Compare = base.Compare

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// TableInfo contains the common information for table related events.
type TableInfo struct {
	// FileNum is the internal DB identifier for the table.
	FileNum base.FileNum
	// Size is the size of the file in bytes.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest InternalKey
	// Largest is the largest internal key in the table.
	Largest InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum uint64
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum uint64
}

// TableStats contains statistics on a table used for compaction heuristics.
type TableStats struct {
	// Valid true if stats have been loaded for the table. The rest of the
	// structure is populated only if true.
	Valid bool
	// The total number of entries in the table.
	NumEntries uint64
	// The number of point and range deletion entries in the table.
	NumDeletions uint64
	// Estimate of the total disk space that may be dropped by this table's
	// point deletions by compacting them.
	PointDeletionsBytesEstimate uint64
	// Estimate of the total disk space that may be dropped by this table's
	// range deletions by compacting them. This estimate is at data-block
	// granularity and is not updated if compactions beneath the table reduce
	// the amount of reclaimable disk space. It also does not account for
	// overlapping data in L0 and ignores L0 sublevels, but the error that
	// introduces is expected to be small.
	//
	// Tables in the bottommost level of the LSM may have a nonzero estimate
	// if snapshots or move compactions prevented the elision of their range
	// tombstones.
	RangeDeletionsBytesEstimate uint64
}

// FileMetadata holds the metadata for an on-disk table.
type FileMetadata struct {
	// Reference count for the file: incremented when a file is added to a
	// version and decremented when the version is unreferenced. The file is
	// obsolete when the reference count falls to zero.
	refs int32
	// FileNum is the file number.
	FileNum base.FileNum
	// Size is the size of the file, in bytes.
	Size uint64
	// File creation time in seconds since the epoch (1970-01-01 00:00:00
	// UTC). For ingested sstables, this corresponds to the time the file was
	// ingested.
	CreationTime int64
	// Smallest and Largest are the inclusive bounds for the internal keys
	// stored in the table.
	Smallest InternalKey
	Largest  InternalKey
	// Smallest and largest sequence numbers in the table.
	SmallestSeqNum uint64
	LargestSeqNum  uint64
	// True if the file is actively being compacted. Protected by DB.mu.
	Compacting bool
	// Stats describe table statistics. Protected by DB.mu.
	Stats TableStats
	// For L0 files only. Protected by DB.mu. Used to generate L0 sublevels and
	// pick L0 compactions.
	//
	// IsIntraL0Compacting is set to True if this file is part of an intra-L0
	// compaction. When it's true, Compacting must also be true. If Compacting
	// is true and IsIntraL0Compacting is false for an L0 file, the file must
	// be part of a compaction to Lbase.
	IsIntraL0Compacting bool
	// Fields inside the Atomic struct should be accessed atomically.
	Atomic struct {
		// AllowedSeeks is used to determine if a file should be picked for
		// a read triggered compaction.
		// Iterator after every after every positioning operation
		// that returns a user key (eg. Next, Prev, SeekGE, SeekLT, etc).
		AllowedSeeks int64
	}
	subLevel         int
	l0Index          int
	minIntervalIndex int
	maxIntervalIndex int

	// True if user asked us to compact this file. This flag is only set and
	// respected by RocksDB but exists here to preserve its value in the
	// MANIFEST.
	markedForCompaction bool
}

func (m *FileMetadata) String() string {
	return fmt.Sprintf("%s:%s-%s", m.FileNum, m.Smallest, m.Largest)
}

// Validate validates the metadata for consistency with itself, returning an
// error if inconsistent.
func (m *FileMetadata) Validate(cmp Compare, formatKey base.FormatKey) error {
	if base.InternalCompare(cmp, m.Smallest, m.Largest) > 0 {
		return base.CorruptionErrorf("file %s has inconsistent bounds: %s vs %s",
			errors.Safe(m.FileNum), m.Smallest.Pretty(formatKey),
			m.Largest.Pretty(formatKey))
	}
	if m.SmallestSeqNum > m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent seqnum bounds: %d vs %d",
			errors.Safe(m.FileNum), m.SmallestSeqNum, m.LargestSeqNum)
	}
	return nil
}

// TableInfo returns a subset of the FileMetadata state formatted as a
// TableInfo.
func (m *FileMetadata) TableInfo() TableInfo {
	return TableInfo{
		FileNum:        m.FileNum,
		Size:           m.Size,
		Smallest:       m.Smallest,
		Largest:        m.Largest,
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return +1
	default:
		return 0
	}
}

func (m *FileMetadata) cmpSeqNum(b *FileMetadata) int {
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if m.LargestSeqNum != b.LargestSeqNum {
		return cmpUint64(m.LargestSeqNum, b.LargestSeqNum)
	}
	// Then by smallest sequence number.
	if m.SmallestSeqNum != b.SmallestSeqNum {
		return cmpUint64(m.SmallestSeqNum, b.SmallestSeqNum)
	}
	// Break ties by file number.
	return cmpUint64(uint64(m.FileNum), uint64(b.FileNum))
}

func (m *FileMetadata) lessSeqNum(b *FileMetadata) bool {
	return m.cmpSeqNum(b) < 0
}

func (m *FileMetadata) cmpSmallestKey(b *FileMetadata, cmp Compare) int {
	return base.InternalCompare(cmp, m.Smallest, b.Smallest)
}

// KeyRange returns the minimum smallest and maximum largest internalKey for
// all the FileMetadata in iters.
func KeyRange(ucmp Compare, iters ...LevelIterator) (smallest, largest InternalKey) {
	first := true
	for _, iter := range iters {
		for meta := iter.First(); meta != nil; meta = iter.Next() {
			if first {
				first = false
				smallest, largest = meta.Smallest, meta.Largest
				continue
			}
			if base.InternalCompare(ucmp, smallest, meta.Smallest) >= 0 {
				smallest = meta.Smallest
			}
			if base.InternalCompare(ucmp, largest, meta.Largest) <= 0 {
				largest = meta.Largest
			}
		}
	}
	return smallest, largest
}

type bySeqNum []*FileMetadata

func (b bySeqNum) Len() int { return len(b) }
func (b bySeqNum) Less(i, j int) bool {
	return b[i].lessSeqNum(b[j])
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by increasing sequence number.
func SortBySeqNum(files []*FileMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	files []*FileMetadata
	cmp   Compare
}

func (b bySmallest) Len() int { return len(b.files) }
func (b bySmallest) Less(i, j int) bool {
	return b.files[i].cmpSmallestKey(b.files[j], b.cmp) < 0
}
func (b bySmallest) Swap(i, j int) { b.files[i], b.files[j] = b.files[j], b.files[i] }

// SortBySmallest sorts the specified files by smallest key using the supplied
// comparison function to order user keys.
func SortBySmallest(files []*FileMetadata, cmp Compare) {
	sort.Sort(bySmallest{files, cmp})
}

func overlaps(iter LevelIterator, cmp Compare, start, end []byte) LevelSlice {
	startIter := iter.Clone()
	startIter.SeekGE(cmp, start)

	endIter := iter.Clone()
	endIter.SeekGE(cmp, end)

	// endIter is now pointing at the *first* file with a largest key >= end.
	// If there are multiple files including the user key `end`, we want all
	// of them, so move forward.
	for endIter.Current() != nil && cmp(endIter.Current().Largest.UserKey, end) == 0 {
		endIter.Next()
	}
	// LevelSlice uses inclusive bounds, so if we seeked to the end sentinel
	// or nexted too far because Largest.UserKey equaled `end`, go back.
	if !endIter.iter.valid() || cmp(endIter.Current().Smallest.UserKey, end) > 0 {
		endIter.Prev()
	}

	iter = startIter.Clone()
	return LevelSlice{
		iter:  iter.iter,
		start: &startIter.iter,
		end:   &endIter.iter,
	}
}

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// NewVersion constructs a new Version with the provided files. It requires
// the provided files are already well-ordered. It's intended for testing.
func NewVersion(
	cmp Compare, formatKey base.FormatKey, flushSplitBytes int64, files [NumLevels][]*FileMetadata,
) *Version {
	var v Version
	for l := range files {
		// NB: We specifically insert `files` into the B-Tree in the order
		// they appear within `files`. Some tests depend on this behavior in
		// order to test consistency checking, etc. Once we've constructed the
		// initial B-Tree, we swap out the btreeCmp for the correct one.
		v.Levels[l].tree, _ = makeBTree(btreeCmpSpecificOrder(files[l]), files[l])

		if l == 0 {
			v.Levels[l].tree.cmp = btreeCmpSeqNum
		} else {
			v.Levels[l].tree.cmp = btreeCmpSmallestKey(cmp)
		}
	}
	if err := v.InitL0Sublevels(cmp, formatKey, flushSplitBytes); err != nil {
		panic(err)
	}
	return &v
}

// Version is a collection of file metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type Version struct {
	refs int32

	// The level 0 sstables are organized in a series of sublevels. Similar to
	// the seqnum invariant in normal levels, there is no internal key in a
	// higher level table that has both the same user key and a higher sequence
	// number. Within a sublevel, tables are sorted by their internal key range
	// and any two tables at the same sublevel do not overlap. Unlike the normal
	// levels, sublevel n contains older tables (lower sequence numbers) than
	// sublevel n+1.
	//
	// L0Sublevels.Levels contains L0 files ordered by sublevels. All the files
	// in Files[0] are in L0Sublevels.Levels.
	L0Sublevels *L0Sublevels

	Levels [NumLevels]LevelMetadata

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	Deleted func(obsolete []*FileMetadata)

	// The list the version is linked into.
	list *VersionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *Version
}

func (v *Version) String() string {
	return v.Pretty(base.DefaultFormatter)
}

// Pretty returns a string representation of the version.
func (v *Version) Pretty(format base.FormatKey) string {
	var buf bytes.Buffer
	if v.L0Sublevels != nil {
		for sublevel := len(v.L0Sublevels.Levels) - 1; sublevel >= 0; sublevel-- {
			fmt.Fprintf(&buf, "0.%d:\n", sublevel)
			v.L0Sublevels.Levels[sublevel].Each(func(f *FileMetadata) {
				fmt.Fprintf(&buf, "  %06d:[%s-%s]\n", f.FileNum,
					format(f.Smallest.UserKey), format(f.Largest.UserKey))
			})
		}
	}
	for level := 1; level < NumLevels; level++ {
		if v.Levels[level].Empty() {
			continue
		}
		fmt.Fprintf(&buf, "%d:\n", level)
		iter := v.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(&buf, "  %s:[%s-%s]\n", f.FileNum,
				format(f.Smallest.UserKey), format(f.Largest.UserKey))
		}
	}
	return buf.String()
}

// DebugString returns an alternative format to String() which includes
// sequence number and kind information for the sstable boundaries.
func (v *Version) DebugString(format base.FormatKey) string {
	var buf bytes.Buffer

	if v.L0Sublevels != nil {
		for sublevel := len(v.L0Sublevels.Levels) - 1; sublevel >= 0; sublevel-- {
			fmt.Fprintf(&buf, "0.%d:\n", sublevel)
			v.L0Sublevels.Levels[sublevel].Each(func(f *FileMetadata) {
				fmt.Fprintf(&buf, "  %06d:[%s-%s]\n", f.FileNum,
					f.Smallest.Pretty(format), f.Largest.Pretty(format))
			})
		}
	}
	for level := 1; level < NumLevels; level++ {
		if v.Levels[level].Empty() {
			continue
		}
		fmt.Fprintf(&buf, "%d:\n", level)
		iter := v.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(&buf, "  %s:[%s-%s]\n", f.FileNum,
				f.Smallest.Pretty(format), f.Largest.Pretty(format))
		}
	}
	return buf.String()
}

// Refs returns the number of references to the version.
func (v *Version) Refs() int32 {
	return atomic.LoadInt32(&v.refs)
}

// Ref increments the version refcount.
func (v *Version) Ref() {
	atomic.AddInt32(&v.refs, 1)
}

// Unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *Version) Unref() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		obsolete := v.unrefFiles()
		l := v.list
		l.mu.Lock()
		l.Remove(v)
		v.Deleted(obsolete)
		l.mu.Unlock()
	}
}

// UnrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *Version) UnrefLocked() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		v.list.Remove(v)
		v.Deleted(v.unrefFiles())
	}
}

func (v *Version) unrefFiles() []*FileMetadata {
	var obsolete []*FileMetadata
	for _, lm := range v.Levels {
		obsolete = append(obsolete, lm.release()...)
	}
	return obsolete
}

// Next returns the next version in the list of versions.
func (v *Version) Next() *Version {
	return v.next
}

// InitL0Sublevels initializes the L0Sublevels
func (v *Version) InitL0Sublevels(
	cmp Compare, formatKey base.FormatKey, flushSplitBytes int64,
) error {
	var err error
	v.L0Sublevels, err = NewL0Sublevels(&v.Levels[0], cmp, formatKey, flushSplitBytes)
	return err
}

// Contains returns a boolean indicating whether the provided file exists in
// the version at the given level. If level is non-zero then Contains binary
// searches among the files. If level is zero, Contains scans the entire
// level.
func (v *Version) Contains(level int, cmp Compare, m *FileMetadata) bool {
	iter := v.Levels[level].Iter()
	if level > 0 {
		overlaps := v.Overlaps(level, cmp, m.Smallest.UserKey, m.Largest.UserKey)
		iter = overlaps.Iter()
	}
	for f := iter.First(); f != nil; f = iter.Next() {
		if f == m {
			return true
		}
	}
	return false
}

// Overlaps returns all elements of v.files[level] whose user key range
// intersects the inclusive range [start, end]. If level is non-zero then the
// user key ranges of v.files[level] are assumed to not overlap (although they
// may touch). If level is zero then that assumption cannot be made, and the
// [start, end] range is expanded to the union of those matching ranges so far
// and the computation is repeated until [start, end] stabilizes.
// The returned files are a subsequence of the input files, i.e., the ordering
// is not changed.
func (v *Version) Overlaps(level int, cmp Compare, start, end []byte) LevelSlice {
	if level == 0 {
		// Indices that have been selected as overlapping.
		l0 := v.Levels[level]
		l0Iter := l0.Iter()
		selectedIndices := make([]bool, l0.Len())
		numSelected := 0
		var slice LevelSlice
		for {
			restart := false
			for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
				selected := selectedIndices[i]
				if selected {
					continue
				}
				smallest := meta.Smallest.UserKey
				largest := meta.Largest.UserKey
				if cmp(largest, start) < 0 {
					// meta is completely before the specified range; skip it.
					continue
				}
				if cmp(smallest, end) > 0 {
					// meta is completely after the specified range; skip it.
					continue
				}
				// Overlaps.
				selectedIndices[i] = true
				numSelected++

				// Since level == 0, check if the newly added fileMetadata has
				// expanded the range. We expand the range immediately for files
				// we have remaining to check in this loop. All already checked
				// and unselected files will need to be rechecked via the
				// restart below.
				if cmp(smallest, start) < 0 {
					start = smallest
					restart = true
				}
				if cmp(largest, end) > 0 {
					end = largest
					restart = true
				}
			}

			if !restart {
				// Construct a B-Tree containing only the matching items.
				var tr btree
				tr.cmp = v.Levels[level].tree.cmp
				for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
					if selectedIndices[i] {
						err := tr.insert(meta)
						if err != nil {
							panic(err)
						}
					}
				}
				slice = LevelSlice{iter: tr.iter(), length: tr.length}
				// TODO(jackson): Avoid the oddity of constructing and
				// immediately releasing a B-Tree. Make LevelSlice an
				// interface?
				tr.release()
				break
			}
			// Continue looping to retry the files that were not selected.
		}
		return slice
	}

	return overlaps(v.Levels[level].Iter(), cmp, start, end)
}

// CheckOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *Version) CheckOrdering(cmp Compare, format base.FormatKey) error {
	for sublevel := len(v.L0Sublevels.Levels) - 1; sublevel >= 0; sublevel-- {
		sublevelIter := v.L0Sublevels.Levels[sublevel].Iter()
		if err := CheckOrdering(cmp, format, L0Sublevel(sublevel), sublevelIter); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString(format))
		}
	}

	for level, lm := range v.Levels {
		if err := CheckOrdering(cmp, format, Level(level), lm.Iter()); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString(format))
		}
	}
	return nil
}

// CheckConsistency checks that all of the files listed in the version exist
// and their on-disk sizes match the sizes listed in the version.
func (v *Version) CheckConsistency(dirname string, fs vfs.FS) error {
	var buf bytes.Buffer
	var args []interface{}

	for level, files := range v.Levels {
		iter := files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			path := base.MakeFilename(fs, dirname, base.FileTypeTable, f.FileNum)
			info, err := fs.Stat(path)
			if err != nil {
				buf.WriteString("L%d: %s: %v\n")
				args = append(args, errors.Safe(level), errors.Safe(f.FileNum), err)
				continue
			}
			if info.Size() != int64(f.Size) {
				buf.WriteString("L%d: %s: file size mismatch (%s): %d (disk) != %d (MANIFEST)\n")
				args = append(args, errors.Safe(level), errors.Safe(f.FileNum), path,
					errors.Safe(info.Size()), errors.Safe(f.Size))
				continue
			}
		}
	}

	if buf.Len() == 0 {
		return nil
	}
	return errors.Errorf(buf.String(), args...)
}

// VersionList holds a list of versions. The versions are ordered from oldest
// to newest.
type VersionList struct {
	mu   *sync.Mutex
	root Version
}

// Init initializes the version list.
func (l *VersionList) Init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// Empty returns true if the list is empty, and false otherwise.
func (l *VersionList) Empty() bool {
	return l.root.next == &l.root
}

// Front returns the oldest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Front() *Version {
	return l.root.next
}

// Back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Back() *Version {
	return l.root.prev
}

// PushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *VersionList) PushBack(v *Version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
}

// Remove removes the specified version from the list.
func (l *VersionList) Remove(v *Version) {
	if v == &l.root {
		panic("cannot remove version list root node")
	}
	if v.list != l {
		panic("version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}

// CheckOrdering checks that the files are consistent with respect to
// seqnums (for level 0 files -- see detailed comment below) and increasing and non-
// overlapping internal key ranges (for non-level 0 files).
func CheckOrdering(cmp Compare, format base.FormatKey, level Level, files LevelIterator) error {
	// The invariants to check for L0 sublevels are the same as the ones to
	// check for all other levels. However, if L0 is not organized into
	// sublevels, or if all L0 files are being passed in, we do the legacy L0
	// checks, defined in the detailed comment below.
	if level == Level(0) {
		// We have 2 kinds of files:
		// - Files with exactly one sequence number: these could be either ingested files
		//   or flushed files. We cannot tell the difference between them based on FileMetadata,
		//   so our consistency checking here uses the weaker checks assuming it is a narrow
		//   flushed file. We cannot error on ingested files having sequence numbers coincident
		//   with flushed files as the seemingly ingested file could just be a flushed file
		//   with just one key in it which is a truncated range tombstone sharing sequence numbers
		//   with other files in the same flush.
		// - Files with multiple sequence numbers: these are necessarily flushed files.
		//
		// Three cases of overlapping sequence numbers:
		// Case 1:
		// An ingested file contained in the sequence numbers of the flushed file -- it must be
		// fully contained (not coincident with either end of the flushed file) since the memtable
		// must have been at [a, b-1] (where b > a) when the ingested file was assigned sequence
		// num b, and the memtable got a subsequent update that was given sequence num b+1, before
		// being flushed.
		//
		// So a sequence [1000, 1000] [1002, 1002] [1000, 2000] is invalid since the first and
		// third file are inconsistent with each other. So comparing adjacent files is insufficient
		// for consistency checking.
		//
		// Visually we have something like
		// x------y x-----------yx-------------y (flushed files where x, y are the endpoints)
		//     y       y  y        y             (y's represent ingested files)
		// And these are ordered in increasing order of y. Note that y's must be unique.
		//
		// Case 2:
		// A flushed file that did not overlap in keys with any file in any level, but does overlap
		// in the file key intervals. This file is placed in L0 since it overlaps in the file
		// key intervals but since it has no overlapping data, it is assigned a sequence number
		// of 0 in RocksDB. We handle this case for compatibility with RocksDB.
		//
		// Case 3:
		// A sequence of flushed files that overlap in sequence numbers with one another,
		// but do not overlap in keys inside the sstables. These files correspond to
		// partitioned flushes or the results of intra-L0 compactions of partitioned
		// flushes.

		var prev *FileMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if prev == nil {
				continue
			}
			// Validate that the sorting is sane.
			if prev.LargestSeqNum == 0 && f.LargestSeqNum == prev.LargestSeqNum {
				// Multiple files satisfying case 2 mentioned above.
			} else if !prev.lessSeqNum(f) {
				return base.CorruptionErrorf("L0 files %s and %s are not properly ordered: <#%d-#%d> vs <#%d-#%d>",
					errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
					errors.Safe(prev.SmallestSeqNum), errors.Safe(prev.LargestSeqNum),
					errors.Safe(f.SmallestSeqNum), errors.Safe(f.LargestSeqNum))
			}
		}
	} else {
		var prev *FileMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if err := f.Validate(cmp, format); err != nil {
				return errors.Wrapf(err, "%s ", level)
			}
			if prev != nil {
				if prev.cmpSmallestKey(f, cmp) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s are not properly ordered: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
				if base.InternalCompare(cmp, prev.Largest, f.Smallest) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s have overlapping ranges: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
			}
		}
	}
	return nil
}
