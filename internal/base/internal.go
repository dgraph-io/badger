package base // import "github.com/dgraph-io/badger/v3/internal/base"

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// InternalKeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type InternalKeyKind uint8

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete  InternalKeyKind = 0
	InternalKeyKindSet                     = 1
	InternalKeyKindMerge                   = 2
	InternalKeyKindLogData                 = 3
	// InternalKeyKindColumnFamilyDeletion                     = 4
	// InternalKeyKindColumnFamilyValue                        = 5
	// InternalKeyKindColumnFamilyMerge                        = 6
	InternalKeyKindSingleDelete = 7
	// InternalKeyKindColumnFamilySingleDelete                 = 8
	// InternalKeyKindBeginPrepareXID                          = 9
	// InternalKeyKindEndPrepareXID                            = 10
	// InternalKeyKindCommitXID                                = 11
	// InternalKeyKindRollbackXID                              = 12
	// InternalKeyKindNoop                                     = 13
	// InternalKeyKindColumnFamilyRangeDelete                  = 14
	InternalKeyKindRangeDelete = 15
	// InternalKeyKindColumnFamilyBlobIndex                    = 16
	// InternalKeyKindBlobIndex                                = 17

	// This maximum value isn't part of the file format. It's unlikely,
	// but future extensions may increase this value.
	//
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 17

	// A marker for an invalid key.
	InternalKeyKindInvalid InternalKeyKind = 255

	// InternalKeySeqNumBatch is a bit that is set on batch sequence numbers
	// which prevents those entries from being excluded from iteration.
	InternalKeySeqNumBatch = uint64(1 << 55)

	// InternalKeySeqNumMax is the largest valid sequence number.
	InternalKeySeqNumMax = uint64(1<<56 - 1)

	// InternalKeyRangeDeleteSentinel is the marker for a range delete sentinel
	// key. This sequence number and kind are used for the upper stable boundary
	// when a range deletion tombstone is the largest key in an sstable. This is
	// necessary because sstable boundaries are inclusive, while the end key of a
	// range deletion tombstone is exclusive.
	InternalKeyRangeDeleteSentinel = (InternalKeySeqNumMax << 8) | InternalKeyKindRangeDelete
)

var internalKeyKindNames = []string{
	InternalKeyKindDelete:       "DEL",
	InternalKeyKindSet:          "SET",
	InternalKeyKindMerge:        "MERGE",
	InternalKeyKindLogData:      "LOGDATA",
	InternalKeyKindSingleDelete: "SINGLEDEL",
	InternalKeyKindRangeDelete:  "RANGEDEL",
	InternalKeyKindMax:          "MAX",
	InternalKeyKindInvalid:      "INVALID",
}

func (k InternalKeyKind) String() string {
	if int(k) < len(internalKeyKindNames) {
		return internalKeyKindNames[k]
	}
	return fmt.Sprintf("UNKNOWN:%d", k)
}

// InternalKey is a key used for the in-memory and on-disk partial DBs that
//
// It consists of the user key
// followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type InternalKey struct {
	UserKey []byte
	Trailer uint64
}

// InvalidInternalKey is an invalid internal key for which Valid() will return
// false.
var InvalidInternalKey = MakeInternalKey(nil, 0, InternalKeyKindInvalid)

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (seqNum << 8) | uint64(kind),
	}
}

// MakeSearchKey constructs an internal key that is appropriate for searching
// for a the specified user key. The search key contain the maximual sequence
// number and kind ensuring that it sorts before any other internal keys for
// the same user key.
func MakeSearchKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (InternalKeySeqNumMax << 8) | uint64(InternalKeyKindMax),
	}
}

// MakeRangeDeleteSentinelKey constructs an internal key that is a range
// deletion sentinel key, used as the upper boundary for an sstable when a
// range deletion is the largest key in an sstable.
func MakeRangeDeleteSentinelKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: InternalKeyRangeDeleteSentinel,
	}
}

var kindsMap = map[string]InternalKeyKind{
	"DEL":       InternalKeyKindDelete,
	"SINGLEDEL": InternalKeyKindSingleDelete,
	"RANGEDEL":  InternalKeyKindRangeDelete,
	"SET":       InternalKeyKindSet,
	"MERGE":     InternalKeyKindMerge,
	"INVALID":   InternalKeyKindInvalid,
	"MAX":       InternalKeyKindMax,
}

// ParseInternalKey parses the string representation of an internal key. The
// format is <user-key>.<kind>.<seq-num>. If the seq-num starts with a "b" it
// is marked as a batch-seq-num (i.e. the InternalKeySeqNumBatch bit is set).
func ParseInternalKey(s string) InternalKey {
	x := strings.Split(s, ".")
	ukey := x[0]
	kind, ok := kindsMap[x[1]]
	if !ok {
		panic(fmt.Sprintf("unknown kind: %q", x[1]))
	}
	j := 0
	if x[2][0] == 'b' {
		j = 1
	}
	seqNum, _ := strconv.ParseUint(x[2][j:], 10, 64)
	if x[2][0] == 'b' {
		seqNum |= InternalKeySeqNumBatch
	}
	return MakeInternalKey([]byte(ukey), seqNum, kind)
}

// DecodeInternalKey decodes an encoded internal key. See InternalKey.Encode().
func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - 8
	var trailer uint64
	if n >= 0 {
		trailer = binary.LittleEndian.Uint64(encodedKey[n:])
		encodedKey = encodedKey[:n:n]
	} else {
		trailer = uint64(InternalKeyKindInvalid)
		encodedKey = nil
	}
	return InternalKey{
		UserKey: encodedKey,
		Trailer: trailer,
	}
}

// InternalCompare compares two internal keys using the specified comparison
// function. For equal user keys, internal keys compare in descending sequence
// number order. For equal user keys and sequence numbers, internal keys
// compare in descending kind order (though this should never happen in
// practice).
func InternalCompare(userCmp Compare, a, b InternalKey) int {
	if x := userCmp(a.UserKey, b.UserKey); x != 0 {
		return x
	}
	if a.Trailer > b.Trailer {
		return -1
	}
	if a.Trailer < b.Trailer {
		return 1
	}
	return 0
}

// Encode encodes the receiver into the buffer. The buffer must be large enough
// to hold the encoded data. See InternalKey.Size().
func (k InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], k.Trailer)
}

// EncodeTrailer returns the trailer encoded to an 8-byte array.
func (k InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.Trailer)
	return buf
}

// Separator returns a separator key such that k <= x && x < other, where less
// than is consistent with the Compare function. The buf parameter may be used
// to store the returned InternalKey.UserKey, though it is valid to pass a
// nil. See the Separator type for details on separator keys.
func (k InternalKey) Separator(
	cmp Compare, sep Separator, buf []byte, other InternalKey,
) InternalKey {
	buf = sep(buf, k.UserKey, other.UserKey)
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
		// The separator user key is physically shorter than k.UserKey (if it is
		// longer, we'll continue to use "k"), but logically after. Tack on the max
		// sequence number to the shortened user key. Note that we could tack on
		// any sequence number and kind here to create a valid separator key. We
		// use the max sequence number to match the behavior of LevelDB and
		// RocksDB.
		return MakeInternalKey(buf, InternalKeySeqNumMax, InternalKeyKindMax)
	}
	return k
}

// Successor returns a successor key such that k <= x. A simple implementation
// may return k unchanged. The buf parameter may be used to store the returned
// InternalKey.UserKey, though it is valid to pass a nil.
func (k InternalKey) Successor(cmp Compare, succ Successor, buf []byte) InternalKey {
	buf = succ(buf, k.UserKey)
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
		// The successor user key is physically shorter that k.UserKey (if it is
		// longer, we'll continue to use "k"), but logically after. Tack on the max
		// sequence number to the shortened user key. Note that we could tack on
		// any sequence number and kind here to create a valid separator key. We
		// use the max sequence number to match the behavior of LevelDB and
		// RocksDB.
		return MakeInternalKey(buf, InternalKeySeqNumMax, InternalKeyKindMax)
	}
	return k
}

// Size returns the encoded size of the key.
func (k InternalKey) Size() int {
	return len(k.UserKey) + 8
}

// SetSeqNum sets the sequence number component of the key.
func (k *InternalKey) SetSeqNum(seqNum uint64) {
	k.Trailer = (seqNum << 8) | (k.Trailer & 0xff)
}

// SeqNum returns the sequence number component of the key.
func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

// Visible returns true if the key is visible at the specified snapshot
// sequence number.
func (k InternalKey) Visible(snapshot uint64) bool {
	seqNum := k.SeqNum()
	return seqNum < snapshot || (seqNum&InternalKeySeqNumBatch) != 0
}

// SetKind sets the kind component of the key.
func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | uint64(kind)
}

// Kind returns the kind compoment of the key.
func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

// Valid returns true if the key has a valid kind.
func (k InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

// Clone clones the storage for the UserKey component of the key.
func (k InternalKey) Clone() InternalKey {
	if k.UserKey == nil {
		return k
	}
	return InternalKey{
		UserKey: append([]byte(nil), k.UserKey...),
		Trailer: k.Trailer,
	}
}

// String returns a string representation of the key.
func (k InternalKey) String() string {
	return fmt.Sprintf("%s#%d,%d", FormatBytes(k.UserKey), k.SeqNum(), k.Kind())
}

// Pretty returns a formatter for the key.
func (k InternalKey) Pretty(f FormatKey) fmt.Formatter {
	return prettyInternalKey{k, f}
}

type prettyInternalKey struct {
	InternalKey
	formatKey FormatKey
}

func (k prettyInternalKey) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, "%s#%d,%s", k.formatKey(k.UserKey), k.SeqNum(), k.Kind())
}
