package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"

	"github.com/dgraph-io/badger/y"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

func (p valuePointer) Less(o valuePointer) bool {
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p valuePointer) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

const vptrSize = 12

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode(b []byte) []byte {
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint32(b[8:12], p.Offset)
	return b[:vptrSize]
}

// Decode decodes the value pointer into the provided byte buffer.
func (p *valuePointer) Decode(b []byte) {
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint32(b[8:12])
}

// header is used in value log as a header before Entry.
type header struct {
	klen      uint32
	vlen      uint32
	expiresAt uint64
	meta      byte
	userMeta  byte
}

const (
	// Maximum possible size of the header. The maximum size of header struct will be 18 but the
	// maximum size of varint encoded header will be 21.
	maxHeaderSize = 21
)

// Encode encodes the header into []byte. The provided []byte should be atleast 5 bytes. The
// function will panic if out []byte isn't large enough to hold all the values.
// The encoded header looks like
// +------------+--------------+-----------+------+----------+
// | Key Length | Value Length | ExpiresAt | Meta | UserMeta |
// +------------+--------------+-----------+------+----------+
func (h header) Encode(out []byte) int {
	index := 0
	index += binary.PutUvarint(out[index:], uint64(h.klen))
	index += binary.PutUvarint(out[index:], uint64(h.vlen))
	index += binary.PutUvarint(out[index:], h.expiresAt)
	out[index] = h.meta
	index++
	out[index] = h.userMeta
	return index + 1
}

// Decode decodes the given header from the provided byte slice.
func (h *header) Decode(buf []byte) {
	klen, count := binary.Uvarint(buf)
	h.klen = uint32(klen)
	buf = buf[count:]
	vlen, count := binary.Uvarint(buf)
	h.vlen = uint32(vlen)
	buf = buf[count:]
	expiresAt, count := binary.Uvarint(buf)
	h.expiresAt = uint64(expiresAt)
	buf = buf[count:]
	h.meta = buf[0]
	h.userMeta = buf[1]
}

// Entry provides Key, Value, UserMeta and ExpiresAt. This struct can be used by
// the user to set data.
type Entry struct {
	Key       []byte
	Value     []byte
	UserMeta  byte
	ExpiresAt uint64 // time.Unix
	meta      byte

	// Fields maintained internally.
	offset   uint32
	skipVlog bool
}

func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
}

// Encodes e to buf. Returns number of bytes written.
// The encoded entry looks like
// +---------------+--------+-----+-------+----------+
// | Header Length | Header | Key | Value | Checksum |
// +---------------+--------+-----+-------+----------+
func encodeEntry(e *Entry, buf *bytes.Buffer) (int, error) {
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	headerEnc := make([]byte, maxHeaderSize)
	headerLen := h.Encode(headerEnc)
	// Ensure we don't overflow uint8.
	y.AssertTrue(headerLen < math.MaxUint8)
	// Write header length.
	buf.Write([]byte{byte(headerLen)})

	// Trim headerEnc to contain only valid bytes.
	headerEnc = headerEnc[:headerLen]
	buf.Write(headerEnc)
	hash := crc32.New(y.CastagnoliCrcTable)
	if _, err := hash.Write(headerEnc); err != nil {
		return 0, err
	}

	buf.Write(e.Key)
	if _, err := hash.Write(e.Key); err != nil {
		return 0, err
	}

	buf.Write(e.Value)
	if _, err := hash.Write(e.Value); err != nil {
		return 0, err
	}

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	buf.Write(crcBuf[:])
	// 1 byte is used to store the size of the header.
	return 1 + len(headerEnc) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d UserMeta: %d Offset: %d len(val)=%d",
		prefix, e.Key, e.meta, e.UserMeta, e.offset, len(e.Value))
}

// NewEntry creates a new entry with key and value passed in args. This newly created entry can be
// set in a transaction by calling txn.SetEntry(). All other properties of Entry can be set by
// calling WithMeta, WithDiscard, WithTTL methods on it.
// This function uses key and value reference, hence users must
// not modify key and value until the end of transaction.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// WithMeta adds meta data to Entry e. This byte is stored alongside the key
// and can be used as an aid to interpret the value or store other contextual
// bits corresponding to the key-value pair of entry.
func (e *Entry) WithMeta(meta byte) *Entry {
	e.UserMeta = meta
	return e
}

// WithDiscard adds a marker to Entry e. This means all the previous versions of the key (of the
// Entry) will be eligible for garbage collection.
// This method is only useful if you have set a higher limit for options.NumVersionsToKeep. The
// default setting is 1, in which case, this function doesn't add any more benefit. If however, you
// have a higher setting for NumVersionsToKeep (in Dgraph, we set it to infinity), you can use this
// method to indicate that all the older versions can be discarded and removed during compactions.
func (e *Entry) WithDiscard() *Entry {
	e.meta = bitDiscardEarlierVersions
	return e
}

// WithTTL adds time to live duration to Entry e. Entry stored with a TTL would automatically expire
// after the time has elapsed, and will be eligible for garbage collection.
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// withMergeBit sets merge bit in entry's metadata. This
// function is called by MergeOperator's Add method.
func (e *Entry) withMergeBit() *Entry {
	e.meta = bitMergeEntry
	return e
}
