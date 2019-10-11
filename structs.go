package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
	// we don't encode logtype because we just need it in the in-memory phase. to differentiate
	// between type of vptr.
	log logType
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

const vptrSize = unsafe.Sizeof(valuePointer{})

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode() []byte {
	b := make([]byte, vptrSize)
	// Copy over the content from p to b.
	*(*valuePointer)(unsafe.Pointer(&b[0])) = p
	return b
}

// Decode decodes the value pointer into the provided byte buffer.
func (p *valuePointer) Decode(b []byte) {
	*p = *(*valuePointer)(unsafe.Pointer(&b[0]))
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
// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
func (h header) Encode(out []byte) int {
	out[0], out[1] = h.meta, h.userMeta
	index := 2
	index += binary.PutUvarint(out[index:], uint64(h.klen))
	index += binary.PutUvarint(out[index:], uint64(h.vlen))
	index += binary.PutUvarint(out[index:], h.expiresAt)
	return index
}

// Decode decodes the given header from the provided byte slice.
// Returns the number of bytes read.
func (h *header) Decode(buf []byte) int {
	h.meta, h.userMeta = buf[0], buf[1]
	index := 2
	klen, count := binary.Uvarint(buf[index:])
	h.klen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.vlen = uint32(vlen)
	index += count
	h.expiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}

// DecodeFrom reads the header from the hashReader.
// Returns the number of bytes read.
func (h *header) DecodeFrom(reader *hashReader) (int, error) {
	var err error
	h.meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	h.userMeta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.klen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.vlen = uint32(vlen)
	h.expiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.bytesRead, nil
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
	hlen     int // Length of the header.
	forceWal bool
}

func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
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

// entryEncoder is used to encode entry to the log format.
// It takes care of encryption.
type entryEncoder struct {
	dataKey *pb.DataKey
}

func (encoder *entryEncoder) encode(e *Entry, buf *bytes.Buffer, IV []byte) (int, error) {
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	y.Check2(buf.Write(headerEnc[:sz]))
	// write hash.
	hash := crc32.New(y.CastagnoliCrcTable)
	y.Check2(hash.Write(headerEnc[:sz]))
	// we'll encrypt only key and value.
	if encoder.encryptionEnabled() {
		// TODO: no need to allocate the bytes. we can calculate the encrypted buf one by one
		// since we're using ctr mode of AES encryption. Ordering won't changed. Need some
		// refactoring in XORBlock which will work like stream cipher.
		eBuf := make([]byte, 0, len(e.Key)+len(e.Value))
		eBuf = append(eBuf, e.Key...)
		eBuf = append(eBuf, e.Value...)
		var err error
		eBuf, err = y.XORBlock(eBuf, encoder.dataKey.Data, IV)
		if err != nil {
			return 0, y.Wrapf(err, "Error while encoding entry for vlog.")
		}
		// write encrypted buf.
		y.Check2(buf.Write(eBuf))
		// write the hash.
		y.Check2(hash.Write(eBuf))
	} else {
		// Encryption is disabled so writing directly to the buffer.
		// write key.
		y.Check2(buf.Write(e.Key))
		// write key hash.
		y.Check2(hash.Write(e.Key))
		// write value.
		y.Check2(buf.Write(e.Value))
		// write value hash.
		y.Check2(hash.Write(e.Value))
	}
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	y.Check2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (encoder *entryEncoder) encryptionEnabled() bool {
	return encoder.dataKey != nil
}
