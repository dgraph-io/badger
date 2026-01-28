package main

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

const (
	// TODO(pawan) - Make this 2 bytes long. Right now ParsedKey has ByteType and
	// bytePrefix. Change it so that it just has one field which has all the information.

	// ByteData indicates the key stores data.
	ByteData = byte(0x00)
	// ByteIndex indicates the key stores an index.
	ByteIndex = byte(0x02)
	// ByteReverse indicates the key stores a reverse index.
	ByteReverse = byte(0x04)
	// ByteCount indicates the key stores a count index.
	ByteCount = byte(0x08)
	// ByteCountRev indicates the key stores a reverse count index.
	ByteCountRev = ByteCount | ByteReverse
	// DefaultPrefix is the prefix used for data, index and reverse keys so that relative
	// order of data doesn't change keys of same attributes are located together.
	DefaultPrefix = byte(0x00)
	ByteSchema    = byte(0x01)
	ByteType      = byte(0x02)
	// ByteSplit signals that the key stores an individual part of a multi-part list.
	ByteSplit = byte(0x04)
	// ByteUnused is a constant to specify keys which need to be discarded.
	ByteUnused = byte(0xff)
	// RootNamespace is the default namespace name.
	RootNamespace = uint64(0)
	// GalaxyNamespace is the default namespace name.
	GalaxyNamespace = uint64(0)
	// IgnoreBytes is the byte range which will be ignored while prefix match in subscription.
	IgnoreBytes = "1-8"
	// NamespaceOffset is the offset in badger key from which the next 8 bytes contain namespace.
	NamespaceOffset = 1
)

// ParsedKey represents a key that has been parsed into its multiple attributes.
type ParsedKey struct {
	ByteType    string
	Attr        string
	Uid         uint64
	HasStartUid bool
	StartUid    uint64
	Term        string
	Count       uint32
	BytePrefix  byte
}

// ByteTypeToString converts a byte type value to its string representation.
func ByteTypeToString(b byte) string {
	switch b {
	case ByteData:
		return "data"
	case ByteIndex:
		return "index"
	case ByteReverse:
		return "reverse"
	case ByteCount:
		return "count"
	case ByteCountRev:
		return "count_reverse"
	default:
		return "unknown"
	}
}

// Parse would parse the key. ParsedKey does not reuse the key slice, so the key slice can change
// without affecting the contents of ParsedKey.
func Parse(key []byte) (ParsedKey, error) {
	var p ParsedKey

	if len(key) < 9 {
		return p, errors.New("Key length less than 9")
	}
	p.BytePrefix = key[0]
	namespace := key[1:9]
	key = key[9:]
	if p.BytePrefix == ByteUnused {
		return p, nil
	}

	p.HasStartUid = p.BytePrefix == ByteSplit

	if len(key) < 3 {
		return p, errors.Errorf("Invalid format for key %v", key)
	}
	sz := int(binary.BigEndian.Uint16(key[:2]))
	k := key[2:]

	if len(k) < sz {
		return p, errors.Errorf("Invalid size %v for key %v", sz, key)
	}
	p.Attr = string(namespace) + string(k[:sz])
	k = k[sz:]

	switch p.BytePrefix {
	case ByteSchema, ByteType:
		return p, nil
	default:
	}

	byteTypeValue := k[0]
	p.ByteType = ByteTypeToString(byteTypeValue)
	k = k[1:]

	switch byteTypeValue {
	case ByteData, ByteReverse:
		if len(k) < 8 {
			return p, errors.Errorf("uid length < 8 for key: %q, parsed key: %+v", key, p)
		}
		p.Uid = binary.BigEndian.Uint64(k)
		if p.Uid == 0 {
			return p, errors.Errorf("Invalid UID with value 0 for key: %v", key)
		}
		if !p.HasStartUid {
			break
		}

		if len(k) != 16 {
			return p, errors.Errorf("StartUid length != 8 for key: %q, parsed key: %+v", key, p)
		}

		k = k[8:]
		p.StartUid = binary.BigEndian.Uint64(k)
	case ByteIndex:
		if !p.HasStartUid {
			p.Term = string(k)
			break
		}

		if len(k) < 8 {
			return p, errors.Errorf("StartUid length < 8 for key: %q, parsed key: %+v", key, p)
		}

		term := k[:len(k)-8]
		startUid := k[len(k)-8:]
		p.Term = string(term)
		p.StartUid = binary.BigEndian.Uint64(startUid)
	case ByteCount, ByteCountRev:
		if len(k) < 4 {
			return p, errors.Errorf("count length < 4 for key: %q, parsed key: %+v", key, p)
		}
		p.Count = binary.BigEndian.Uint32(k)

		if !p.HasStartUid {
			break
		}

		if len(k) != 12 {
			return p, errors.Errorf("StartUid length != 8 for key: %q, parsed key: %+v", key, p)
		}

		k = k[4:]
		p.StartUid = binary.BigEndian.Uint64(k)
	default:
		// Some other data type.
		return p, errors.Errorf("Invalid data type")
	}
	return p, nil
}
