package badger

import (
	"bytes"
	"crypto/aes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/y"

	"github.com/dgraph-io/badger/pb"
)

// keyRegistryFileName is file name of key registry.
const keyRegistryFileName = "KEYREGISTRY"

// keyRegistryRewriteFileName is  file name of key registry.
const keyRegistryRewriteFileName = "REWRITE-KEYREGISTRY"

// SanityText is used to check whether the given user provided storage key is valid or not
var sanityText = []byte("!Badger!Registry!")

// KeyRegistry used to maintain all the data keys.
type KeyRegistry struct {
	dataKeys    map[uint64]*pb.DataKey
	lastCreated int64
	nextKeyID   uint64
	storageKey  []byte
	fp          *os.File
}

func newKeyRegistry(storageKey []byte) *KeyRegistry {
	return &KeyRegistry{
		dataKeys:   make(map[uint64]*pb.DataKey),
		nextKeyID:  0,
		storageKey: storageKey,
	}
}
func openKeyRegistry(dir string, readOnly bool, storageKey []byte) (*KeyRegistry, error) {
	path := filepath.Join(dir, keyRegistryFileName)
	var flags uint32
	if readOnly {
		flags |= y.ReadOnly
	}
	fp, err := y.OpenExistingFile(path, flags)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		kr := newKeyRegistry(storageKey)
		if readOnly {
			return kr, nil
		}
		fp.Close()
		if err := rewriteRegistry(dir, kr, storageKey); err != nil {
			return nil, err
		}
		fp, err = y.OpenExistingFile(path, flags)
		if err != nil {
			return nil, err
		}
	}
	return buildKeyRegistry(fp, storageKey)
}

func buildKeyRegistry(fp *os.File, storageKey []byte) (*KeyRegistry, error) {
	readPos := int64(0)
	iv, err := y.Read(fp, readPos, aes.BlockSize)
	if err != nil {
		return nil, err
	}
	readPos += aes.BlockSize
	eSanityText, err := y.Read(fp, readPos, len(sanityText))
	if err != nil {
		return nil, err
	}
	if len(storageKey) > 0 {
		var err error
		eSanityText, err = y.XORBlock(storageKey, iv, eSanityText, 0)
		if err != nil {
			return nil, err
		}
	}
	if bytes.Compare(eSanityText, sanityText) != 0 {
		return nil, ErrStorageKeyMismatch
	}
	readPos += int64(len(sanityText))
	stat, err := fp.Stat()
	if err != nil {
		return nil, err
	}
	kr := newKeyRegistry(storageKey)
	for {
		if readPos == stat.Size() {
			break
		}
		lenCrcBuf, err := y.Read(fp, readPos, 8)
		if err != nil {
			return nil, err
		}
		readPos += 8
		l := int64(binary.BigEndian.Uint32(lenCrcBuf[0:4]))
		data, err := y.Read(fp, readPos, int(l))
		if err != nil {
			return nil, err
		}
		if crc32.Checksum(data, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:]) {
			return nil, errBadChecksum
		}
		dataKey := &pb.DataKey{}
		err = dataKey.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		if len(storageKey) > 0 {
			var err error
			dataKey.Data, err = y.XORBlock(storageKey, dataKey.IV, dataKey.Data, 0)
			if err != nil {
				return nil, err
			}
		}
		if dataKey.KeyID > kr.nextKeyID {
			kr.nextKeyID = dataKey.KeyID
		}
		if dataKey.CreatedAt > (kr.lastCreated) {
			kr.lastCreated = dataKey.CreatedAt
		}
		kr.dataKeys[kr.nextKeyID] = dataKey
		readPos += l
	}
	fp.Seek(0, io.SeekEnd)
	kr.fp = fp
	return kr, nil
}

func rewriteRegistry(dir string, reg *KeyRegistry, storageKey []byte) error {
	reWritePath := filepath.Join(dir, keyRegistryRewriteFileName)
	fp, err := y.OpenTruncFile(reWritePath, false)
	if err != nil {
		return err
	}
	iv, err := y.GenereateIV()
	if err != nil {
		return err
	}
	eSanity := sanityText
	if len(storageKey) > 0 {
		var err error
		eSanity, err = y.XORBlock(storageKey, iv, eSanity, 0)
		if err != nil {
			return err
		}
	}
	if _, err = fp.Write(iv); err != nil {
		return err
	}
	if _, err = fp.Write(eSanity); err != nil {
		return err
	}
	defer fp.Close()
	for _, k := range reg.dataKeys {
		err := storeDataKey(fp, storageKey, k, false)
		if err != nil {
			return err
		}
	}
	if err = y.FileSync(fp); err != nil {
		return nil
	}
	registryPath := filepath.Join(dir, keyRegistryFileName)
	if err = fp.Close(); err != nil {
		return err
	}
	if err = os.Rename(reWritePath, registryPath); err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	return nil
}

func (kr *KeyRegistry) getDataKey() (*pb.DataKey, error) {
	diff := time.Now().Sub(time.Unix(kr.lastCreated, 0))
	if diff.Hours()/24 > 10 {
		kr.nextKeyID++
		k := make([]byte, len(kr.storageKey))
		iv, err := y.GenereateIV()
		if err != nil {
			return nil, err
		}
		rand.Read(k)
		dk := &pb.DataKey{}
		dk.KeyID = kr.nextKeyID
		dk.Data = k
		dk.CreatedAt = time.Now().Unix()
		dk.IV = iv
		err = storeDataKey(kr.fp, kr.storageKey, dk, true)
		if err != nil {
			return nil, err
		}
		// storeDatakey encrypts the datakey
		// So, placing unencrypted key in the memory
		dk.Data = k
		kr.dataKeys[kr.nextKeyID] = dk
		return dk, nil
	}
	return kr.dataKeys[kr.nextKeyID], nil
}

func (kr *KeyRegistry) close() {
	kr.fp.Close()
}

func storeDataKey(fp *os.File, storageKey []byte, k *pb.DataKey, sync bool) error {
	if len(storageKey) > 0 {
		var err error
		// In memory, we'll have decrypted key.
		k.Data, err = y.XORBlock(storageKey, k.IV, k.Data, 0)
		if err != nil {
			return err
		}
	}
	data, err := k.Marshal()
	if err != nil {
		return err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(data, y.CastagnoliCrcTable))
	fp.Write(lenCrcBuf[:])
	fp.Write(data)
	if sync {
		err := fp.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}
