package pb

import (
	"io"

	"google.golang.org/protobuf/proto"
)

func (c *Checksum) Size() int {
	return proto.Size(c)
}

func (c *Checksum) Marshal() ([]byte, error) {
	return proto.Marshal(c)
}

func (c *Checksum) MarshalTo(dst []byte) (int, error) {
	size := c.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return c.MarshalToSizedBuffer(dst[:size])
}

func (c *Checksum) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := c.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], c)
	return size, err
}

func (c *Checksum) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, c)
}

func (dk *DataKey) Size() int {
	return proto.Size(dk)
}

func (dk *DataKey) Marshal() ([]byte, error) {
	return proto.Marshal(dk)
}

func (dk *DataKey) MarshalTo(dst []byte) (int, error) {
	size := dk.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return dk.MarshalToSizedBuffer(dst[:size])
}

func (dk *DataKey) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := dk.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], dk)
	return size, err
}

func (dk *DataKey) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, dk)
}

func (kv *KV) Size() int {
	return proto.Size(kv)
}

func (kv *KV) Marshal() ([]byte, error) {
	return proto.Marshal(kv)
}

func (kv *KV) MarshalTo(dst []byte) (int, error) {
	size := kv.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return kv.MarshalToSizedBuffer(dst[:size])
}

func (kv *KV) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := kv.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], kv)
	return size, err
}

func (kv *KV) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, kv)
}

func (kvl *KVList) Size() int {
	return proto.Size(kvl)
}

func (kvl *KVList) Marshal() ([]byte, error) {
	return proto.Marshal(kvl)
}

func (kvl *KVList) MarshalTo(dst []byte) (int, error) {
	size := kvl.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return kvl.MarshalToSizedBuffer(dst[:size])
}

func (kvl *KVList) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := kvl.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], kvl)
	return size, err
}

func (kvl *KVList) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, kvl)
}

func (mc *ManifestChange) Size() int {
	return proto.Size(mc)
}

func (mc *ManifestChange) Marshal() ([]byte, error) {
	return proto.Marshal(mc)
}

func (mc *ManifestChange) MarshalTo(dst []byte) (int, error) {
	size := mc.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return mc.MarshalToSizedBuffer(dst[:size])
}

func (mc *ManifestChange) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := mc.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], mc)
	return size, err
}

func (mc *ManifestChange) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, mc)
}

func (mcs *ManifestChangeSet) Size() int {
	return proto.Size(mcs)
}

func (mcs *ManifestChangeSet) Marshal() ([]byte, error) {
	return proto.Marshal(mcs)
}

func (mcs *ManifestChangeSet) MarshalTo(dst []byte) (int, error) {
	size := mcs.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return mcs.MarshalToSizedBuffer(dst[:size])
}

func (mcs *ManifestChangeSet) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := mcs.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], mcs)
	return size, err
}

func (mcs *ManifestChangeSet) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, mcs)
}

func (m *Match) Size() int {
	return proto.Size(m)
}

func (m *Match) Marshal() ([]byte, error) {
	return proto.Marshal(m)
}

func (m *Match) MarshalTo(dst []byte) (int, error) {
	size := m.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	return m.MarshalToSizedBuffer(dst[:size])
}

func (m *Match) MarshalToSizedBuffer(dst []byte) (int, error) {
	size := m.Size()
	if len(dst) < size {
		return 0, io.ErrShortWrite
	}

	_, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], m)
	return size, err
}

func (m *Match) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, m)
}
