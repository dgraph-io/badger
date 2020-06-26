package y

import (
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

// BlockPool is a specialized sync.Pool for byte slices of fixed size.
//
// It exists because using a sync.Pool directly for byte slices is tricky
// because the slice header is a struct under the hood, and copied by value.
// Storing it in a pool directly requires the allocation of an interface
// value (i.e. a pointer).
//
// See https://go-review.googlesource.com/c/go/+/24371 for discussion.
type BlockPool struct {
	BlockSize int
	pool      sync.Pool
}

func (b *BlockPool) Get() []byte {
	ptr := b.pool.Get()
	if ptr == nil {
		return make([]byte, b.BlockSize)
	}

	var ret []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	slice.Data = uintptr(unsafe.Pointer(ptr.(*byte)))
	slice.Len = b.BlockSize
	slice.Cap = b.BlockSize
	// Make sure ptr is not garbage collected until we are done building the slice.
	runtime.KeepAlive(ptr)
	return ret
}

func (b *BlockPool) Put(buf []byte) {
	AssertTrue(cap(buf) == b.BlockSize)
	buf = buf[:1]
	b.pool.Put(&buf[0])
}
