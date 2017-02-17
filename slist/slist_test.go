package slist

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	//	"fmt"
)

func TestBasic(t *testing.T) {
	l := NewSkiplist()
	val1 := int(42)
	//	val2 := int(52)
	//	val3 := int(62)
	require.EqualValues(t, kNilValue,
		l.Put([]byte("key1"), unsafe.Pointer(&val1), false))
	//	v := l.Get([]byte("key1"))
	//	require.NotNil(t, v)

	//	vv := *(*int)(v)
	//	require.EqualValues(t, val1, vv)

	//	fmt.Printf("~~~%d\n", *(*int)(v))
}
