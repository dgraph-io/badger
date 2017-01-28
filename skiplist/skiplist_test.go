package skiplist

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/dgraph/x"
)

func TestSeek(t *testing.T) {
	list := NewSkiplist(10, 3)
	it := list.Iterator()

	require.False(t, it.Valid())

	list.Insert("def")
	list.Insert("abc")

	it.Seek("abc")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.Seek("a")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.Seek("d")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "def")

	it.Seek("fff")
	require.False(t, it.Valid())
}

func TestSeekForPrev(t *testing.T) {
	list := NewSkiplist(10, 3)
	it := list.Iterator()

	require.False(t, it.Valid())

	list.Insert("def")
	list.Insert("abc")

	it.Seek("abc")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.SeekForPrev("a")
	require.False(t, it.Valid())

	it.SeekForPrev("d")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "abc")

	it.SeekForPrev("fff")
	require.True(t, it.Valid())
	require.EqualValues(t, it.Key(), "def")
}

func TestNext(t *testing.T) {
	list := NewSkiplist(10, 3)
	it := list.Iterator()

	list.Insert("abc")
	it.SeekToFirst()
	require.EqualValues(t, it.Key(), "abc")

	list.Insert("def")
	list.Insert("cde")
	it.Next()
	require.EqualValues(t, it.Key(), "cde")
	it.Next()
	require.EqualValues(t, it.Key(), "def")
}

func TestPrev(t *testing.T) {
	list := NewSkiplist(10, 3)
	it := list.Iterator()

	list.Insert("def")
	it.SeekToLast()
	require.EqualValues(t, it.Key(), "def")

	list.Insert("abc")
	list.Insert("cde")
	it.Prev()
	require.EqualValues(t, it.Key(), "cde")
	it.Prev()
	require.EqualValues(t, it.Key(), "abc")
}

func TestMain(m *testing.M) {
	x.SetTestRun()
	x.Init()
	os.Exit(m.Run())
}
