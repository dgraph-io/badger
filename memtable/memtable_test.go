package memtable

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/y"
)

func TestBasic(t *testing.T) {
	m := NewMemtable(DefaultKeyComparator)
	require.NotNil(t, m)

	m.Add(123, y.TypeValue, []byte("somekey"), []byte("hohoho"))
	m.Add(120, y.TypeValue, []byte("somekey"), []byte("hohoho"))
}
