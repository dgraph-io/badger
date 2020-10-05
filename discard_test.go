package badger

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscardStats(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	ds, err := initDiscardStats(opt)
	require.NoError(t, err)
	for i := uint32(0); i < 20; i++ {
		require.Equal(t, int64(i*100), ds.Update(i, int64(i*100)))
	}
	ds.iterate(func(id, val uint64) {
		require.Equal(t, id*100, val)
	})
	for i := uint32(0); i < 10; i++ {
		require.Equal(t, 0, int(ds.Update(i, -1)))
	}
	ds.iterate(func(id, val uint64) {
		if id < 10 {
			require.Zero(t, val)
			return
		}
		require.Equal(t, int(id*100), int(val))
	})
}
