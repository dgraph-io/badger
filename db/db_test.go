package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBWrite(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	wb := NewWriteBatch(10)
	for i := 0; i < 100; i++ {
		wb.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}
	require.NoError(t, db.Write(wb))

	wb2 := NewWriteBatch(10)
	for i := 0; i < 100; i++ {
		wb2.Put([]byte(fmt.Sprintf("KEY%d", i)), []byte(fmt.Sprintf("VAL%d", i)))
	}
	require.NoError(t, db.Write(wb2))
}

func TestDBGet(t *testing.T) {
	db := NewDB(DefaultDBOptions)
	require.NoError(t, db.Put([]byte("key1"), []byte("val1")))
	require.EqualValues(t, "val1", db.Get([]byte("key1")))

	require.NoError(t, db.Put([]byte("key1"), []byte("val2")))
	require.EqualValues(t, "val2", db.Get([]byte("key1")))

	require.NoError(t, db.Delete([]byte("key1")))
	require.Nil(t, db.Get([]byte("key1")))

	require.NoError(t, db.Put([]byte("key1"), []byte("val3")))
	require.EqualValues(t, "val3", db.Get([]byte("key1")))
}
