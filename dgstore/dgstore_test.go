// +build dgbadger

/*
Test compatibility with dgraph.

Make sure dgraph has the dgs package. Then run test:
go test -tags dgbadger .
*/
package dgstore

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/dgraph/dgs"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/badger"
)

func TestBasic(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	var kv dgs.Store

	itOpt := badger.IteratorOptions{
		PrefetchSize: 10,
		FetchValues:  true,
	}
	kv = NewStore(&badger.DefaultOptions, &itOpt)
	defer kv.Close()
	// If this compiles, then all the dgs interfaces are satisfied.
}
