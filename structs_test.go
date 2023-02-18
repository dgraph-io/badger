package badger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression test for github.com/dgraph-io/badger/pull/1800
func TestLargeEncode(t *testing.T) {

	var headerEnc [maxHeaderSize]byte
	h := header{math.MaxUint16, math.MaxUint32, math.MaxUint64, math.MaxUint8, math.MaxUint8}
	require.NotPanics(t, func() { _ = h.Encode(headerEnc[:]) })

}
