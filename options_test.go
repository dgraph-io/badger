package badger

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestOptions(t *testing.T) {
	options := DefaultOptions("")

	spew.Dump(options)
}
