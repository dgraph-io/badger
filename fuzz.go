package badger

import (
	"io/ioutil"
	"os"
)

// FuzzKeySplits implements the fuzzer
func FuzzKeySplits(data []byte) int {
	// We first do initial setup
	dir, err := ioutil.TempDir(".", "badger-test")
	defer os.RemoveAll(dir)
	if err != nil {
		return -1
	}
	opts := DefaultOptions("").WithInMemory(true).WithLoggingLevel(ERROR)
	db, err := Open(opts)
	defer db.Close()
	if err != nil {
		return -1
	}

	// Here we fuzz KeySplits
	_ = db.KeySplits(data)
	return 1
}
