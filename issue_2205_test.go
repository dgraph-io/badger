package badger

import (
	"testing"
)

func TestIssue2205_MemoryLeak(t *testing.T) {
	// Loop 1000 times to force the leak to accumulate
	for i := 0; i < 1000; i++ {
		// Initialize an in-memory database configuration
		opt := DefaultOptions("").WithInMemory(true)
		opt.Logger = nil // Turn off logging so our terminal doesn't get spammed

		db, err := Open(opt)
		if err != nil {
			t.Fatalf("Failed to open DB on iteration %d: %v", i, err)
		}

		// Perform a single write transaction
		err = db.Update(func(txn *Txn) error {
			return txn.Set([]byte("leak-test"), []byte("value"))
		})
		if err != nil {
			t.Fatalf("Failed to write on iteration %d: %v", i, err)
		}

		// Close the database. This is where the bug lives!
		// It is supposed to clean up everything, but it's leaving something behind.
		err = db.Close()
		if err != nil {
			t.Fatalf("Failed to close DB on iteration %d: %v", i, err)
		}
	}
}