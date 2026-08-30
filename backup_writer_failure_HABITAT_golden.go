// backup_writer_failure_HABITAT_golden.go
package badger

import (
	"bytes"
	"errors"
	"testing"
)

// failingWriter is a writer that always returns an error.
type failingWriter struct{}

func (f *failingWriter) Write(p []byte) (int, error) {
	return 0, errors.New("writer failure")
}

func TestBackupWriterFailure_HABITAT(t *testing.T) {
	// Use a temporary in-memory Badger DB for testing
	opts := DefaultOptions("").WithInMemory(true)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open Badger DB: %v", err)
	}
	defer db.Close()

	// Put a sample key-value pair
	if err := db.Update(func(txn *Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	}); err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Use failingWriter to simulate backup writer failure
	fw := &failingWriter{}

	// db.Backup returns two values: ts and err
	_, err = db.Backup(fw, 0)
	if err == nil {
		t.Fatalf("Expected Backup to fail due to writer error, but got nil")
	}

	// Also test a successful backup to ensure DB.Backup works
	var buf bytes.Buffer
	_, err = db.Backup(&buf, 0)
	if err != nil {
		t.Fatalf("Backup failed unexpectedly: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("Expected backup data, got empty buffer")
	}
}
