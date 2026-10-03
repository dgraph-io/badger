package badger_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// failWriter simulates a writer that always fails
type failWriter struct{}

func (f *failWriter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("writer failure")
}

func TestBackupWriterFailure_HABITAT(t *testing.T) {
	// Use in-memory Badger DB for test
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open Badger DB: %v", err)
	}
	defer db.Close()

	// Insert a key-value pair
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key1"), []byte("value1"))
	})
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Backup with failing writer should return an error
	_, err = db.Backup(&failWriter{}, 0)
	if err == nil {
		t.Errorf("Expected Backup to fail due to writer error, but got nil")
	}

	// Backup to normal buffer should succeed
	buf := &bytes.Buffer{}
	_, err = db.Backup(buf, 0)
	if err != nil {
		t.Errorf("Expected successful backup, got error: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("Expected some data in backup, but buffer is empty")
	}
}
