package main_test

import (
	"testing"

	main "github.com/dgraph-io/badger/v4/utility/key-parser"
)

func Test_LinePattern(t *testing.T) {

	lines := []string{
		"Key: 000000000000000000000b4576656e742e76616c756500000000000002fbe0\tversion: 1\tsize: 75\tmeta: b1000",
	}

	for _, line := range lines {
		matches := main.LinePattern.FindStringSubmatch(line)
		if matches == nil {
			t.Errorf("Expected line to match pattern, but it did not: %s", line)
			continue
		}

		if len(matches) != 6 {
			t.Errorf("Expected 6 matches, got %d for line: %s", len(matches), line)
			continue
		}

		t.Logf("Line matched successfully: %s", line)
		t.Logf("  Key (hex): %s", matches[1])
		t.Logf("  Version: %s", matches[2])
		t.Logf("  Size: %s", matches[3])
		t.Logf("  Meta: %s", matches[4])
		t.Logf("  Discard: %s", matches[5])
	}
}
