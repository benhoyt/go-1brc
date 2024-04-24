package main

import (
	"os"
	"testing"
)

func TestSplitFileEntire(t *testing.T) {
	const path = "testdata/split.txt"

	parts, err := splitFile(path, 4)
	if err != nil {
		t.Fatalf("Failed to split %s: %v", path, err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat %s: %v", path, err)
	}
	fileSize := st.Size()

	partsSize := int64(0)
	for _, part := range parts {
		partsSize += part.size
	}

	if partsSize != fileSize {
		t.Errorf("Want size %d, got %d", fileSize, partsSize)
	}
}
