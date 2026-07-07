package storage

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestDiskAtomicWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := NewDiskStorage(dir)
	content := []byte("hello-atomic-world")
	path := "test.txt"

	err = store.AtomicWrite(path, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("AtomicWrite failed: %v", err)
	}

	// Verify file was written
	fullPath := filepath.Join(dir, path)
	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}

	if string(data) != "hello-atomic-world" {
		t.Fatalf("expected 'hello-atomic-world', got '%s'", string(data))
	}
}

func TestDiskListPrefix(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := NewDiskStorage(dir)

	_ = store.Write("test-1.txt", bytes.NewReader([]byte("1")))
	_ = store.Write("test-2.txt", bytes.NewReader([]byte("2")))
	_ = store.Write("other.txt", bytes.NewReader([]byte("3")))

	matches, err := store.List("test-")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(matches), matches)
	}

	sort.Strings(matches)
	if matches[0] != "test-1.txt" || matches[1] != "test-2.txt" {
		t.Fatalf("unexpected matches: %v", matches)
	}
}
