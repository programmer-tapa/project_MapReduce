package worker

import (
	"bytes"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"

	"mapreduce/internal/core"
	"mapreduce/internal/storage"
)

func TestWorkerExecutor(t *testing.T) {
	dir, err := ioutil.TempDir("", "worker_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := storage.NewDiskStorage(dir)
	
	// Create sample input file
	inputContent := "apple banana apple orange banana apple"
	err = store.Write("input-0.txt", bytes.NewReader([]byte(inputContent)))
	if err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	// We use the wordcount script from examples/wordcount.py
	// Relative path to examples/wordcount.py from internal/worker is ../../examples/wordcount.py
	w := New("localhost:9090", "../../examples/wordcount.py", store)

	// 1. Run Map Task
	mapTask := core.TaskAssignment{
		Type:     core.MapTask,
		TaskID:   0,
		Filename: "input-0.txt",
		NReduce:  2,
		NMaps:    1,
	}

	err = w.ExecuteMap(mapTask)
	if err != nil {
		t.Fatalf("ExecuteMap failed: %v", err)
	}

	// Verify intermediate files mr-0-0 and mr-0-1 exist
	m0, err := store.Read("mr-0-0")
	if err != nil {
		t.Fatalf("failed to read mr-0-0: %v", err)
	}
	m0Bytes, _ := ioutil.ReadAll(m0)
	m0.Close()

	m1, err := store.Read("mr-0-1")
	if err != nil {
		t.Fatalf("failed to read mr-0-1: %v", err)
	}
	m1Bytes, _ := ioutil.ReadAll(m1)
	m1.Close()

	if len(m0Bytes) == 0 && len(m1Bytes) == 0 {
		t.Fatalf("intermediate files are empty")
	}

	// 2. Run Reduce Task for partition 0
	reduceTask0 := core.TaskAssignment{
		Type:    core.ReduceTask,
		TaskID:  0,
		NReduce: 2,
		NMaps:   1,
	}
	err = w.ExecuteReduce(reduceTask0)
	if err != nil {
		t.Fatalf("ExecuteReduce partition 0 failed: %v", err)
	}

	// Run Reduce Task for partition 1
	reduceTask1 := core.TaskAssignment{
		Type:    core.ReduceTask,
		TaskID:  1,
		NReduce: 2,
		NMaps:   1,
	}
	err = w.ExecuteReduce(reduceTask1)
	if err != nil {
		t.Fatalf("ExecuteReduce partition 1 failed: %v", err)
	}

	// Read output files mr-out-0 and mr-out-1
	out0, err := store.Read("mr-out-0")
	if err != nil {
		t.Fatalf("failed to read mr-out-0: %v", err)
	}
	out0Bytes, _ := ioutil.ReadAll(out0)
	out0.Close()

	out1, err := store.Read("mr-out-1")
	if err != nil {
		t.Fatalf("failed to read mr-out-1: %v", err)
	}
	out1Bytes, _ := ioutil.ReadAll(out1)
	out1.Close()

	allOutput := string(out0Bytes) + "\n" + string(out1Bytes)
	lines := strings.Split(allOutput, "\n")

	var results []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed != "" {
			results = append(results, trimmed)
		}
	}
	sort.Strings(results)

	expected := []string{
		"apple\t3",
		"banana\t2",
		"orange\t1",
	}

	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d: %v", len(expected), len(results), results)
	}

	for i := range expected {
		if results[i] != expected[i] {
			t.Errorf("expected '%s', got '%s'", expected[i], results[i])
		}
	}
}
