package worker

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"mapreduce/internal/core"
	"mapreduce/internal/streaming"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ExecuteMap reads the input file, pipes content through the map subprocess,
// partitions the output into nReduce intermediate files using ihash(key) % nReduce,
// and writes them atomically via the storage interface.
func (w *Worker) ExecuteMap(task core.TaskAssignment) error {
	// 1. Read input split
	rc, err := w.storage.Read(task.Filename)
	if err != nil {
		return fmt.Errorf("failed to read input file %s: %w", task.Filename, err)
	}
	defer rc.Close()

	inputData, err := ioutil.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read input data: %w", err)
	}
	bytesRead.Add(float64(len(inputData)))

	// 2. Spawn map subprocess
	subprocessSpawns.Inc()
	runner := streaming.NewRunner(w.appCommand, "map")
	stdoutReader, err := runner.Run(bytes.NewReader(inputData))
	if err != nil {
		return fmt.Errorf("failed to run map subprocess: %w", err)
	}
	defer stdoutReader.Close()

	outputData, err := ioutil.ReadAll(stdoutReader)
	if err != nil {
		return fmt.Errorf("failed to read map output: %w", err)
	}

	// 3. Parse output key-values
	kvs, err := streaming.ParseMapOutput(outputData)
	if err != nil {
		return fmt.Errorf("failed to parse map output: %w", err)
	}

	// 4. Partition and write intermediate files
	partitionBufs := make([]bytes.Buffer, task.NReduce)
	for _, kv := range kvs {
		r := ihash(kv.Key) % int(task.NReduce)
		partitionBufs[r].WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
	}

	for r := 0; r < int(task.NReduce); r++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, r)
		data := partitionBufs[r].Bytes()
		err := w.storage.AtomicWrite(filename, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to write intermediate file %s: %w", filename, err)
		}
		bytesWritten.Add(float64(len(data)))
	}

	return nil
}

// ExecuteReduce reads all intermediate files for this reduce partition,
// sorts by key, pipes grouped key-values through the reduce subprocess,
// and writes the final output atomically.
func (w *Worker) ExecuteReduce(task core.TaskAssignment) error {
	// 1. List files in storage and identify those belonging to partition task.TaskID
	files, err := w.storage.List("mr-")
	if err != nil {
		return fmt.Errorf("failed to list intermediate files: %w", err)
	}

	var partitionFiles []string
	for _, f := range files {
		// Expect name like mr-X-Y where Y matches task.TaskID
		base := filepath.Base(f)
		parts := strings.Split(base, "-")
		if len(parts) == 3 && parts[0] == "mr" && parts[2] == strconv.Itoa(int(task.TaskID)) {
			partitionFiles = append(partitionFiles, f)
		}
	}

	// 2. Read all partition files and gather all KeyValue pairs
	var allKVs []core.KeyValue
	for _, pf := range partitionFiles {
		rc, err := w.storage.Read(pf)
		if err != nil {
			return fmt.Errorf("failed to read intermediate file %s: %w", pf, err)
		}
		data, err := ioutil.ReadAll(rc)
		rc.Close()
		if err != nil {
			return fmt.Errorf("failed to read intermediate data from %s: %w", pf, err)
		}
		bytesRead.Add(float64(len(data)))

		kvs, err := streaming.ParseMapOutput(data)
		if err != nil {
			return fmt.Errorf("failed to parse intermediate data from %s: %w", pf, err)
		}
		allKVs = append(allKVs, kvs...)
	}

	// 3. Sort key-value pairs by Key
	sort.Slice(allKVs, func(i, j int) bool {
		return allKVs[i].Key < allKVs[j].Key
	})

	// 4. Format input for reduce subprocess
	var reduceInput bytes.Buffer
	for _, kv := range allKVs {
		reduceInput.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
	}

	// 5. Spawn reduce subprocess
	subprocessSpawns.Inc()
	runner := streaming.NewRunner(w.appCommand, "reduce")
	stdoutReader, err := runner.Run(&reduceInput)
	if err != nil {
		return fmt.Errorf("failed to run reduce subprocess: %w", err)
	}
	defer stdoutReader.Close()

	outputData, err := ioutil.ReadAll(stdoutReader)
	if err != nil {
		return fmt.Errorf("failed to read reduce output: %w", err)
	}

	// 6. Write final output atomically
	outFilename := fmt.Sprintf("mr-out-%d", task.TaskID)
	err = w.storage.AtomicWrite(outFilename, bytes.NewReader(outputData))
	if err != nil {
		return fmt.Errorf("failed to write output file %s: %w", outFilename, err)
	}
	bytesWritten.Add(float64(len(outputData)))

	return nil
}
