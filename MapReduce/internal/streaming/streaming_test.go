package streaming

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestParseMapOutput(t *testing.T) {
	input := []byte("hello\t1\nworld\t2\nno_tab_value\n")
	res, err := ParseMapOutput(input)
	if err != nil {
		t.Fatalf("ParseMapOutput failed: %v", err)
	}

	if len(res) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res))
	}

	if res[0].Key != "hello" || res[0].Value != "1" {
		t.Errorf("unexpected res[0]: %+v", res[0])
	}
	if res[1].Key != "world" || res[1].Value != "2" {
		t.Errorf("unexpected res[1]: %+v", res[1])
	}
	if res[2].Key != "no_tab_value" || res[2].Value != "" {
		t.Errorf("unexpected res[2]: %+v", res[2])
	}
}

func TestRunnerWordCount(t *testing.T) {
	// examples/wordcount.py is located at ../../examples/wordcount.py relative to internal/streaming
	runner := NewRunner("python3", "../../examples/wordcount.py", "map")

	input := bytes.NewBufferString("hello world hello")
	reader, err := runner.Run(input)
	if err != nil {
		t.Fatalf("failed to start runner: %v", err)
	}
	defer reader.Close()

	output, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read runner stdout: %v", err)
	}

	res, err := ParseMapOutput(output)
	if err != nil {
		t.Fatalf("failed to parse runner output: %v", err)
	}

	// Should emit "hello\t1", "world\t1", "hello\t1"
	if len(res) != 3 {
		t.Fatalf("expected 3 pairs, got %d: %+v", len(res), res)
	}

	if res[0].Key != "hello" || res[0].Value != "1" {
		t.Errorf("expected hello 1, got %+v", res[0])
	}
	if res[1].Key != "world" || res[1].Value != "1" {
		t.Errorf("expected world 1, got %+v", res[1])
	}
	if res[2].Key != "hello" || res[2].Value != "1" {
		t.Errorf("expected hello 1, got %+v", res[2])
	}
}
