package streaming

import (
	"bytes"
	"fmt"
	"strings"

	"mapreduce/internal/core"
)

// ParseMapOutput reads tab-separated key\tvalue lines from a map subprocess
// and returns them as a slice of KeyValue pairs.
func ParseMapOutput(output []byte) ([]core.KeyValue, error) {
	var results []core.KeyValue
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		parts := strings.SplitN(trimmed, "\t", 2)
		var kv core.KeyValue
		if len(parts) == 2 {
			kv = core.KeyValue{Key: parts[0], Value: parts[1]}
		} else {
			kv = core.KeyValue{Key: parts[0], Value: ""}
		}
		results = append(results, kv)
	}

	return results, nil
}

// FormatReduceInput formats grouped key-value pairs into the line format
// expected by the reduce subprocess on stdin.
func FormatReduceInput(key string, values []string) []byte {
	var buf bytes.Buffer
	for _, val := range values {
		buf.WriteString(fmt.Sprintf("%s\t%s\n", key, val))
	}
	return buf.Bytes()
}
