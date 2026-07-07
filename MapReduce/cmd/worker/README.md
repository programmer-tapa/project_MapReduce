# worker

`worker` runs as a distributed task execution daemon. It continuously polls the coordinator for tasks, executes Map/Reduce functions by launching user application processes, and writes outputs to the shared storage layer.

---

## Capabilities

1. **Polled Task Execution:** Polls the coordinator's gRPC service for tasks. It executes Map or Reduce tasks, sleeps on wait signals, and terminates cleanly when instructed.
2. **Process Isolation (Subprocess Runner):** Executes user-defined Map or Reduce functions packaged as separate executables (e.g., Python scripts) in isolated child processes using standard streams (`stdin`/`stdout`).
3. **Partitioning:** For Map tasks, it hashes output keys using FNV-1a (`ihash(key) % NReduce`) and writes intermediate files atomically.
4. **Intermediate Aggregation & Sorting:** For Reduce tasks, it downloads all intermediate files for its partition ID, sorts the key-values alphabetically, passes them through the reducer process, and saves the final result.
5. **Atomic I/O Execution:** Writes all intermediate and output files using temporary staging files followed by an atomic rename to prevent other workers from reading partial or corrupted outputs.
6. **Observability:** Exposes a Prometheus HTTP endpoint (`/metrics`) exposing counters for tasks processed (successful/failed), execution durations, bytes read/written, and subprocess spawns.

---

## Command Line Flags

* `--coordinator-addr`: Address (IP:port) of the coordinator gRPC server (default is `localhost:9090`).
* `--app`: Required. Path to the user Map/Reduce application script or executable binary (e.g., `examples/wordcount.py`).
* `--storage`: Storage adapter backend, either `disk` or `s3` (default is `disk`).
* `--s3-endpoint`: The network endpoint address of the S3 service (default is `localhost:9000`).
* `--s3-bucket`: The target S3 bucket (default is `mapreduce`).
* `--s3-access-key`: Access key identifier for S3 credentials (default is `minioadmin`).
* `--s3-secret-key`: Secret key identifier for S3 credentials (default is `minioadmin`).
* `--metrics-addr`: The HTTP bind address for exposing Prometheus metrics (default is `:9092`).
