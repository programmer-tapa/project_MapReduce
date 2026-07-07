# coordinator

`coordinator` runs the centralized control server for a MapReduce job execution. It orchestrates task scheduling, tracks worker status, triggers phase transitions, and exposes Prometheus metrics.

---

## Capabilities

1. **Job Lifecycle Management:** Manages transition phases of a MapReduce job: `MapPhase` -> `ReducePhase` -> `CompletedPhase`.
2. **Task Assignment & Scheduling:** Assigns Map and Reduce tasks to active workers via a gRPC interface.
3. **Fault Tolerance & Task Rescheduling:** Monitors in-progress tasks. If a worker fails to report success within 10 seconds, the task is marked as unassigned and rescheduled.
4. **Storage Orchestration:** Supports local disk storage or S3-compatible object storage (e.g. MinIO) for input/output files. It automatically handles uploading local inputs to S3 if S3 storage is enabled.
5. **Observability:** Exposes an HTTP `/metrics` endpoint to monitor active workers, task assignments, task runtimes, and speculative execution occurrences.

---

## Command Line Flags

* `--addr`: The gRPC address where the coordinator server listens for workers (default is `:9090`).
* `--input`: A comma-separated list of local input files or a wildcard glob pattern matching files (e.g. `testdata/*.txt`).
* `--nreduce`: Number of reduce partitions/buckets to partition intermediate keys into (default is `10`).
* `--storage`: Storage adapter backend to use, either `disk` or `s3` (default is `disk`).
* `--s3-endpoint`: The network endpoint address of the S3 service (default is `localhost:9000`).
* `--s3-bucket`: The target S3 bucket where input/intermediate/output files reside (default is `mapreduce`).
* `--s3-access-key`: Access key identifier for authentication (default is `minioadmin`).
* `--s3-secret-key`: Secret key identifier for authentication (default is `minioadmin`).
* `--metrics-addr`: The HTTP bind address for exposing Prometheus metrics (default is `:9091`).
