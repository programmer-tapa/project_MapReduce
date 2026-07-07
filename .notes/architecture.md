# MapReduce: Architectural Plan

This document details the production-grade, containerized, and fault-tolerant **distributed MapReduce system** in Go. 

---

## 1. Overview
The framework executes distributed batch processing jobs by partitioning them into a centralized gRPC scheduler (**Coordinator**) and stateless workers (**Workers**). It replaces the typical academic constraints (like Unix domain sockets and fragile OS-level Go `.so` plugins) with:
- **gRPC over TCP** for inter-service communication.
- **Language-Agnostic Hadoop-style Streaming** via stdin/stdout pipe execution in external subprocesses (Python, Node.js, etc.).
- **Pluggable Storage Layer** supporting both local filesystem and AWS S3/MinIO compatible object stores.
- **Prometheus & Grafana** observability stack mapping metrics scraper targets.

---

## 2. System Architecture

```
                          ┌─────────────────────────────────┐
                          │         Coordinator             │
                          │                                 │
  input files ────────►   │  mapTasks[]   reduceTasks[]     │
                          │  phase: Map → Reduce → Done     │
                          │                                 │
                          │  gRPC API (Port 9090)           │
                          │  Prometheus Metrics (Port 9091) │
                          └────────────┬────────────────────┘
                                       │    gRPC over TCP
                     ┌─────────────────┼─────────────────┐
                     ▼                 ▼                 ▼
              ┌────────────┐   ┌────────────┐   ┌────────────┐
              │  Worker 0  │   │  Worker 1  │   │  Worker N  │
              │            │   │            │   │            │
              │ Run Loop   │   │ Run Loop   │   │ Run Loop   │
              │            │   │            │   │            │
              │ Streaming  │   │ Streaming  │   │ Streaming  │
              │ Subprocess │   │ Subprocess │   │ Subprocess │
              └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
                    │                │                  │
                    └────────────────┴──────────────────┘
                                     │
                        S3 / Shared Storage / Local Disk
                           mr-X-Y  (intermediate splits)
                           mr-out-Y  (final output)
```

---

## 3. Project File Structure

The project code is organized under the following directory layout:

| File/Folder | Description |
| :--- | :--- |
| `MapReduce/cmd/coordinator/main.go` | Main entrypoint for launching the Coordinator process. |
| `MapReduce/cmd/worker/main.go` | Main entrypoint for launching Worker instances. |
| `MapReduce/internal/core/` | Core abstractions, types (`types.go`), interface boundaries (`interfaces.go`), and storage definitions (`storage.go`). |
| `MapReduce/internal/coordinator/` | Coordinator state scheduler, timeout management, and Prometheus metrics registry. |
| `MapReduce/internal/worker/` | Worker polling control flow and MapReduce task invocation logic. |
| `MapReduce/internal/streaming/` | Hadoop-style external process runner (`runner.go`) and TSV parsing logic (`parser.go`). |
| `MapReduce/internal/storage/` | Disk and S3/MinIO compatible storage implementations. |
| `MapReduce/internal/transport/` | gRPC client-server transport wrappers. |
| `MapReduce/proto/` | Protobuf service definition schema (`mapreduce.proto`). |

---

## 4. Core Abstractions & Data Structures

### 4.1 Tasks & Job Phases (`MapReduce/internal/core/types.go`)

```go
type TaskType int
const (
    MapTask    TaskType = iota
    ReduceTask
    WaitTask                  // Scheduler is waiting for current phase tasks to finish
    ExitTask                  // Job complete, worker should exit
)

type TaskState int
const (
    Unassigned TaskState = iota
    InProgress
    Completed
)

type JobPhase int
const (
    MapPhase       JobPhase = iota
    ReducePhase
    CompletedPhase
)

type MapTaskInfo struct {
    Filename   string
    State      TaskState
    AssignedAt time.Time // Used for 10-second timeout tracking
}

type ReduceTaskInfo struct {
    State      TaskState
    AssignedAt time.Time
}
```

### 4.2 Scheduler Interface (`MapReduce/internal/core/interfaces.go`)

The Scheduler interface decouples the core coordinator state engine from gRPC transport layers to ensure isolated, testable components:

```go
type Scheduler interface {
    AssignTask() TaskAssignment
    CompleteTask(report TaskReport)
    IsDone() bool
}
```

### 4.3 Storage Interface (`MapReduce/internal/core/storage.go`)

Allows the engine to dynamically read and write files without knowing if it's operating on a local filesystem or cloud object storage:

```go
type Storage interface {
    Read(path string) (io.ReadCloser, error)
    Write(path string, r io.Reader) error
    AtomicWrite(path string, r io.Reader) error
    List(prefix string) ([]string, error)
    Remove(path string) error
}
```

---

## 5. Control Flow

### 5.1 Coordinator State Machine
1. **Initialization:** Coordinator parses input splits, initializes `mapTasks` (size = number of input files) and `reduceTasks` (size = `nReduce`), sets `phase = MapPhase`, and registers gRPC & metrics servers.
2. **Task Assignment (`AssignTask`):**
   - **`MapPhase`:** Coordinator scans `mapTasks`. If it finds an `Unassigned` task, or a task `InProgress` that has timed out (> 10s), it returns it as a `MapTask` and updates `AssignedAt = time.Now()`. If all are in-progress but not yet completed, it returns `WaitTask`. If all map tasks are finished, it transitions to `ReducePhase`.
   - **`ReducePhase`:** Scrapes and assigns `reduceTasks` using the same timeout/rescheduling rules. If all reduce tasks are finished, it transitions to `CompletedPhase`.
   - **`CompletedPhase`:** Returns `ExitTask` immediately.
3. **Task Completion (`CompleteTask`):**
   - Marks the task state as `Completed`.
   - **Idempotency Guard:** If the task is already `Completed` (due to a late response from a previously timed-out worker), the late report is discarded.

### 5.2 Worker Polling Loop
1. Worker continuously polls the Coordinator via gRPC `GetTask`.
2. Matches the returned task type:
   - **`MAP_TASK`:** Calls `ExecuteMap()`.
   - **`REDUCE_TASK`:** Calls `ExecuteReduce()`.
   - **`WAIT_TASK`:** Sleeps for `500ms` and polls again.
   - **`EXIT_TASK`:** Exits the execution loop and shuts down gracefully.
3. If gRPC communication fails (e.g., coordinator has shut down), the worker terminates.

---

## 6. Phase Execution Lifecycle

### 6.1 Map Task Execution
1. **Download:** Worker reads the split content from `Storage` (Local Disk or S3).
2. **Subprocess Streaming:** Worker spawns the user's Map application script (e.g. Python) as a subprocess, piping raw data through `stdin`.
3. **Parse output:** Worker reads the script's `stdout` stream, parsing tab-separated key-value pairs (`key\tvalue\n`).
4. **Partitioning:** Groups keys into `nReduce` partitions using `ihash(key) % nReduce`.
5. **Atomic Write:** Writes partitioned buffers to intermediate files named `mr-X-Y` (where `X` is the Map Task ID and `Y` is the partition index `0..nReduce-1`) utilizing `Storage.AtomicWrite` (write to temp file, flush, and rename).
6. **Reporting:** Notifies the Coordinator via `ReportTask`.

### 6.2 Reduce Task Execution
1. **Gather:** Worker queries `Storage.List("mr-")` and filters files matching the target partition `Y` (e.g., `mr-*-Y`).
2. **Merge & Sort:** Reads all filtered files, aggregates keys, and sorts them alphabetically.
3. **Subprocess Streaming:** Spawns the Reduce script as a subprocess, streaming sorted keys and values through `stdin`.
4. **Final Output:** Reads aggregated values from `stdout` and writes them atomically to `mr-out-Y` via `Storage.AtomicWrite`.
5. **Reporting:** Reports the task complete.

---

## 7. Fault Tolerance & Speculative Execution

### 7.1 Timeout Rescheduling
The coordinator tracks `AssignedAt` timestamps for active tasks. During `GetTask`, if a task's state is `InProgress` and `time.Since(AssignedAt) > 10 * time.Second`, the coordinator re-claims it and issues it to the newly requesting worker.

### 7.2 Late Task Resolving
To handle stragglers (workers that are not dead but extremely slow):
- If Worker A stalls, the coordinator re-assigns the task to Worker B after 10 seconds.
- Whichever worker finishes first and calls `ReportTask` will cause the task to transition to `Completed`.
- When the straggler eventually reports, the coordinator detects that the task's state is already `Completed` and discards it.

---

## 8. Storage Safety & Atomicity
To avoid corrupt state files from crashed or timed-out workers:
- **Local Disk:** The disk storage adapter writes to a temporary file in the target directory, calls `.Sync()` to flush OS page caches, and renames it using `os.Rename`. `os.Rename` is atomic on POSIX-compliant filesystems.
- **S3 Storage:** Objects uploaded to S3 are visible only upon successful completion of the PUT request, preventing partial reads of in-progress uploads.

---

## 9. Observability & Monitoring
Both Coordinator and Worker processes expose Prometheus metrics endpoints (`:9091` / `:9092`).
- **Coordinator Metrics:**
  - `mapreduce_coordinator_active_workers` (Gauge): Current count of active workers.
  - `mapreduce_coordinator_speculative_executions_total` (Counter): Number of times tasks have been re-assigned due to timeout.
  - `mapreduce_coordinator_tasks_total` / `mapreduce_coordinator_tasks_completed` (Counters): Job progression stats.
- **Grafana Dashboard:** Configured to query Prometheus and display cluster throughput, active runners, task duration histograms, and failures in real-time.
