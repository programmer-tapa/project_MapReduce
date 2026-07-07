# Protocol Buffers (pb)

This directory contains the auto-generated Go files for the MapReduce service transport layer. These files are generated from the Protobuf definition file located at `proto/mapreduce.proto` in the project root.

> [!WARNING]
> Do **not** edit any of the files in this directory manually (except for `doc.go` or this `README.md`). All changes to the protocol or message formats should be made in `proto/mapreduce.proto`, followed by regeneration of the Go code.

## 🛠️ Code Generation

To regenerate the Go code from the `.proto` file, run the following command from the root of the `MapReduce` repository:

```bash
make proto
```

Alternatively, you can run the `protoc` command directly:

```bash
protoc \
  --go_out=. --go_opt=module=mapreduce \
  --go-grpc_out=. --go-grpc_opt=module=mapreduce \
  proto/mapreduce.proto
```

### Prerequisites
Make sure you have the following installed:
1. **Protocol Buffers Compiler (`protoc`)**
2. **Go plugins for gRPC and Protocol Buffers**:
   - `protoc-gen-go`
   - `protoc-gen-go-grpc`

---

## 🗂️ Files in this Directory

*   **`mapreduce.pb.go`**: Contains the serialization and message definition code.
*   **`mapreduce_grpc.pb.go`**: Contains the client and server interfaces and stubs for the MapReduce service.
*   **`doc.go`**: Package documentation and quick-reference commands.
*   **`README.md`**: This documentation file.

---

## 🛰️ Service Overview

The `MapReduceService` defines the RPC contract between the MapReduce workers and the coordinator.

### Methods

#### 1. `GetTask`
*   **Request**: `GetTaskRequest` (contains `worker_id`)
*   **Response**: `GetTaskResponse` (contains `task_type`, `task_id`, `filename`, `n_reduce`, `n_maps`)
*   **Description**: Called by a worker to request the next available task (Map, Reduce, Wait, or Exit).

#### 2. `ReportTask`
*   **Request**: `ReportTaskRequest` (contains `worker_id`, `task_type`, `task_id`, `success`, `error_msg`)
*   **Response**: `ReportTaskResponse` (empty)
*   **Description**: Called by a worker to notify the coordinator of task completion or failure.

#### 3. `Heartbeat`
*   **Request**: `HeartbeatRequest` (contains `worker_id`)
*   **Response**: `HeartbeatResponse` (contains `acknowledged`)
*   **Description**: Optional keep-alive signal sent by workers to indicate they are active.

### Task Types (`TaskType`)

*   `MAP_TASK` (0)
*   `REDUCE_TASK` (1)
*   `WAIT_TASK` (2)
*   `EXIT_TASK` (3)
