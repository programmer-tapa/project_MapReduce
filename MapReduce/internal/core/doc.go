// Package core defines the foundational domain types and interfaces for the
// MapReduce framework. This is the innermost layer of the architecture —
// it has ZERO external dependencies and ZERO knowledge of transport, storage,
// or infrastructure concerns.
//
// All other packages depend on core; core depends on nothing.
//
// Key types:
//   - TaskType, TaskState, JobPhase — enums governing the task lifecycle
//   - MapTask, ReduceTask — value objects describing unit of work
//   - KeyValue — the fundamental data unit flowing through the pipeline
//
// Key interfaces:
//   - Scheduler — task assignment and lifecycle management (implemented by coordinator)
//   - Executor — task execution contract (implemented by worker)
//   - Storage — file read/write abstraction (implemented by disk, S3, etc.)
package core
