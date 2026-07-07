// Package worker implements the MapReduce worker process.
//
// The worker runs an infinite polling loop:
//  1. Request a task from the coordinator via gRPC
//  2. Execute the task (Map or Reduce) by spawning user-defined programs
//  3. Write output via the storage interface
//  4. Report completion back to the coordinator
//  5. Repeat until ExitTask is received or coordinator is unreachable
//
// Task execution uses Hadoop-style streaming: the worker spawns an external
// subprocess (e.g., a Python script) and communicates via stdin/stdout pipes.
// This makes the framework language-agnostic.
package worker
