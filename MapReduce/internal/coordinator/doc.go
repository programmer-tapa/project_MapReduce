// Package coordinator implements the MapReduce coordinator (master).
//
// The coordinator manages the complete lifecycle of a MapReduce job:
//   - Initializes map tasks from input file splits
//   - Assigns tasks to workers on request (via the core.Scheduler interface)
//   - Detects stalled workers via timeout and re-issues their tasks
//   - Transitions through MapPhase → ReducePhase → CompletedPhase
//
// All coordinator state is guarded by a single sync.Mutex.
// The coordinator is single-process; fault tolerance for the coordinator itself
// is out of scope (documented as a known SPOF in architecture.md).
package coordinator
