package core

import "time"

// KeyValue is the fundamental data unit in the MapReduce pipeline.
// Map functions emit KeyValue pairs; Reduce functions consume them grouped by Key.
type KeyValue struct {
	Key   string
	Value string
}

// TaskType identifies the category of work assigned to a worker.
type TaskType int

const (
	MapTask    TaskType = iota // Process an input split through the map function
	ReduceTask                // Aggregate intermediate values through the reduce function
	WaitTask                  // No work available; worker should back off and retry
	ExitTask                  // Job complete; worker should shut down gracefully
)

// TaskState tracks the lifecycle of an individual task within the coordinator.
type TaskState int

const (
	Unassigned TaskState = iota // Task has not been assigned to any worker
	InProgress                  // Task is currently being executed by a worker
	Completed                   // Task has been successfully completed
)

// JobPhase represents the current phase of the overall MapReduce job.
type JobPhase int

const (
	MapPhase       JobPhase = iota // All map tasks must complete before advancing
	ReducePhase                    // All reduce tasks must complete before advancing
	CompletedPhase                 // All work is done; coordinator can shut down
)

// MapTaskInfo holds the state of a single map task within the coordinator.
type MapTaskInfo struct {
	Filename   string    // Input file (one per split)
	State      TaskState // Current lifecycle state
	AssignedAt time.Time // Tracks when this task was last dispatched to a worker
}

// ReduceTaskInfo holds the state of a single reduce task within the coordinator.
type ReduceTaskInfo struct {
	State      TaskState // Current lifecycle state
	AssignedAt time.Time // Tracks when this task was last dispatched to a worker
}
