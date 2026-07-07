package core

// TaskAssignment is the response from the scheduler when a worker requests work.
type TaskAssignment struct {
	Type     TaskType // What kind of task (Map, Reduce, Wait, Exit)
	TaskID   int      // Unique task index within its phase
	Filename string   // Input file path (Map tasks only)
	NReduce  int      // Total number of reduce partitions
	NMaps    int      // Total number of map tasks (needed by reduce workers)
}

// TaskReport is sent by a worker to the coordinator after completing a task.
type TaskReport struct {
	Type   TaskType // MapTask or ReduceTask
	TaskID int      // Which task was completed
}

// Scheduler defines the coordinator's task management contract.
// The coordinator implements this; the transport layer (gRPC server) calls it.
//
// This interface isolates scheduling logic from transport concerns,
// allowing the coordinator to be tested without standing up a network server.
type Scheduler interface {
	// AssignTask finds an available task and returns it.
	// Returns a WaitTask if tasks are pending but none are free.
	// Returns an ExitTask if the job is complete.
	AssignTask() TaskAssignment

	// CompleteTask marks a task as finished.
	// Idempotent — late completions from timed-out workers are safely ignored.
	CompleteTask(report TaskReport)

	// IsDone returns true when all phases are complete and the job can terminate.
	IsDone() bool
}

// Executor defines the worker's task execution contract.
// The worker implements this; the polling loop calls it.
type Executor interface {
	// ExecuteMap runs a map task: reads the input file, applies the map function,
	// partitions output into nReduce intermediate files.
	ExecuteMap(task TaskAssignment) error

	// ExecuteReduce runs a reduce task: reads all intermediate files for this
	// reduce partition, sorts by key, applies the reduce function, writes output.
	ExecuteReduce(task TaskAssignment) error
}
