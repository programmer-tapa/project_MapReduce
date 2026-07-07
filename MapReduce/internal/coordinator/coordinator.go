package coordinator

import (
	"sync"

	"mapreduce/internal/core"
)

// Coordinator holds all server-side state for a MapReduce job.
// It implements the core.Scheduler interface.
//
// Thread safety: all exported methods acquire c.mu before accessing state.
type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []core.MapTaskInfo
	reduceTasks []core.ReduceTaskInfo
	phase       core.JobPhase
	nReduce     int
	nMaps       int
}

// New creates a Coordinator from a list of input files and the reduce partition count.
func New(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:       core.MapPhase,
		nReduce:     nReduce,
		nMaps:       len(files),
		mapTasks:    make([]core.MapTaskInfo, len(files)),
		reduceTasks: make([]core.ReduceTaskInfo, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = core.MapTaskInfo{
			Filename: file,
			State:    core.Unassigned,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = core.ReduceTaskInfo{
			State: core.Unassigned,
		}
	}

	return c
}
