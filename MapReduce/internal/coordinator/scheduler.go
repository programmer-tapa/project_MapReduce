package coordinator

import (
	"time"

	"mapreduce/internal/core"
)

// AssignTask finds the next available task for a requesting worker.
// Implements core.Scheduler.
func (c *Coordinator) AssignTask() core.TaskAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	timeout := 10 * time.Second

	switch c.phase {
	case core.MapPhase:
		allCompleted := true
		for i := 0; i < len(c.mapTasks); i++ {
			task := &c.mapTasks[i]
			if task.State == core.Completed {
				continue
			}
			allCompleted = false

			if task.State == core.Unassigned || (task.State == core.InProgress && now.Sub(task.AssignedAt) > timeout) {
				if task.State == core.InProgress {
					speculativeExecutions.Inc()
				}
				task.State = core.InProgress
				task.AssignedAt = now

				tasksAssigned.WithLabelValues("map").Inc()

				return core.TaskAssignment{
					Type:     core.MapTask,
					TaskID:   i,
					Filename: task.Filename,
					NReduce:  c.nReduce,
					NMaps:    c.nMaps,
				}
			}
		}

		if allCompleted {
			c.phase = core.ReducePhase
			coordinatorPhase.Set(float64(core.ReducePhase))
		} else {
			return core.TaskAssignment{Type: core.WaitTask}
		}

		fallthrough

	case core.ReducePhase:
		allCompleted := true
		for i := 0; i < len(c.reduceTasks); i++ {
			task := &c.reduceTasks[i]
			if task.State == core.Completed {
				continue
			}
			allCompleted = false

			if task.State == core.Unassigned || (task.State == core.InProgress && now.Sub(task.AssignedAt) > timeout) {
				if task.State == core.InProgress {
					speculativeExecutions.Inc()
				}
				task.State = core.InProgress
				task.AssignedAt = now

				tasksAssigned.WithLabelValues("reduce").Inc()

				return core.TaskAssignment{
					Type:    core.ReduceTask,
					TaskID:  i,
					NReduce: c.nReduce,
					NMaps:   c.nMaps,
				}
			}
		}

		if allCompleted {
			c.phase = core.CompletedPhase
			coordinatorPhase.Set(float64(core.CompletedPhase))
			return core.TaskAssignment{Type: core.ExitTask}
		} else {
			return core.TaskAssignment{Type: core.WaitTask}
		}

	case core.CompletedPhase:
		return core.TaskAssignment{Type: core.ExitTask}
	}

	return core.TaskAssignment{Type: core.WaitTask}
}

// CompleteTask marks a task as finished. Idempotent.
func (c *Coordinator) CompleteTask(report core.TaskReport) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch report.Type {
	case core.MapTask:
		if report.TaskID >= 0 && report.TaskID < len(c.mapTasks) {
			task := &c.mapTasks[report.TaskID]
			if task.State == core.InProgress {
				task.State = core.Completed
				tasksCompleted.WithLabelValues("map").Inc()
			}
		}
	case core.ReduceTask:
		if report.TaskID >= 0 && report.TaskID < len(c.reduceTasks) {
			task := &c.reduceTasks[report.TaskID]
			if task.State == core.InProgress {
				task.State = core.Completed
				tasksCompleted.WithLabelValues("reduce").Inc()
			}
		}
	}

	c.checkPhaseTransition()
}

func (c *Coordinator) checkPhaseTransition() {
	if c.phase == core.MapPhase {
		allCompleted := true
		for _, task := range c.mapTasks {
			if task.State != core.Completed {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			c.phase = core.ReducePhase
			coordinatorPhase.Set(float64(core.ReducePhase))
		}
	}

	if c.phase == core.ReducePhase {
		allCompleted := true
		for _, task := range c.reduceTasks {
			if task.State != core.Completed {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			c.phase = core.CompletedPhase
			coordinatorPhase.Set(float64(core.CompletedPhase))
		}
	}
}

// IsDone returns true when all phases are complete and the job can terminate.
func (c *Coordinator) IsDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == core.CompletedPhase
}
