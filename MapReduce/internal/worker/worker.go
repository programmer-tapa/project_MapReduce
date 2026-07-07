package worker

import (
	"fmt"
	"log"
	"time"

	"mapreduce/internal/core"
	"mapreduce/internal/transport"
)

// Worker manages the lifecycle of a single worker process.
// It implements the core.Executor interface.
type Worker struct {
	coordinatorAddr string
	appCommand      string
	storage         core.Storage
	workerID        string
}

// New creates a Worker that connects to the given coordinator address
// and uses the provided storage backend for file I/O.
func New(coordinatorAddr string, appCommand string, storage core.Storage) *Worker {
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	return &Worker{
		coordinatorAddr: coordinatorAddr,
		appCommand:      appCommand,
		storage:         storage,
		workerID:        workerID,
	}
}

// Run starts the main polling loop. Blocks until the job is complete
// or the coordinator becomes unreachable.
func (w *Worker) Run() error {
	client, err := transport.NewGRPCClient(w.coordinatorAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	log.Printf("[Worker %s] Started polling loop...", w.workerID)

	for {
		task, err := client.GetTask()
		if err != nil {
			log.Printf("[Worker %s] Coordinator unreachable, shutting down: %v", w.workerID, err)
			return err
		}

		switch task.Type {
		case core.MapTask:
			log.Printf("[Worker %s] Map task %d assigned: %s", w.workerID, task.TaskID, task.Filename)

			start := time.Now()
			err := w.ExecuteMap(task)
			duration := time.Since(start).Seconds()

			status := "success"
			if err != nil {
				status = "failure"
				log.Printf("[Worker %s] Map task %d failed: %v", w.workerID, task.TaskID, err)
			} else {
				log.Printf("[Worker %s] Map task %d completed successfully", w.workerID, task.TaskID)
			}

			tasksExecuted.WithLabelValues("map", status).Inc()
			taskDuration.WithLabelValues("map").Observe(duration)

			reportErr := client.ReportTask(core.TaskReport{
				Type:   core.MapTask,
				TaskID: task.TaskID,
			})
			if reportErr != nil {
				log.Printf("[Worker %s] Failed to report map task %d: %v", w.workerID, task.TaskID, reportErr)
			}

		case core.ReduceTask:
			log.Printf("[Worker %s] Reduce task %d assigned", w.workerID, task.TaskID)

			start := time.Now()
			err := w.ExecuteReduce(task)
			duration := time.Since(start).Seconds()

			status := "success"
			if err != nil {
				status = "failure"
				log.Printf("[Worker %s] Reduce task %d failed: %v", w.workerID, task.TaskID, err)
			} else {
				log.Printf("[Worker %s] Reduce task %d completed successfully", w.workerID, task.TaskID)
			}

			tasksExecuted.WithLabelValues("reduce", status).Inc()
			taskDuration.WithLabelValues("reduce").Observe(duration)

			reportErr := client.ReportTask(core.TaskReport{
				Type:   core.ReduceTask,
				TaskID: task.TaskID,
			})
			if reportErr != nil {
				log.Printf("[Worker %s] Failed to report reduce task %d: %v", w.workerID, task.TaskID, reportErr)
			}

		case core.WaitTask:
			time.Sleep(500 * time.Millisecond)

		case core.ExitTask:
			log.Printf("[Worker %s] Exit signal received, shutting down gracefully", w.workerID)
			return nil
		}
	}
}
