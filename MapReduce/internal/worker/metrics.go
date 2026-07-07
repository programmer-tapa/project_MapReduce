package worker

import "github.com/prometheus/client_golang/prometheus"

var (
	tasksExecuted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mapreduce_worker_tasks_executed_total",
		Help: "Total number of tasks executed by this worker, labeled by task_type and status.",
	}, []string{"task_type", "status"})

	taskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mapreduce_worker_task_duration_seconds",
		Help:    "Histogram of task execution durations in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"task_type"})

	subprocessSpawns = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mapreduce_worker_subprocess_spawned_total",
		Help: "Total number of external Map/Reduce subprocesses spawned.",
	})

	bytesRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mapreduce_worker_bytes_read_total",
		Help: "Total number of bytes read from storage by the worker.",
	})

	bytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mapreduce_worker_bytes_written_total",
		Help: "Total number of bytes written to storage by the worker.",
	})
)

func init() {
	prometheus.MustRegister(tasksExecuted)
	prometheus.MustRegister(taskDuration)
	prometheus.MustRegister(subprocessSpawns)
	prometheus.MustRegister(bytesRead)
	prometheus.MustRegister(bytesWritten)
}
