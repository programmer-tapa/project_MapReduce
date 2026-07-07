package coordinator

import "github.com/prometheus/client_golang/prometheus"

var (
	activeWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mapreduce_coordinator_active_workers",
		Help: "Number of active workers currently polling the coordinator.",
	})

	tasksAssigned = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mapreduce_coordinator_tasks_assigned_total",
		Help: "Total number of tasks assigned to workers.",
	}, []string{"task_type"})

	tasksCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mapreduce_coordinator_tasks_completed_total",
		Help: "Total number of successfully completed tasks.",
	}, []string{"task_type"})

	tasksFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mapreduce_coordinator_tasks_failed_total",
		Help: "Total number of tasks that failed.",
	}, []string{"task_type"})

	taskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mapreduce_coordinator_task_duration_seconds",
		Help:    "Task duration histogram in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"task_type"})

	coordinatorPhase = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mapreduce_coordinator_phase",
		Help: "Current phase of the coordinator (0=Map, 1=Reduce, 2=Completed).",
	})

	speculativeExecutions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mapreduce_coordinator_speculative_executions_total",
		Help: "Total number of speculative task re-executions due to timeout.",
	})
)

func init() {
	prometheus.MustRegister(activeWorkers)
	prometheus.MustRegister(tasksAssigned)
	prometheus.MustRegister(tasksCompleted)
	prometheus.MustRegister(tasksFailed)
	prometheus.MustRegister(taskDuration)
	prometheus.MustRegister(coordinatorPhase)
	prometheus.MustRegister(speculativeExecutions)
}
