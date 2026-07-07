package coordinator

import (
	"testing"
	"time"

	"mapreduce/internal/core"
)

func TestAssignMapTask(t *testing.T) {
	files := []string{"fileA.txt", "fileB.txt"}
	c := New(files, 3)

	t1 := c.AssignTask()
	if t1.Type != core.MapTask || t1.TaskID != 0 || t1.Filename != "fileA.txt" {
		t.Fatalf("expected map task 0 for fileA.txt, got %+v", t1)
	}

	t2 := c.AssignTask()
	if t2.Type != core.MapTask || t2.TaskID != 1 || t2.Filename != "fileB.txt" {
		t.Fatalf("expected map task 1 for fileB.txt, got %+v", t2)
	}

	t3 := c.AssignTask()
	if t3.Type != core.WaitTask {
		t.Fatalf("expected WaitTask, got %+v", t3)
	}
}

func TestTimeoutReissue(t *testing.T) {
	files := []string{"fileA.txt"}
	c := New(files, 1)

	t1 := c.AssignTask()
	if t1.Type != core.MapTask || t1.TaskID != 0 {
		t.Fatalf("expected map task 0, got %+v", t1)
	}

	// Immediate assignment should wait
	t2 := c.AssignTask()
	if t2.Type != core.WaitTask {
		t.Fatalf("expected WaitTask before timeout, got %+v", t2)
	}

	// Advance AssignedAt manually to mock timeout
	c.mu.Lock()
	c.mapTasks[0].AssignedAt = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

	// Should reissue task 0
	t3 := c.AssignTask()
	if t3.Type != core.MapTask || t3.TaskID != 0 {
		t.Fatalf("expected map task 0 to be re-issued, got %+v", t3)
	}
}

func TestPhaseTransition(t *testing.T) {
	files := []string{"fileA.txt", "fileB.txt"}
	c := New(files, 2)

	t1 := c.AssignTask()
	t2 := c.AssignTask()

	c.CompleteTask(core.TaskReport{Type: core.MapTask, TaskID: t1.TaskID})
	c.CompleteTask(core.TaskReport{Type: core.MapTask, TaskID: t2.TaskID})

	// Should transition to Reduce phase and assign reduce task
	tr1 := c.AssignTask()
	if tr1.Type != core.ReduceTask || tr1.TaskID != 0 {
		t.Fatalf("expected reduce task 0, got %+v", tr1)
	}

	tr2 := c.AssignTask()
	if tr2.Type != core.ReduceTask || tr2.TaskID != 1 {
		t.Fatalf("expected reduce task 1, got %+v", tr2)
	}

	c.CompleteTask(core.TaskReport{Type: core.ReduceTask, TaskID: tr1.TaskID})
	c.CompleteTask(core.TaskReport{Type: core.ReduceTask, TaskID: tr2.TaskID})

	// Should be complete
	exit := c.AssignTask()
	if exit.Type != core.ExitTask {
		t.Fatalf("expected ExitTask, got %+v", exit)
	}

	if !c.IsDone() {
		t.Fatalf("expected IsDone to be true")
	}
}

func TestIdempotentCompletion(t *testing.T) {
	files := []string{"fileA.txt"}
	c := New(files, 1)

	t1 := c.AssignTask()
	c.CompleteTask(core.TaskReport{Type: core.MapTask, TaskID: t1.TaskID})

	// Late duplicate report should not crash or corrupt
	c.CompleteTask(core.TaskReport{Type: core.MapTask, TaskID: t1.TaskID})
}
