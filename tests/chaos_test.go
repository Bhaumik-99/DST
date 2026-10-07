package tests

import (
	"context"
	"testing"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/chaos"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
	"github.com/distributed-task-scheduler/dts/pkg/worker"
)

func TestCrashRecoveryAndWorkerFailover(t *testing.T) {
	// Configure fast reaper and timeouts for the test
	cfg := scheduler.CoordinatorConfig{
		WorkerTimeout:   1200 * time.Millisecond,
		DefaultLease:    1000 * time.Millisecond,
		ReaperInterval:  300 * time.Millisecond,
		HeartbeatPeriod: 500 * time.Millisecond,
	}
	coord := scheduler.NewCoordinator(cfg)
	coord.Start()
	defer coord.Stop()

	// Spawn 6 workers
	workers := make([]*worker.WorkerNode, 0, 6)
	for i := 1; i <= 6; i++ {
		w := worker.NewWorkerNode(string(rune('A'+i-1)), 10, coord)
		w.Start()
		workers = append(workers, w)
		defer w.Stop()
	}

	runner := chaos.NewChaosRunner(coord, workers)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	report, err := runner.RunChaosTest(ctx, 100)
	if err != nil {
		t.Fatalf("chaos test returned error: %v", err)
	}

	if !report.Success {
		t.Fatalf("chaos test failed: %s", report.Message)
	}

	if report.WorkersKilled == 0 {
		t.Errorf("expected workers to be killed")
	}

	if report.JobsReclaimed == 0 {
		t.Logf("Notice: all jobs may have finished before kill, or jobs were reclaimed")
	}

	metrics := coord.GetStore().GetMetrics()
	if metrics.TotalCompleted < 100 {
		t.Errorf("expected at least 100 jobs completed, got %d", metrics.TotalCompleted)
	}
}
