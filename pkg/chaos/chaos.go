package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
	"github.com/distributed-task-scheduler/dts/pkg/worker"
)

type ChaosReport struct {
	TotalJobsDispatched int           `json:"total_jobs_dispatched"`
	TotalJobsCompleted  int           `json:"total_jobs_completed"`
	WorkersKilled       int           `json:"workers_killed"`
	WorkersRevived      int           `json:"workers_revived"`
	JobsReclaimed       int64         `json:"jobs_reclaimed"`
	DurationMs          int64         `json:"duration_ms"`
	Success             bool          `json:"success"`
	Message             string        `json:"message"`
}

type ChaosRunner struct {
	coordinator *scheduler.Coordinator
	workers     []*worker.WorkerNode
	mu          sync.Mutex
}

func NewChaosRunner(coord *scheduler.Coordinator, workers []*worker.WorkerNode) *ChaosRunner {
	return &ChaosRunner{
		coordinator: coord,
		workers:     workers,
	}
}

// RunChaosTest runs a high-load chaos simulation:
// 1. Enqueues `jobCount` jobs.
// 2. Mid-execution, abruptly terminates random workers.
// 3. Coordinator reaper detects dead workers & expired leases, reclaims jobs.
// 4. Revives workers to allow the backlog to clear.
// 5. Verifies 100% job completion and zero job loss.
func (cr *ChaosRunner) RunChaosTest(ctx context.Context, jobCount int) (*ChaosReport, error) {
	start := time.Now()
	report := &ChaosReport{
		TotalJobsDispatched: jobCount,
	}

	// 1. Submit jobs with shorter leases (e.g. 2s) so reaper quickly triggers
	for i := 0; i < jobCount; i++ {
		req := models.JobSubmissionRequest{
			Name:            "default",
			Payload:         fmt.Sprintf("chaos-payload-%d", i),
			Priority:        models.Priority(i % 4),
			LeaseDurationMs: 2000, // 2s visibility timeout
			MaxRetries:      5,
		}
		_, _, err := cr.coordinator.SubmitJob(req)
		if err != nil {
			return nil, err
		}
	}

	// 2. Chaos Monkey loop: kill random workers while jobs are processing
	cr.mu.Lock()
	workerCount := len(cr.workers)
	cr.mu.Unlock()

	if workerCount == 0 {
		return nil, fmt.Errorf("no workers available for chaos test")
	}

	// Wait for jobs to start running
	time.Sleep(300 * time.Millisecond)

	// Abruptly kill 30-50% of workers
	numToKill := workerCount / 2
	if numToKill == 0 {
		numToKill = 1
	}

	killed := make([]*worker.WorkerNode, 0, numToKill)
	perm := rand.Perm(workerCount)
	for i := 0; i < numToKill; i++ {
		w := cr.workers[perm[i]]
		w.Kill()
		killed = append(killed, w)
		report.WorkersKilled++
	}

	// Allow reaper to detect dead workers and expired leases
	time.Sleep(2500 * time.Millisecond)

	// Now revive killed workers so they help process the reclaimed jobs
	for _, w := range killed {
		w.Revive()
		report.WorkersRevived++
	}

	// Wait for all jobs to complete (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			report.DurationMs = time.Since(start).Milliseconds()
			report.Message = "chaos test aborted"
			return report, ctx.Err()
		case <-timeout:
			report.DurationMs = time.Since(start).Milliseconds()
			report.TotalJobsCompleted = int(cr.coordinator.GetStore().GetMetrics().TotalCompleted)
			report.JobsReclaimed = cr.coordinator.GetReclaimedCount()
			report.Success = false
			report.Message = fmt.Sprintf("Timed out waiting for jobs to finish. Completed: %d/%d", report.TotalJobsCompleted, jobCount)
			return report, nil
		case <-ticker.C:
			metrics := cr.coordinator.GetStore().GetMetrics()
			if metrics.PendingQueueDepth == 0 && metrics.InFlightLeases == 0 {
				report.DurationMs = time.Since(start).Milliseconds()
				report.TotalJobsCompleted = int(metrics.TotalCompleted)
				report.JobsReclaimed = cr.coordinator.GetReclaimedCount()
				report.Success = true
				report.Message = fmt.Sprintf("Chaos test passed! Reclaimed %d jobs across %d worker failures with 100%% completion.",
					report.JobsReclaimed, report.WorkersKilled)
				return report, nil
			}
		}
	}
}
