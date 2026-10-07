package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
	"github.com/distributed-task-scheduler/dts/pkg/worker"
)

func TestIdempotencyExactOnce(t *testing.T) {
	coord := scheduler.NewCoordinator(scheduler.DefaultConfig())
	coord.Start()
	defer coord.Stop()

	w := worker.NewWorkerNode("test-worker-1", 10, coord)
	w.Start()
	defer w.Stop()

	key := "idemp-payment-unique-12345"

	// Concurrent submission of the exact same idempotency key across 50 goroutines
	var wg sync.WaitGroup
	createdCount := 0
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := models.JobSubmissionRequest{
				Name:           "default",
				Payload:        fmt.Sprintf("{\"charge\": 99, \"attempt\": %d}", idx),
				Priority:       models.PriorityCritical,
				IdempotencyKey: key,
			}
			job, created, err := coord.SubmitJob(req)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if job == nil {
				t.Errorf("job is nil")
				return
			}
			if created {
				mu.Lock()
				createdCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if createdCount != 1 {
		t.Fatalf("expected exactly 1 job created, but got %d (idempotency breach!)", createdCount)
	}

	// Wait for the single job to complete
	time.Sleep(200 * time.Millisecond)

	metrics := coord.GetStore().GetMetrics()
	if metrics.TotalCompleted != 1 {
		t.Errorf("expected exactly 1 completed job, got %d", metrics.TotalCompleted)
	}

	// Submit again after completion
	reqAfter := models.JobSubmissionRequest{
		Name:           "default",
		Payload:        "post-completion-attempt",
		Priority:       models.PriorityCritical,
		IdempotencyKey: key,
	}
	jobAfter, createdAfter, err := coord.SubmitJob(reqAfter)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if createdAfter {
		t.Fatalf("expected createdAfter to be false for duplicate key")
	}
	if jobAfter.State != models.StateCompleted {
		t.Errorf("expected returned cached job to be in COMPLETED state, got %v", jobAfter.State)
	}
}
