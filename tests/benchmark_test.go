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

func TestThroughputBenchmark100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K benchmark in short mode")
	}

	jobCount := 100000
	workerCount := 16
	concurrency := 50

	coord := scheduler.NewCoordinator(scheduler.DefaultConfig())
	coord.Start()
	defer coord.Stop()

	// Spin up worker fleet
	workers := make([]*worker.WorkerNode, 0, workerCount)
	for i := 1; i <= workerCount; i++ {
		w := worker.NewWorkerNode(fmt.Sprintf("bench-w-%d", i), concurrency, coord)
		w.Start()
		workers = append(workers, w)
		defer w.Stop()
	}

	time.Sleep(50 * time.Millisecond)

	startSubmission := time.Now()
	numProducers := 8
	jobsPerProducer := jobCount / numProducers
	var wg sync.WaitGroup

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < jobsPerProducer; i++ {
				req := models.JobSubmissionRequest{
					Name:            "benchmark",
					Payload:         "bench",
					Priority:        models.Priority(i % 4),
					LeaseDurationMs: 10000,
					MaxRetries:      1,
				}
				_, _, _ = coord.SubmitJob(req)
			}
		}()
	}

	wg.Wait()
	t.Logf("Submitted %d jobs in %v", jobCount, time.Since(startSubmission))

	startProcessing := time.Now()
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			metrics := coord.GetStore().GetMetrics()
			t.Fatalf("Timeout waiting for 100K jobs. Completed: %d/%d", metrics.TotalCompleted, jobCount)
		case <-ticker.C:
			metrics := coord.GetStore().GetMetrics()
			if metrics.TotalCompleted >= int64(jobCount) {
				dur := time.Since(startProcessing)
				throughput := float64(jobCount) / dur.Seconds()
				t.Logf("100K Benchmark Complete! Processed %d jobs in %v. Throughput: %.2f jobs/sec",
					jobCount, dur, throughput)
				t.Logf("Metrics: p50: %.2fms, p95: %.2fms, p99: %.2fms",
					metrics.P50LatencyMs, metrics.P95LatencyMs, metrics.P99LatencyMs)

				if throughput < 5000 {
					t.Logf("Warning: Throughput was %.2f jps, target is 5000 jps", throughput)
				}
				return
			}
		}
	}
}
