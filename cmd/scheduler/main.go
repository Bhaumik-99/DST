package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
	"github.com/distributed-task-scheduler/dts/pkg/server"
	"github.com/distributed-task-scheduler/dts/pkg/worker"
)

func main() {
	port := flag.Int("port", 8080, "Port for HTTP server & dashboard")
	workerCount := flag.Int("workers", 6, "Number of concurrent worker nodes to spawn")
	workerConcurrency := flag.Int("concurrency", 50, "Concurrency per worker node")
	webDir := flag.String("web", "./web", "Directory containing dashboard web assets")
	flag.Parse()

	// Locate web directory if relative
	absWebDir, err := filepath.Abs(*webDir)
	if err != nil {
		absWebDir = *webDir
	}

	log.Printf("=========================================================")
	log.Printf("  AETHER DTS - Distributed Task Scheduler")
	log.Printf("  Engine Target: 5,000+ jobs/sec with Crash Recovery")
	log.Printf("=========================================================")

	// 1. Initialize Coordinator with crash reaper
	coordCfg := scheduler.DefaultConfig()
	coord := scheduler.NewCoordinator(coordCfg)
	coord.Start()
	log.Printf("[Coordinator] Scheduler core initialized with reaper loop")

	// 2. Initialize Worker Fleet
	workers := make([]*worker.WorkerNode, 0, *workerCount)
	for i := 1; i <= *workerCount; i++ {
		workerID := fmt.Sprintf("worker-%02d", i)
		w := worker.NewWorkerNode(workerID, *workerConcurrency, coord)
		w.Start()
		workers = append(workers, w)
		log.Printf("[Fleet] Spawned %s with concurrency %d", workerID, *workerConcurrency)
	}

	// 3. Dispatch initial demo jobs so dashboard is active immediately
	dispatchDemoJobs(coord)

	// 4. Start HTTP Server and Web Dashboard
	srv := server.NewServer(coord, workers, absWebDir)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: srv,
	}

	go func() {
		log.Printf("[HTTP Server] Listening on http://localhost:%d", *port)
		log.Printf("[Web UI] Dashboard active at http://localhost:%d", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("\n[Shutdown] Stopping scheduler cluster...")
	coord.Stop()
	for _, w := range workers {
		w.Stop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
	log.Printf("[Shutdown] Cluster shutdown complete.")
}

func dispatchDemoJobs(coord *scheduler.Coordinator) {
	// Recurring Cron Job
	_, _, _ = coord.SubmitJob(models.JobSubmissionRequest{
		Name:            "default",
		Payload:         `{"event":"telemetry_sync","source":"sensor_grid_alpha"}`,
		Priority:        models.PriorityHigh,
		CronExpr:        "*/5 * * * * *", // Every 5 seconds
		LeaseDurationMs: 8000,
		MaxRetries:      3,
	})

	// Flaky task testing retries + backoff
	_, _, _ = coord.SubmitJob(models.JobSubmissionRequest{
		Name:            "flaky",
		Payload:         `{"service":"external_payment_gateway","attempt_recovery":true}`,
		Priority:        models.PriorityCritical,
		LeaseDurationMs: 4000,
		MaxRetries:      3,
		BaseBackoffMs:   500,
	})

	// Instant high-priority job
	_, _, _ = coord.SubmitJob(models.JobSubmissionRequest{
		Name:            "default",
		Payload:         `{"order_id":"ORD-8819","action":"fulfill_priority_package"}`,
		Priority:        models.PriorityCritical,
		LeaseDurationMs: 5000,
		IdempotencyKey:  "order-dedup-8819",
	})

	log.Printf("[Bootstrap] Dispatched demo cron job (every 5s), flaky retry job, and P0 priority job")
}
