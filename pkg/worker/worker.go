package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
)

// TaskHandler represents the execution logic for a task type.
type TaskHandler func(ctx context.Context, job *models.Job) (string, error)

type WorkerNode struct {
	ID          string
	Hostname    string
	Concurrency int
	coordinator *scheduler.Coordinator

	handlers    map[string]TaskHandler
	handlerMu   sync.RWMutex

	ctxMu       sync.Mutex // Protects ctx and cancel during Kill/Revive transitions
	ctx         context.Context
	cancel      context.CancelFunc

	activeJobs  atomic.Int32
	isKilled    atomic.Bool
	isPaused    atomic.Bool

	batchSize   int
	pollDelay   time.Duration
}

func NewWorkerNode(id string, concurrency int, coord *scheduler.Coordinator) *WorkerNode {
	host, _ := os.Hostname()
	if host == "" {
		host = "node-localhost"
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &WorkerNode{
		ID:          id,
		Hostname:    fmt.Sprintf("%s-%s", host, id),
		Concurrency: concurrency,
		coordinator: coord,
		handlers:    make(map[string]TaskHandler),
		ctx:         ctx,
		cancel:      cancel,
		batchSize:   concurrency,
		pollDelay:   10 * time.Millisecond,
	}

	w.registerDefaultHandlers()
	return w
}

func (w *WorkerNode) RegisterHandler(taskType string, handler TaskHandler) {
	w.handlerMu.Lock()
	defer w.handlerMu.Unlock()
	w.handlers[taskType] = handler
}

func (w *WorkerNode) registerDefaultHandlers() {
	// Standard fast task
	w.RegisterHandler("default", func(ctx context.Context, job *models.Job) (string, error) {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
		return fmt.Sprintf("processed job %s payload: %s", job.ID, job.Payload), nil
	})

	// Benchmark instant task for high throughput (5K+ jobs/sec)
	w.RegisterHandler("benchmark", func(ctx context.Context, job *models.Job) (string, error) {
		// Microsecond-scale execution to test pure scheduler throughput
		return "ok", nil
	})

	// Flaky task that fails 2 times before succeeding (demonstrates retry + exponential backoff)
	w.RegisterHandler("flaky", func(ctx context.Context, job *models.Job) (string, error) {
		if job.RetryCount < 2 {
			return "", fmt.Errorf("transient network timeout (retry %d/%d)", job.RetryCount, job.MaxRetries)
		}
		return fmt.Sprintf("succeeded on retry %d", job.RetryCount), nil
	})

	// Fatal task that always fails to demonstrate Dead-Letter Queue (DLQ)
	w.RegisterHandler("fail_dlq", func(ctx context.Context, job *models.Job) (string, error) {
		return "", fmt.Errorf("unrecoverable schema validation error (destined for DLQ)")
	})

	// Long running task demonstrating lease auto-renewal
	w.RegisterHandler("long_running", func(ctx context.Context, job *models.Job) (string, error) {
		for i := 0; i < 5; i++ {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(500 * time.Millisecond):
				// Renew lease midway
				w.coordinator.RenewLease(job.ID, w.ID, 10*time.Second)
			}
		}
		return "long running job completed with renewed leases", nil
	})
}

func (w *WorkerNode) Start() {
	w.coordinator.RegisterWorker(w.ID, w.Hostname, w.Concurrency)

	go w.heartbeatLoop()
	go w.pollingLoop()
}

func (w *WorkerNode) Stop() {
	w.cancel()
}

func (w *WorkerNode) Kill() {
	w.isKilled.Store(true)
	w.coordinator.GetStore().SetWorkerState(w.ID, models.WorkerDead, true)
	w.ctxMu.Lock()
	w.cancel()
	w.ctxMu.Unlock()
}

func (w *WorkerNode) Revive() {
	if !w.isKilled.Load() {
		return
	}
	w.ctxMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx
	w.cancel = cancel
	w.ctxMu.Unlock()
	w.isKilled.Store(false)
	w.coordinator.GetStore().SetWorkerState(w.ID, models.WorkerHealthy, false)
	w.Start()
}

// getContext returns the current context in a race-safe way.
func (w *WorkerNode) getContext() context.Context {
	w.ctxMu.Lock()
	defer w.ctxMu.Unlock()
	return w.ctx
}

func (w *WorkerNode) Pause() {
	w.isPaused.Store(true)
	w.coordinator.GetStore().SetWorkerState(w.ID, models.WorkerPaused, false)
}

func (w *WorkerNode) Resume() {
	w.isPaused.Store(false)
	w.coordinator.GetStore().SetWorkerState(w.ID, models.WorkerHealthy, false)
}

func (w *WorkerNode) IsKilled() bool {
	return w.isKilled.Load()
}

func (w *WorkerNode) heartbeatLoop() {
	ticker := time.NewTicker(1200 * time.Millisecond)
	defer ticker.Stop()

	ctx := w.getContext()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.isKilled.Load() {
				w.coordinator.WorkerHeartbeat(w.ID, int(w.activeJobs.Load()))
			}
		}
	}
}

func (w *WorkerNode) pollingLoop() {
	sem := make(chan struct{}, w.Concurrency)
	ctx := w.getContext()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if w.isKilled.Load() || w.isPaused.Load() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Calculate available slots
		curActive := int(w.activeJobs.Load())
		available := w.Concurrency - curActive
		if available <= 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		batchLimit := available
		if batchLimit > w.batchSize {
			batchLimit = w.batchSize
		}

		jobs := w.coordinator.LeaseJobs(w.ID, batchLimit)
		if len(jobs) == 0 {
			time.Sleep(w.pollDelay)
			continue
		}

		for _, j := range jobs {
			sem <- struct{}{}
			w.activeJobs.Add(1)

			go func(job *models.Job) {
				defer func() {
					<-sem
					w.activeJobs.Add(-1)
				}()
				w.executeJob(job)
			}(j)
		}
	}
}

func (w *WorkerNode) executeJob(job *models.Job) {
	if w.isKilled.Load() {
		return
	}

	start := time.Now()

	// Select handler
	handlerName := job.Name
	w.handlerMu.RLock()
	handler, ok := w.handlers[handlerName]
	if !ok {
		handler = w.handlers["default"]
	}
	w.handlerMu.RUnlock()

	result, err := handler(w.getContext(), job)
	execDur := time.Since(start)

	if w.isKilled.Load() {
		// Worker crashed mid-execution! Do NOT report completion or failure;
		// Coordinator's crash recovery reaper will reclaim the expired lease!
		return
	}

	if err != nil {
		w.coordinator.FailJob(job.ID, w.ID, err.Error())
	} else {
		w.coordinator.CompleteJob(job.ID, w.ID, result, execDur)
	}
}
