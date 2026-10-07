package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/cron"
	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/storage"
)

type CoordinatorConfig struct {
	WorkerTimeout   time.Duration // Time before declaring worker dead (default: 5s)
	DefaultLease    time.Duration // Default lease duration (default: 10s)
	ReaperInterval  time.Duration // Interval for scanning expired leases (default: 1s)
	HeartbeatPeriod time.Duration // Interval between worker heartbeats (default: 1.5s)
}

func DefaultConfig() CoordinatorConfig {
	return CoordinatorConfig{
		WorkerTimeout:   5 * time.Second,
		DefaultLease:    10 * time.Second,
		ReaperInterval:  1 * time.Second,
		HeartbeatPeriod: 1500 * time.Millisecond,
	}
}

// Coordinator is the central scheduler brain managing jobs, workers, cron, and crash recovery.
type Coordinator struct {
	config    CoordinatorConfig
	store     *storage.Store
	ctx       context.Context
	cancel    context.CancelFunc
	cronSchedules sync.Map // Map of JobName -> cron.Schedule

	// Chaos metrics
	reclaimedCount atomic.Int64
}

func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		config: cfg,
		store:  storage.NewStore(),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *Coordinator) Start() {
	go c.startReaperLoop()
}

func (c *Coordinator) Stop() {
	c.cancel()
}

func (c *Coordinator) GetStore() *storage.Store {
	return c.store
}

// SubmitJob creates or deduplicates a job and enqueues it.
func (c *Coordinator) SubmitJob(req models.JobSubmissionRequest) (*models.Job, bool, error) {
	// Compute execution time
	now := time.Now()
	scheduledAt := now
	if req.DelayMs > 0 {
		scheduledAt = now.Add(time.Duration(req.DelayMs) * time.Millisecond)
	}

	// Handle Cron
	if req.CronExpr != "" {
		sch, err := cron.Parse(req.CronExpr)
		if err != nil {
			return nil, false, fmt.Errorf("invalid cron expression %q: %w", req.CronExpr, err)
		}
		c.cronSchedules.Store(req.Name, sch)
		scheduledAt = sch.Next(now)
	}

	leaseDur := time.Duration(req.LeaseDurationMs) * time.Millisecond
	if leaseDur <= 0 {
		leaseDur = c.config.DefaultLease
	}

	baseBackoff := time.Duration(req.BaseBackoffMs) * time.Millisecond
	if baseBackoff <= 0 {
		baseBackoff = 500 * time.Millisecond
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	job := &models.Job{
		ID:             generateID("job"),
		Name:           req.Name,
		Payload:        req.Payload,
		Priority:       req.Priority,
		State:          models.StatePending,
		CronExpr:       req.CronExpr,
		ScheduledAt:    scheduledAt,
		LeaseDuration:  leaseDur,
		MaxRetries:     maxRetries,
		BaseBackoff:    baseBackoff,
		MaxBackoff:     30 * time.Second,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	finalJob, created := c.store.SubmitJobIfIdempotent(job)
	return finalJob, created, nil
}

// LeaseJobs allows a worker to claim up to `batchSize` jobs.
func (c *Coordinator) LeaseJobs(workerID string, batchSize int) []*models.Job {
	return c.store.LeaseBatch(workerID, batchSize, time.Now())
}

// RenewLease allows a worker to extend its job lock.
func (c *Coordinator) RenewLease(jobID, workerID string, extension time.Duration) bool {
	return c.store.RenewLease(jobID, workerID, extension)
}

// CompleteJob marks a job as done. If it has a cron expression, schedules the next cycle.
func (c *Coordinator) CompleteJob(jobID, workerID, result string, duration time.Duration) (*models.Job, bool) {
	job, ok := c.store.MarkCompleted(jobID, workerID, result, duration)
	if !ok {
		return nil, false
	}

	// If recurring cron job, schedule next run
	if job.CronExpr != "" {
		c.scheduleNextCron(job)
	}

	return job, true
}

func (c *Coordinator) scheduleNextCron(prevJob *models.Job) {
	val, ok := c.cronSchedules.Load(prevJob.Name)
	var sch cron.Schedule
	if ok {
		sch = val.(cron.Schedule)
	} else {
		parsed, err := cron.Parse(prevJob.CronExpr)
		if err != nil {
			return
		}
		sch = parsed
		c.cronSchedules.Store(prevJob.Name, sch)
	}

	nextTime := sch.Next(time.Now())
	nextJob := &models.Job{
		ID:             generateID("cron"),
		Name:           prevJob.Name,
		Payload:        prevJob.Payload,
		Priority:       prevJob.Priority,
		State:          models.StatePending,
		CronExpr:       prevJob.CronExpr,
		ScheduledAt:    nextTime,
		LeaseDuration:  prevJob.LeaseDuration,
		MaxRetries:     prevJob.MaxRetries,
		BaseBackoff:    prevJob.BaseBackoff,
		MaxBackoff:     prevJob.MaxBackoff,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	c.store.EnqueueJob(nextJob)
}

// FailJob registers a job failure, triggering exponential backoff or DLQ.
func (c *Coordinator) FailJob(jobID, workerID, errMsg string) (*models.Job, bool) {
	return c.store.MarkFailed(jobID, workerID, errMsg)
}

// RegisterWorker registers a new or returning worker node.
func (c *Coordinator) RegisterWorker(id, hostname string, concurrency int) *models.Worker {
	w := &models.Worker{
		ID:            id,
		Hostname:      hostname,
		Concurrency:   concurrency,
		State:         models.WorkerHealthy,
		LastHeartbeat: time.Now(),
		RegisteredAt:  time.Now(),
	}
	c.store.RegisterWorker(w)
	return w
}

// WorkerHeartbeat updates the worker's liveness timestamp.
func (c *Coordinator) WorkerHeartbeat(workerID string, activeJobs int) bool {
	return c.store.HeartbeatWorker(workerID, activeJobs)
}

// startReaperLoop continuously runs crash recovery & lease reclamation.
func (c *Coordinator) startReaperLoop() {
	ticker := time.NewTicker(c.config.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.performCrashRecovery()
		}
	}
}

// performCrashRecovery reclaims orphaned jobs from dead workers and expired leases.
func (c *Coordinator) performCrashRecovery() {
	now := time.Now()

	// 1. Detect dead workers (missed heartbeats)
	workerCutoff := now.Add(-c.config.WorkerTimeout)
	deadWorkers := c.store.GetDeadWorkers(workerCutoff)
	for _, w := range deadWorkers {
		reclaimed := c.store.ReclaimWorkerJobs(w.ID)
		if len(reclaimed) > 0 {
			c.reclaimedCount.Add(int64(len(reclaimed)))
			log.Printf("[Crash Recovery] Worker %s died; reclaimed %d in-flight jobs", w.ID, len(reclaimed))
		}
	}

	// 2. Detect expired visibility leases (worker hung or didn't renew in time)
	expiredJobs := c.store.GetExpiredLeases(now)
	for _, j := range expiredJobs {
		reclaimed := c.store.ReclaimJob(j.ID)
		if reclaimed != nil {
			c.reclaimedCount.Add(1)
			log.Printf("[Crash Recovery] Lease expired for job %s (worker: %s); reclaimed and re-queued", j.ID, j.LeasedTo)
		}
	}
}

func (c *Coordinator) GetReclaimedCount() int64 {
	return c.reclaimedCount.Load()
}

func generateID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(b))
}
