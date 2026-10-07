package storage

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/queue"
)

type IdempotencyRecord struct {
	JobID     string
	State     models.JobState
	Result    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// Store manages all in-memory persistent states, atomic indexing, and metrics.
type Store struct {
	mu           sync.RWMutex
	jobs         map[string]*models.Job
	leasedJobs   map[string]*models.Job // Key: JobID
	dlqJobs      map[string]*models.Job // Key: JobID
	workers      map[string]*models.Worker
	idempotency  map[string]*IdempotencyRecord

	queue        *queue.JobQueue

	// High-speed atomic counters
	totalSubmitted  atomic.Int64
	totalCompleted  atomic.Int64
	totalFailed     atomic.Int64
	totalRetried    atomic.Int64
	totalDeadLetter atomic.Int64

	// Sliding window throughput tracking
	twMu            sync.Mutex
	recentCompleted []time.Time

	// Latency sample reservoir for percentiles
	latMu      sync.Mutex
	latencies  []int64 // milliseconds
}

func NewStore() *Store {
	return &Store{
		jobs:            make(map[string]*models.Job),
		leasedJobs:      make(map[string]*models.Job),
		dlqJobs:         make(map[string]*models.Job),
		workers:         make(map[string]*models.Worker),
		idempotency:     make(map[string]*IdempotencyRecord),
		queue:           queue.NewJobQueue(),
		recentCompleted: make([]time.Time, 0, 20000),
		latencies:       make([]int64, 0, 5000),
	}
}

// SubmitJobIfIdempotent atomically checks idempotency and enqueues the job if novel.
// Returns (job, created). If created is false, job is the existing deduplicated record.
func (s *Store) SubmitJobIfIdempotent(job *models.Job) (*models.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.IdempotencyKey != "" {
		if rec, exists := s.idempotency[job.IdempotencyKey]; exists && time.Now().Before(rec.ExpiresAt) {
			if existingJob, ok := s.jobs[rec.JobID]; ok {
				return existingJob.Clone(), false
			}
		}

		s.idempotency[job.IdempotencyKey] = &IdempotencyRecord{
			JobID:     job.ID,
			State:     job.State,
			Result:    job.Result,
			CreatedAt: job.CreatedAt,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
	}

	s.jobs[job.ID] = job
	s.totalSubmitted.Add(1)
	s.queue.PushJob(job)
	return job, true
}

// EnqueueJob puts a job into the pending queue.
func (s *Store) EnqueueJob(job *models.Job) {
	s.SubmitJobIfIdempotent(job)
}

// CheckIdempotency checks if an active or completed job already exists for this key.
func (s *Store) CheckIdempotency(key string) (*models.Job, bool) {
	if key == "" {
		return nil, false
	}
	s.mu.RLock()
	rec, exists := s.idempotency[key]
	var job *models.Job
	if exists && time.Now().Before(rec.ExpiresAt) {
		job = s.jobs[rec.JobID]
	}
	s.mu.RUnlock()

	if job != nil {
		return job.Clone(), true
	}
	return nil, false
}

// RequeueJob pushes an existing job back into the priority queue (e.g. after retry or recovery).
func (s *Store) RequeueJob(job *models.Job) {
	s.mu.Lock()
	delete(s.leasedJobs, job.ID)
	s.mu.Unlock()

	s.queue.PushJob(job)
}

// LeaseBatch atomically leases up to maxJobs for workerID with the given lease duration.
func (s *Store) LeaseBatch(workerID string, maxJobs int, now time.Time) []*models.Job {
	leased := make([]*models.Job, 0, maxJobs)

	s.mu.Lock()
	defer s.mu.Unlock()

	for len(leased) < maxJobs {
		job := s.queue.PopReadyJob(now)
		if job == nil {
			break
		}

		leaseDur := job.LeaseDuration
		if leaseDur <= 0 {
			leaseDur = 10 * time.Second
		}
		expiresAt := now.Add(leaseDur)

		job.State = models.StateLeased
		job.LeasedTo = workerID
		job.LeasedAt = &now
		job.LeaseExpiresAt = &expiresAt
		job.UpdatedAt = now

		s.leasedJobs[job.ID] = job
		leased = append(leased, job)
	}

	return leased
}

// RenewLease extends the visibility timeout for an active job.
func (s *Store) RenewLease(jobID, workerID string, extension time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.leasedJobs[jobID]
	if !exists || job.LeasedTo != workerID {
		return false
	}

	newExpiry := time.Now().Add(extension)
	job.LeaseExpiresAt = &newExpiry
	job.UpdatedAt = time.Now()
	return true
}

// MarkCompleted marks a leased job as successfully completed.
func (s *Store) MarkCompleted(jobID, workerID, result string, execDuration time.Duration) (*models.Job, bool) {
	s.mu.Lock()
	job, exists := s.leasedJobs[jobID]
	if !exists || job.LeasedTo != workerID {
		s.mu.Unlock()
		return nil, false
	}

	now := time.Now()
	job.State = models.StateCompleted
	job.Result = result
	job.CompletedAt = &now
	job.ExecutionTime = execDuration.Milliseconds()
	job.UpdatedAt = now
	delete(s.leasedJobs, jobID)

	if job.IdempotencyKey != "" {
		if rec, ok := s.idempotency[job.IdempotencyKey]; ok {
			rec.State = models.StateCompleted
			rec.Result = result
		}
	}

	// Update worker processed count in the same critical section
	if w, ok := s.workers[workerID]; ok {
		w.TotalProcessed++
		if w.ActiveJobs > 0 {
			w.ActiveJobs--
		}
	}

	clone := job.Clone()
	s.mu.Unlock()

	s.totalCompleted.Add(1)
	s.recordLatency(execDuration.Milliseconds())
	s.recordThroughput(now)

	return clone, true
}

// MarkFailed handles job failure: schedules retry with backoff or moves to DLQ.
func (s *Store) MarkFailed(jobID, workerID, errMsg string) (*models.Job, bool) {
	s.mu.Lock()
	job, exists := s.leasedJobs[jobID]
	if !exists || job.LeasedTo != workerID {
		s.mu.Unlock()
		return nil, false
	}

	now := time.Now()
	delete(s.leasedJobs, jobID)
	job.LastError = errMsg
	job.UpdatedAt = now

	// Update worker failed count
	if w, ok := s.workers[workerID]; ok {
		w.TotalFailed++
		if w.ActiveJobs > 0 {
			w.ActiveJobs--
		}
	}

	s.totalFailed.Add(1)

	// Check retry condition
	if job.RetryCount < job.MaxRetries {
		job.RetryCount++
		job.State = models.StateRetrying

		// Exponential backoff with jitter: delay = min(max_delay, base * 2^(retry-1)) + jitter
		base := job.BaseBackoff
		if base <= 0 {
			base = 500 * time.Millisecond
		}
		maxBackoff := job.MaxBackoff
		if maxBackoff <= 0 {
			maxBackoff = 30 * time.Second
		}

		factor := 1 << (job.RetryCount - 1)
		delay := base * time.Duration(factor)
		if delay > maxBackoff {
			delay = maxBackoff
		}

		// Jitter (+/- 20%)
		jitter := time.Duration(rand.Int63n(int64(delay/5) + 1))
		delay = delay + jitter

		job.ScheduledAt = now.Add(delay)
		s.totalRetried.Add(1)
		s.mu.Unlock()

		// Re-enqueue for delayed execution
		s.queue.PushJob(job)
		return job.Clone(), true
	}

	// Max retries exceeded -> Dead Letter Queue
	job.State = models.StateDeadLetter
	s.dlqJobs[jobID] = job
	s.totalDeadLetter.Add(1)
	s.mu.Unlock()

	return job.Clone(), false
}

// GetExpiredLeases finds all jobs whose lease expired before the cutoff.
func (s *Store) GetExpiredLeases(cutoff time.Time) []*models.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []*models.Job
	for _, job := range s.leasedJobs {
		if job.LeaseExpiresAt != nil && job.LeaseExpiresAt.Before(cutoff) {
			expired = append(expired, job)
		}
	}
	return expired
}

// ReclaimJob recovers an orphaned or expired job and re-queues it.
func (s *Store) ReclaimJob(jobID string) *models.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.leasedJobs[jobID]
	if !exists {
		return nil
	}

	delete(s.leasedJobs, jobID)
	now := time.Now()
	job.State = models.StatePending
	job.LeasedTo = ""
	job.LeasedAt = nil
	job.LeaseExpiresAt = nil
	job.ScheduledAt = now
	job.UpdatedAt = now

	s.queue.PushJob(job)
	return job
}

// Worker Registry & Heartbeats

func (s *Store) RegisterWorker(w *models.Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[w.ID] = w
}

func (s *Store) HeartbeatWorker(workerID string, activeJobs int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return false
	}

	w.LastHeartbeat = time.Now()
	w.ActiveJobs = activeJobs
	if w.State == models.WorkerDead && !w.ChaosSimulated {
		w.State = models.WorkerHealthy
	}
	return true
}

func (s *Store) SetWorkerState(workerID string, state models.WorkerState, chaos bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return false
	}
	w.State = state
	w.ChaosSimulated = chaos
	return true
}

func (s *Store) GetDeadWorkers(cutoff time.Time) []*models.Worker {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dead []*models.Worker
	for _, w := range s.workers {
		if w.State != models.WorkerDead && w.LastHeartbeat.Before(cutoff) {
			w.State = models.WorkerDead
			dead = append(dead, w)
		}
	}
	return dead
}

// ReclaimWorkerJobs reclaims all jobs leased to a dead worker.
func (s *Store) ReclaimWorkerJobs(workerID string) []*models.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	var reclaimed []*models.Job
	for id, job := range s.leasedJobs {
		if job.LeasedTo == workerID {
			delete(s.leasedJobs, id)
			now := time.Now()
			job.State = models.StatePending
			job.LeasedTo = ""
			job.LeasedAt = nil
			job.LeaseExpiresAt = nil
			job.ScheduledAt = now
			job.UpdatedAt = now
			s.queue.PushJob(job)
			reclaimed = append(reclaimed, job)
		}
	}
	return reclaimed
}

// DLQ Operations

func (s *Store) GetDLQCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.dlqJobs)
}

func (s *Store) GetDLQJobs(limit int) []*models.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*models.Job, 0, len(s.dlqJobs))
	for _, j := range s.dlqJobs {
		result = append(result, j.Clone())
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}

func (s *Store) ReplayDLQJob(jobID string) bool {
	s.mu.Lock()
	job, exists := s.dlqJobs[jobID]
	if !exists {
		s.mu.Unlock()
		return false
	}

	delete(s.dlqJobs, jobID)
	now := time.Now()
	job.State = models.StatePending
	job.RetryCount = 0
	job.LastError = ""
	job.ScheduledAt = now
	job.UpdatedAt = now
	s.mu.Unlock()

	s.queue.PushJob(job)
	return true
}

func (s *Store) PurgeDLQ() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := len(s.dlqJobs)
	s.dlqJobs = make(map[string]*models.Job)
	return count
}

// Queries for UI & Observability

func (s *Store) GetRecentJobs(limit int) []*models.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := make([]*models.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		all = append(all, j.Clone())
	}

	// Sort newest first
	sort.Slice(all, func(i, j int) bool {
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})

	if limit > 0 && len(all) > limit {
		return all[:limit]
	}
	return all
}

func (s *Store) GetWorkers() []*models.Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]*models.Worker, 0, len(s.workers))
	for _, w := range s.workers {
		copyW := *w
		workers = append(workers, &copyW)
	}
	return workers
}

func (s *Store) GetJob(id string) *models.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if j, ok := s.jobs[id]; ok {
		return j.Clone()
	}
	return nil
}

// Latency & Metrics Tracking

func (s *Store) recordLatency(ms int64) {
	s.latMu.Lock()
	defer s.latMu.Unlock()

	const maxSamples = 5000
	if len(s.latencies) < maxSamples {
		s.latencies = append(s.latencies, ms)
	} else {
		// Reservoir sampling: replace a random existing element
		idx := rand.Intn(maxSamples)
		s.latencies[idx] = ms
	}
}

func (s *Store) recordThroughput(t time.Time) {
	s.twMu.Lock()
	defer s.twMu.Unlock()
	s.recentCompleted = append(s.recentCompleted, t)
}

func (s *Store) GetMetrics() models.SystemMetrics {
	s.mu.RLock()
	activeW := 0
	healthyW := 0
	deadW := 0
	for _, w := range s.workers {
		if w.State == models.WorkerHealthy {
			activeW++
			healthyW++
		} else if w.State == models.WorkerDead {
			deadW++
		}
	}
	inFlight := len(s.leasedJobs)
	pending := s.queue.Depth()
	s.mu.RUnlock()

	// Calculate Throughput over trailing 3 seconds
	now := time.Now()
	cutoff := now.Add(-3 * time.Second)

	s.twMu.Lock()
	validIdx := 0
	for validIdx < len(s.recentCompleted) && s.recentCompleted[validIdx].Before(cutoff) {
		validIdx++
	}
	if validIdx > 0 {
		s.recentCompleted = s.recentCompleted[validIdx:]
	}
	countInWindow := len(s.recentCompleted)
	s.twMu.Unlock()

	throughput := float64(countInWindow) / 3.0

	// Compute latencies
	s.latMu.Lock()
	var p50, p95, p99 float64
	if len(s.latencies) > 0 {
		sorted := make([]int64, len(s.latencies))
		copy(sorted, s.latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		n := len(sorted)
		p50 = float64(sorted[percentileIndex(n, 0.50)])
		p95 = float64(sorted[percentileIndex(n, 0.95)])
		p99 = float64(sorted[percentileIndex(n, 0.99)])
	}
	s.latMu.Unlock()

	return models.SystemMetrics{
		ThroughputJPS:     throughput,
		TotalSubmitted:    s.totalSubmitted.Load(),
		TotalCompleted:    s.totalCompleted.Load(),
		TotalFailed:       s.totalFailed.Load(),
		TotalRetried:      s.totalRetried.Load(),
		TotalDeadLetter:   s.totalDeadLetter.Load(),
		ActiveWorkers:     activeW,
		HealthyWorkers:    healthyW,
		DeadWorkers:       deadW,
		PendingQueueDepth: pending,
		InFlightLeases:    inFlight,
		P50LatencyMs:      p50,
		P95LatencyMs:      p95,
		P99LatencyMs:      p99,
	}
}

// percentileIndex computes the correct index for a given percentile, preventing OOB.
func percentileIndex(n int, pct float64) int {
	idx := int(float64(n-1) * pct)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}
