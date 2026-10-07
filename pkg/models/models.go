package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// JobState represents the lifecycle state of a job.
type JobState string

const (
	StatePending    JobState = "PENDING"
	StateScheduled  JobState = "SCHEDULED"
	StateLeased     JobState = "LEASED"
	StateRunning    JobState = "RUNNING"
	StateCompleted  JobState = "COMPLETED"
	StateRetrying   JobState = "RETRYING"
	StateDeadLetter JobState = "DEAD_LETTER"
	StateCancelled  JobState = "CANCELLED"
)

// Priority represents the execution urgency of a job.
// Lower numerical value indicates higher priority (P0 is highest).
type Priority int

const (
	PriorityCritical Priority = 0 // P0
	PriorityHigh     Priority = 1 // P1
	PriorityNormal   Priority = 2 // P2
	PriorityLow      Priority = 3 // P3
)

func (p Priority) String() string {
	switch p {
	case PriorityCritical:
		return "P0-Critical"
	case PriorityHigh:
		return "P1-High"
	case PriorityNormal:
		return "P2-Normal"
	case PriorityLow:
		return "P3-Low"
	default:
		return fmt.Sprintf("P%d", p)
	}
}

// Job defines a schedulable task with leasing, retry, and idempotency guarantees.
type Job struct {
	ID             string        `json:"id"`
	Name           string        `json:"name"`
	Payload        string        `json:"payload"`
	Priority       Priority      `json:"priority"`
	State          JobState      `json:"state"`
	CronExpr       string        `json:"cron_expr,omitempty"`
	ScheduledAt    time.Time     `json:"scheduled_at"`
	LeasedTo       string        `json:"leased_to,omitempty"`
	LeasedAt       *time.Time    `json:"leased_at,omitempty"`
	LeaseExpiresAt *time.Time    `json:"lease_expires_at,omitempty"`
	LeaseDuration  time.Duration `json:"lease_duration_ms"`
	RetryCount     int           `json:"retry_count"`
	MaxRetries     int           `json:"max_retries"`
	BaseBackoff    time.Duration `json:"base_backoff_ms"`
	MaxBackoff     time.Duration `json:"max_backoff_ms"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	LastError      string        `json:"last_error,omitempty"`
	Result         string        `json:"result,omitempty"`
	ExecutionTime  int64         `json:"execution_time_ms,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	CompletedAt    *time.Time    `json:"completed_at,omitempty"`
}

// WorkerState describes worker health.
type WorkerState string

const (
	WorkerHealthy  WorkerState = "HEALTHY"
	WorkerDegraded WorkerState = "DEGRADED"
	WorkerDead     WorkerState = "DEAD"
	WorkerPaused   WorkerState = "PAUSED"
)

// Worker represents an active computing node polling and executing tasks.
type Worker struct {
	ID             string      `json:"id"`
	Hostname       string      `json:"hostname"`
	Concurrency    int         `json:"concurrency"`
	ActiveJobs     int         `json:"active_jobs"`
	TotalProcessed int64       `json:"total_processed"`
	TotalFailed    int64       `json:"total_failed"`
	State          WorkerState `json:"state"`
	LastHeartbeat  time.Time   `json:"last_heartbeat"`
	RegisteredAt   time.Time   `json:"registered_at"`
	ChaosSimulated bool        `json:"chaos_simulated"`
}

// SystemMetrics provides high-level observability stats.
type SystemMetrics struct {
	ThroughputJPS     float64 `json:"throughput_jps"`
	TotalSubmitted    int64   `json:"total_submitted"`
	TotalCompleted    int64   `json:"total_completed"`
	TotalFailed       int64   `json:"total_failed"`
	TotalRetried      int64   `json:"total_retried"`
	TotalDeadLetter   int64   `json:"total_dead_letter"`
	ActiveWorkers     int     `json:"active_workers"`
	HealthyWorkers    int     `json:"healthy_workers"`
	DeadWorkers       int     `json:"dead_workers"`
	PendingQueueDepth int     `json:"pending_queue_depth"`
	InFlightLeases    int     `json:"in_flight_leases"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
}

// JobSubmissionRequest is the payload accepted by the API to enqueue or schedule a job.
type JobSubmissionRequest struct {
	Name           string        `json:"name"`
	Payload        string        `json:"payload"`
	Priority       Priority      `json:"priority"`
	CronExpr       string        `json:"cron_expr,omitempty"`
	DelayMs        int64         `json:"delay_ms,omitempty"`
	LeaseDurationMs int64        `json:"lease_duration_ms,omitempty"`
	MaxRetries     int           `json:"max_retries,omitempty"`
	BaseBackoffMs  int64         `json:"base_backoff_ms,omitempty"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
}

// Clone provides a safe deep copy for reads.
func (j *Job) Clone() *Job {
	data, _ := json.Marshal(j)
	var copy Job
	_ = json.Unmarshal(data, &copy)
	return &copy
}
