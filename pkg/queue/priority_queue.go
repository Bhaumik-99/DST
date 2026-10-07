package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/models"
)

// JobQueue manages jobs ready for leasing, ordered by Priority and ScheduledAt.
type JobQueue struct {
	mu    sync.Mutex
	items []*models.Job
}

func NewJobQueue() *JobQueue {
	jq := &JobQueue{
		items: make([]*models.Job, 0, 10000),
	}
	heap.Init(jq)
	return jq
}

func (jq *JobQueue) Len() int {
	return len(jq.items)
}

func (jq *JobQueue) Less(i, j int) bool {
	// Higher priority (lower numerical value) first
	if jq.items[i].Priority != jq.items[j].Priority {
		return jq.items[i].Priority < jq.items[j].Priority
	}
	// Earlier scheduled time first
	return jq.items[i].ScheduledAt.Before(jq.items[j].ScheduledAt)
}

func (jq *JobQueue) Swap(i, j int) {
	jq.items[i], jq.items[j] = jq.items[j], jq.items[i]
}

func (jq *JobQueue) Push(x interface{}) {
	item := x.(*models.Job)
	jq.items = append(jq.items, item)
}

func (jq *JobQueue) Pop() interface{} {
	old := jq.items
	n := len(old)
	item := old[n-1]
	jq.items = old[0 : n-1]
	return item
}

// Thread-safe methods:

func (jq *JobQueue) PushJob(job *models.Job) {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	heap.Push(jq, job)
}

// PopReadyJob pops the highest priority job that is ready (ScheduledAt <= now).
func (jq *JobQueue) PopReadyJob(now time.Time) *models.Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.items) == 0 {
		return nil
	}

	// In a min-heap, jq.items[0] is the highest priority.
	// But what if items[0] is scheduled in the future?
	// We check if the root is ready.
	if jq.items[0].ScheduledAt.After(now) {
		// Even root is not ready; if root's ScheduledAt is future, we can check if any other item is ready,
		// or maintain delayed jobs separately.
		// For high performance, we scan for the best ready job if root isn't ready.
		bestIdx := -1
		for i, jb := range jq.items {
			if !jb.ScheduledAt.After(now) {
				if bestIdx == -1 {
					bestIdx = i
				} else {
					// Check if item[i] is higher priority than item[bestIdx]
					if jb.Priority < jq.items[bestIdx].Priority ||
						(jb.Priority == jq.items[bestIdx].Priority && jb.ScheduledAt.Before(jq.items[bestIdx].ScheduledAt)) {
						bestIdx = i
					}
				}
			}
		}
		if bestIdx == -1 {
			return nil
		}
		job := heap.Remove(jq, bestIdx).(*models.Job)
		return job
	}

	job := heap.Pop(jq).(*models.Job)
	return job
}

// PopBatchReady pops up to `maxCount` ready jobs atomically.
func (jq *JobQueue) PopBatchReady(now time.Time, maxCount int) []*models.Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.items) == 0 || maxCount <= 0 {
		return nil
	}

	var batch []*models.Job
	for len(jq.items) > 0 && len(batch) < maxCount {
		if jq.items[0].ScheduledAt.After(now) {
			break
		}
		job := heap.Pop(jq).(*models.Job)
		batch = append(batch, job)
	}

	return batch
}

func (jq *JobQueue) Depth() int {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	return len(jq.items)
}
