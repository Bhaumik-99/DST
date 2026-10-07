package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/distributed-task-scheduler/dts/pkg/chaos"
	"github.com/distributed-task-scheduler/dts/pkg/models"
	"github.com/distributed-task-scheduler/dts/pkg/scheduler"
	"github.com/distributed-task-scheduler/dts/pkg/worker"
)

type Server struct {
	coordinator *scheduler.Coordinator
	workers     []*worker.WorkerNode
	workerMu    sync.RWMutex
	webDir      string
	mux         *http.ServeMux
}

func NewServer(coord *scheduler.Coordinator, workers []*worker.WorkerNode, webDir string) *Server {
	s := &Server{
		coordinator: coord,
		workers:     workers,
		webDir:      webDir,
		mux:         http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) AddWorker(w *worker.WorkerNode) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	s.workers = append(s.workers, w)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/metrics", s.handleMetrics)
	s.mux.HandleFunc("/api/stream", s.handleSSEStream)
	s.mux.HandleFunc("/api/jobs", s.handleJobs)
	s.mux.HandleFunc("/api/jobs/", s.handleJobByID)
	s.mux.HandleFunc("/api/workers", s.handleWorkers)
	s.mux.HandleFunc("/api/workers/", s.handleWorkerAction)
	s.mux.HandleFunc("/api/dlq", s.handleDLQ)
	s.mux.HandleFunc("/api/dlq/", s.handleDLQAction)
	s.mux.HandleFunc("/api/benchmark", s.handleBenchmark)
	s.mux.HandleFunc("/api/chaos", s.handleChaos)

	// Static web assets
	fileServer := http.FileServer(http.Dir(s.webDir))
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers for local development flexibility
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.coordinator.GetStore().GetMetrics()
	writeJSON(w, http.StatusOK, metrics)
}

func (s *Server) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			data := map[string]interface{}{
				"metrics":   s.coordinator.GetStore().GetMetrics(),
				"workers":   s.coordinator.GetStore().GetWorkers(),
				"jobs":      s.coordinator.GetStore().GetRecentJobs(25),
				"dlq_count": s.coordinator.GetStore().GetDLQCount(),
			}
			jsonData, _ := json.Marshal(data)
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	}
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 50
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if l, err := strconv.Atoi(lStr); err == nil {
				limit = l
			}
		}
		jobs := s.coordinator.GetStore().GetRecentJobs(limit)
		writeJSON(w, http.StatusOK, jobs)

	case http.MethodPost:
		var req models.JobSubmissionRequest
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "failed to read body")
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}

		if req.Name == "" {
			req.Name = "default"
		}

		job, created, err := s.coordinator.SubmitJob(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		statusCode := http.StatusCreated
		if !created {
			// Idempotent deduplication matched existing job!
			statusCode = http.StatusOK
		}
		writeJSON(w, statusCode, map[string]interface{}{
			"created": created,
			"job":     job,
		})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "job ID required")
		return
	}

	job := s.coordinator.GetStore().GetJob(jobID)
	if job == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers := s.coordinator.GetStore().GetWorkers()
	writeJSON(w, http.StatusOK, workers)
}

func (s *Server) handleWorkerAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// /api/workers/{id}/{action}
	if len(parts) < 4 {
		writeError(w, http.StatusBadRequest, "invalid worker route")
		return
	}
	workerID := parts[2]
	action := parts[3]

	s.workerMu.RLock()
	var targetWorker *worker.WorkerNode
	for _, wn := range s.workers {
		if wn.ID == workerID {
			targetWorker = wn
			break
		}
	}
	s.workerMu.RUnlock()

	if targetWorker == nil {
		writeError(w, http.StatusNotFound, "worker not found")
		return
	}

	switch action {
	case "kill":
		targetWorker.Kill()
		writeJSON(w, http.StatusOK, map[string]string{"status": "killed", "worker_id": workerID})
	case "revive":
		targetWorker.Revive()
		writeJSON(w, http.StatusOK, map[string]string{"status": "revived", "worker_id": workerID})
	case "pause":
		targetWorker.Pause()
		writeJSON(w, http.StatusOK, map[string]string{"status": "paused", "worker_id": workerID})
	case "resume":
		targetWorker.Resume()
		writeJSON(w, http.StatusOK, map[string]string{"status": "resumed", "worker_id": workerID})
	default:
		writeError(w, http.StatusBadRequest, "unknown worker action: "+action)
	}
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jobs := s.coordinator.GetStore().GetDLQJobs(100)
		writeJSON(w, http.StatusOK, jobs)
	case http.MethodDelete:
		count := s.coordinator.GetStore().PurgeDLQ()
		writeJSON(w, http.StatusOK, map[string]interface{}{"purged": count})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleDLQAction(w http.ResponseWriter, r *http.Request) {
	// /api/dlq/{id}/replay
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 4 && parts[3] == "replay" {
		jobID := parts[2]
		ok := s.coordinator.GetStore().ReplayDLQJob(jobID)
		if !ok {
			writeError(w, http.StatusNotFound, "DLQ job not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"replayed": true, "job_id": jobID})
		return
	}
	writeError(w, http.StatusBadRequest, "invalid DLQ action")
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	var req struct {
		JobCount int `json:"job_count"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.JobCount <= 0 {
		req.JobCount = 10000 // Default 10,000 for rapid benchmark
	}

	go func(count int) {
		log.Printf("[Benchmark] Starting benchmark with %d jobs...", count)
		start := time.Now()

		// Use concurrent producers for high-throughput submission
		numProducers := 8
		jobsPerProducer := count / numProducers
		remainder := count % numProducers

		var wg sync.WaitGroup
		for p := 0; p < numProducers; p++ {
			batchCount := jobsPerProducer
			if p == 0 {
				batchCount += remainder
			}
			wg.Add(1)
			go func(producerID, batchCount int) {
				defer wg.Done()
				for i := 0; i < batchCount; i++ {
					subReq := models.JobSubmissionRequest{
						Name:            "benchmark",
						Payload:         fmt.Sprintf("bench-p%d-%d", producerID, i),
						Priority:        models.Priority(i % 4),
						LeaseDurationMs: 5000,
						MaxRetries:      1,
					}
					_, _, _ = s.coordinator.SubmitJob(subReq)
				}
			}(p, batchCount)
		}
		wg.Wait()
		log.Printf("[Benchmark] Submitted %d jobs in %v", count, time.Since(start))
	}(req.JobCount)

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":    "benchmark_started",
		"job_count": req.JobCount,
	})
}

func (s *Server) handleChaos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	var req struct {
		JobCount int `json:"job_count"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.JobCount <= 0 {
		req.JobCount = 200
	}

	s.workerMu.RLock()
	workersCopy := make([]*worker.WorkerNode, len(s.workers))
	copy(workersCopy, s.workers)
	s.workerMu.RUnlock()

	runner := chaos.NewChaosRunner(s.coordinator, workersCopy)

	// Use a background context, not the request context. The HTTP request
	// context gets cancelled as soon as the handler returns, which would
	// immediately abort the chaos test goroutine.
	chaosCtx, chaosCancel := context.WithTimeout(context.Background(), 60*time.Second)

	go func() {
		defer chaosCancel()
		report, err := runner.RunChaosTest(chaosCtx, req.JobCount)
		if err != nil {
			log.Printf("[Chaos Test] Error: %v", err)
			return
		}
		log.Printf("[Chaos Test] Report: %+v", report)
	}()

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":    "chaos_test_started",
		"job_count": req.JobCount,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
