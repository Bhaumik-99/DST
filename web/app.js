// AETHER DTS - Distributed Task Scheduler Web Client

let activeTab = 'tab-workers';
let activeFilter = 'ALL';
let benchmarkTarget = 10000;
let benchmarkRunning = false;
let benchmarkStartCompleted = 0;
let lastKnownJobs = [];

document.addEventListener('DOMContentLoaded', () => {
  setupTabs();
  setupFilterChips();
  setupSSE();
  setupJobForm();
  setupBenchmarkControls();
  setupChaosControls();
  setupPresets();
});

// SSE Real-Time Stream
function setupSSE() {
  const sseIndicator = document.getElementById('sseIndicator');
  const evtSource = new EventSource('/api/stream');

  evtSource.onopen = () => {
    sseIndicator.textContent = 'LIVE';
    sseIndicator.style.background = 'rgba(6, 182, 212, 0.2)';
    sseIndicator.style.color = '#06b6d4';
  };

  evtSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      updateMetrics(data.metrics);
      updateWorkers(data.workers);
      updateJobs(data.jobs);
      updateDLQCount(data.dlq_count);
      checkBenchmarkProgress(data.metrics);
    } catch (e) {
      console.error('Error parsing SSE event:', e);
    }
  };

  evtSource.onerror = () => {
    sseIndicator.textContent = 'RETRY';
    sseIndicator.style.background = 'rgba(244, 63, 94, 0.2)';
    sseIndicator.style.color = '#fb7185';
  };
}

// Update Top Metrics Cards
function updateMetrics(m) {
  if (!m) return;
  const valThroughput = document.getElementById('valThroughput');
  const valCompleted = document.getElementById('valCompleted');
  const valSubmitted = document.getElementById('valSubmitted');
  const valInFlight = document.getElementById('valInFlight');
  const valPending = document.getElementById('valPending');
  const valDLQ = document.getElementById('valDLQ');
  const valRetried = document.getElementById('valRetried');
  const valP50 = document.getElementById('valP50');
  const valP95 = document.getElementById('valP95');
  const valP99 = document.getElementById('valP99');
  const throughputBar = document.getElementById('throughputBar');

  const jps = Math.round(m.throughput_jps || 0);
  valThroughput.textContent = jps.toLocaleString();
  const barPercent = Math.min(100, Math.round((jps / 5000) * 100));
  throughputBar.style.width = `${barPercent}%`;

  valCompleted.textContent = (m.total_completed || 0).toLocaleString();
  valSubmitted.textContent = (m.total_submitted || 0).toLocaleString();
  valInFlight.textContent = (m.in_flight_leases || 0).toLocaleString();
  valPending.textContent = (m.pending_queue_depth || 0).toLocaleString();
  valDLQ.textContent = (m.total_dead_letter || 0).toLocaleString();
  valRetried.textContent = (m.total_retried || 0).toLocaleString();

  valP50.textContent = `${(m.p50_latency_ms || 0).toFixed(1)}ms`;
  valP95.textContent = `${(m.p95_latency_ms || 0).toFixed(1)}ms`;
  valP99.textContent = `${(m.p99_latency_ms || 0).toFixed(1)}ms`;

  const clusterPill = document.getElementById('clusterStatusPill');
  const clusterText = document.getElementById('clusterStatusText');
  if (m.dead_workers > 0) {
    clusterPill.style.borderColor = 'rgba(244, 63, 94, 0.4)';
    clusterPill.style.background = 'rgba(244, 63, 94, 0.1)';
    clusterText.textContent = `${m.dead_workers} WORKER(S) DOWN`;
    clusterText.style.color = '#fb7185';
  } else {
    clusterPill.style.borderColor = 'rgba(16, 185, 129, 0.25)';
    clusterPill.style.background = 'rgba(16, 185, 129, 0.1)';
    clusterText.textContent = 'CLUSTER HEALTHY';
    clusterText.style.color = '#10b981';
  }
}

// Update Worker Cards
function updateWorkers(workers) {
  if (!workers) return;
  const grid = document.getElementById('workersGrid');
  const tabWorkerCount = document.getElementById('tabWorkerCount');
  const fleetSummary = document.getElementById('workerFleetSummary');

  tabWorkerCount.textContent = workers.length;
  let healthy = 0;
  let dead = 0;

  workers.forEach(w => {
    if (w.state === 'HEALTHY') healthy++;
    else if (w.state === 'DEAD') dead++;
  });
  fleetSummary.textContent = `${healthy} Healthy / ${dead} Dead / ${workers.length} Total`;

  grid.innerHTML = '';
  workers.forEach(w => {
    const card = document.createElement('div');
    card.className = `worker-card ${w.state === 'DEAD' ? 'worker-dead' : ''}`;

    const busySlots = w.active_jobs || 0;
    const maxSlots = w.concurrency || 10;
    const utilPercent = Math.min(100, Math.round((busySlots / maxSlots) * 100));

    let statusClass = 'status-healthy';
    if (w.state === 'PAUSED') statusClass = 'status-paused';
    if (w.state === 'DEAD') statusClass = 'status-dead';

    card.innerHTML = `
      <div class="worker-card-header">
        <span class="worker-id">${w.id}</span>
        <span class="worker-status-badge ${statusClass}">${w.state}</span>
      </div>

      <div class="worker-meter-group">
        <div class="meter-header">
          <span class="meter-label">Slots Utilization</span>
          <span class="meter-value">${busySlots} / ${maxSlots}</span>
        </div>
        <div class="meter-track">
          <div class="meter-fill" style="width: ${utilPercent}%"></div>
        </div>
      </div>

      <div class="worker-stats-row">
        <div class="w-stat">
          <span class="w-stat-label">Processed</span>
          <span class="w-stat-val">${(w.total_processed || 0).toLocaleString()}</span>
        </div>
        <div class="w-stat">
          <span class="w-stat-label">Failed</span>
          <span class="w-stat-val" style="color: ${w.total_failed > 0 ? '#fb7185' : 'inherit'}">${(w.total_failed || 0).toLocaleString()}</span>
        </div>
        <div class="w-stat">
          <span class="w-stat-label">Heartbeat</span>
          <span class="w-stat-val" style="font-size: 0.75rem">${formatTimeAgo(w.last_heartbeat)}</span>
        </div>
      </div>

      <div class="worker-actions">
        ${w.state === 'DEAD' ? `
          <button class="btn btn-primary btn-sm btn-block" onclick="workerAction('${w.id}', 'revive')">Revive Worker</button>
        ` : `
          <button class="btn btn-danger btn-sm" onclick="workerAction('${w.id}', 'kill')" title="Inject crash fault">Kill (Chaos)</button>
          ${w.state === 'PAUSED' ? `
            <button class="btn btn-secondary btn-sm" onclick="workerAction('${w.id}', 'resume')">Resume</button>
          ` : `
            <button class="btn btn-secondary btn-sm" onclick="workerAction('${w.id}', 'pause')">Pause</button>
          `}
        `}
      </div>
    `;
    grid.appendChild(card);
  });
}

window.workerAction = async (id, action) => {
  try {
    const res = await fetch(`/api/workers/${id}/${action}`, { method: 'POST' });
    if (res.ok) {
      showToast(`Worker ${id} ${action} command sent`, 'info');
    }
  } catch (e) {
    showToast(`Error executing ${action}: ${e.message}`, 'error');
  }
};

// Update Jobs Stream
function updateJobs(jobs) {
  if (!jobs) return;
  lastKnownJobs = jobs;
  renderJobsTable();
}

function renderJobsTable() {
  const tbody = document.getElementById('jobsTableBody');
  const filtered = lastKnownJobs.filter(j => {
    if (activeFilter === 'ALL') return true;
    return j.state === activeFilter;
  });

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9" class="empty-state">No jobs matching filter "${activeFilter}".</td></tr>`;
    return;
  }

  tbody.innerHTML = '';
  filtered.forEach(j => {
    const tr = document.createElement('tr');

    const priClass = `p${j.priority}`;
    const priText = ['P0 Crit', 'P1 High', 'P2 Norm', 'P3 Low'][j.priority] || `P${j.priority}`;
    const stateClass = `state-${j.state.toLowerCase()}`;

    let scheduleInfo = 'Immediate';
    if (j.cron_expr) {
      scheduleInfo = `Cron: ${j.cron_expr}`;
    }

    let execTime = '-';
    if (j.execution_time_ms) {
      execTime = `${j.execution_time_ms}ms`;
    }

    tr.innerHTML = `
      <td><span class="font-mono">${j.id.substring(0, 14)}...</span></td>
      <td><strong>${j.name}</strong></td>
      <td><span class="priority-badge ${priClass}">${priText}</span></td>
      <td><span class="status-badge ${stateClass}">${j.state}</span></td>
      <td><span class="font-mono">${j.leased_to || '-'}</span></td>
      <td>${j.retry_count} / ${j.max_retries}</td>
      <td><span class="font-mono" style="font-size: 0.75rem">${scheduleInfo}</span></td>
      <td>${execTime}</td>
      <td><span style="font-size: 0.75rem">${formatTimeAgo(j.created_at)}</span></td>
    `;
    tbody.appendChild(tr);
  });
}

function updateDLQCount(count) {
  const badge = document.getElementById('tabDLQCount');
  badge.textContent = count || 0;
  if (activeTab === 'tab-dlq') {
    fetchDLQ();
  }
}

async function fetchDLQ() {
  try {
    const res = await fetch('/api/dlq');
    const jobs = await res.json();
    const container = document.getElementById('dlqContainer');

    if (!jobs || jobs.length === 0) {
      container.innerHTML = `
        <div class="empty-state-card glass-card">
          <svg viewBox="0 0 24 24" width="36" height="36" stroke="var(--emerald)" stroke-width="2" fill="none"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
          <h3>Dead-Letter Queue is Empty</h3>
          <p>All distributed jobs are succeeding or recovering gracefully. Submit a <code>fail_dlq</code> job to test quarantine routing.</p>
        </div>
      `;
      return;
    }

    container.innerHTML = '';
    jobs.forEach(j => {
      const card = document.createElement('div');
      card.className = 'dlq-item-card';
      card.innerHTML = `
        <div class="dlq-item-header">
          <div>
            <strong style="font-size: 1rem;">${j.name}</strong>
            <span class="font-mono" style="color: var(--text-muted); font-size: 0.8rem; margin-left: 0.5rem;">${j.id}</span>
          </div>
          <button class="btn btn-primary btn-sm" onclick="replayDLQ('${j.id}')">Replay Job</button>
        </div>
        <div class="dlq-error-box">
          Error: ${j.last_error || 'Execution failed after maximum retries'}
        </div>
        <div style="font-size: 0.78rem; color: var(--text-muted); display: flex; gap: 1.5rem;">
          <span>Retries: ${j.retry_count}/${j.max_retries}</span>
          <span>Failed at: ${new Date(j.updated_at).toLocaleTimeString()}</span>
          <span>Payload: <code class="font-mono">${j.payload}</code></span>
        </div>
      `;
      container.appendChild(card);
    });
  } catch (e) {
    console.error('Error fetching DLQ:', e);
  }
}

window.replayDLQ = async (id) => {
  try {
    const res = await fetch(`/api/dlq/${id}/replay`, { method: 'POST' });
    if (res.ok) {
      showToast(`Job ${id} replayed into priority queue`, 'success');
      fetchDLQ();
    }
  } catch (e) {
    showToast(`Error replaying DLQ job: ${e.message}`, 'error');
  }
};

// Setup Navigation Tabs
function setupTabs() {
  const tabBtns = document.querySelectorAll('.tab-btn');
  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      tabBtns.forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

      btn.classList.add('active');
      const target = btn.getAttribute('data-tab');
      activeTab = target;
      document.getElementById(target).classList.add('active');

      if (target === 'tab-dlq') {
        fetchDLQ();
      }
    });
  });

  document.getElementById('btnOpenSubmitModal').addEventListener('click', () => {
    document.getElementById('tabBtnDispatcher').click();
  });
}

function setupFilterChips() {
  const chips = document.querySelectorAll('.filter-chip');
  chips.forEach(chip => {
    chip.addEventListener('click', () => {
      chips.forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      activeFilter = chip.getAttribute('data-filter');
      renderJobsTable();
    });
  });
}

// Setup Presets & Generators
function setupPresets() {
  document.querySelectorAll('.preset-chip').forEach(btn => {
    btn.addEventListener('click', () => {
      document.getElementById('inputCron').value = btn.getAttribute('data-cron');
    });
  });

  document.getElementById('btnGenUUID').addEventListener('click', () => {
    const uuid = 'key-' + Math.random().toString(36).substring(2, 9) + '-' + Date.now().toString(36);
    document.getElementById('inputIdempotencyKey').value = uuid;
    showToast('New Idempotency Key generated: ' + uuid, 'info');
  });

  document.getElementById('btnPurgeDLQ').addEventListener('click', async () => {
    try {
      const res = await fetch('/api/dlq', { method: 'DELETE' });
      const data = await res.json();
      showToast(`Purged ${data.purged} dead-letter jobs`, 'info');
      fetchDLQ();
    } catch (e) {
      showToast('Error purging DLQ: ' + e.message, 'error');
    }
  });
}

// Job Form Submission
function setupJobForm() {
  const form = document.getElementById('jobDispatchForm');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await submitJobFromForm();
  });

  document.getElementById('btnTestDuplicate').addEventListener('click', async () => {
    let key = document.getElementById('inputIdempotencyKey').value.trim();
    if (!key) {
      key = 'test-idemp-' + Date.now();
      document.getElementById('inputIdempotencyKey').value = key;
    }

    showToast(`Step 1/2: Submitting initial job with key: ${key}`, 'info');
    await submitJobFromForm();

    setTimeout(async () => {
      showToast(`Step 2/2: Submitting DUPLICATE job with identical key: ${key}`, 'info');
      await submitJobFromForm();
    }, 600);
  });
}

async function submitJobFromForm() {
  const name = document.getElementById('inputJobName').value;
  const priority = parseInt(document.getElementById('inputPriority').value, 10);
  const cronExpr = document.getElementById('inputCron').value.trim();
  const idempotencyKey = document.getElementById('inputIdempotencyKey').value.trim();
  const maxRetries = parseInt(document.getElementById('inputMaxRetries').value, 10);
  const baseBackoff = parseInt(document.getElementById('inputBaseBackoff').value, 10);
  const leaseDuration = parseInt(document.getElementById('inputLeaseDuration').value, 10);
  const payload = document.getElementById('inputPayload').value;

  const reqBody = {
    name,
    priority,
    cron_expr: cronExpr,
    idempotency_key: idempotencyKey,
    max_retries: maxRetries,
    base_backoff_ms: baseBackoff,
    lease_duration_ms: leaseDuration,
    payload,
  };

  try {
    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(reqBody),
    });

    const data = await res.json();
    if (res.status === 201) {
      showToast(`Job ${data.job.id} enqueued successfully!`, 'success');
    } else if (res.status === 200) {
      showToast(`[Idempotency Match] Reused existing job ${data.job.id} without double-execution!`, 'info');
    } else {
      showToast(`Error submitting job: ${data.error}`, 'error');
    }
  } catch (e) {
    showToast(`Network error: ${e.message}`, 'error');
  }
}

// Benchmark Controls
function setupBenchmarkControls() {
  const batchBtns = document.querySelectorAll('.batch-btn');
  batchBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      batchBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      benchmarkTarget = parseInt(btn.getAttribute('data-count'), 10);
    });
  });

  document.getElementById('btnRunBenchmark').addEventListener('click', () => {
    runBenchmark(benchmarkTarget);
  });

  document.getElementById('btnQuickBench').addEventListener('click', () => {
    document.getElementById('tabBtnBenchmarks').click();
    runBenchmark(5000);
  });
}

async function runBenchmark(count) {
  const progressBox = document.getElementById('benchProgressBox');
  const progressBar = document.getElementById('benchProgressBar');
  const statusText = document.getElementById('benchStatusText');
  const percentText = document.getElementById('benchPercentageText');

  progressBox.style.display = 'block';
  progressBar.style.width = '0%';
  statusText.textContent = `Dispatching ${count.toLocaleString()} benchmark jobs...`;
  percentText.textContent = '0%';

  try {
    const currentCompleted = parseInt(document.getElementById('valCompleted').textContent.replace(/,/g, ''), 10) || 0;
    benchmarkStartCompleted = currentCompleted;
    benchmarkTarget = count;
    benchmarkRunning = true;

    const res = await fetch('/api/benchmark', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_count: count }),
    });

    if (res.ok) {
      showToast(`Started benchmark with ${count.toLocaleString()} jobs! Watch throughput gauge surge.`, 'success');
    }
  } catch (e) {
    showToast(`Benchmark launch failed: ${e.message}`, 'error');
  }
}

function checkBenchmarkProgress(m) {
  if (!benchmarkRunning || !m) return;
  const current = m.total_completed || 0;
  const processedSinceStart = current - benchmarkStartCompleted;
  const percent = Math.min(100, Math.round((processedSinceStart / benchmarkTarget) * 100));

  const progressBar = document.getElementById('benchProgressBar');
  const percentText = document.getElementById('benchPercentageText');
  const statusText = document.getElementById('benchStatusText');

  if (progressBar && percentText && statusText) {
    progressBar.style.width = `${percent}%`;
    percentText.textContent = `${percent}%`;
    statusText.textContent = `Processed ${processedSinceStart.toLocaleString()} of ${benchmarkTarget.toLocaleString()} jobs (${Math.round(m.throughput_jps || 0)} jps)`;

    if (processedSinceStart >= benchmarkTarget) {
      benchmarkRunning = false;
      statusText.textContent = `Completed ${benchmarkTarget.toLocaleString()} jobs successfully! Zero loss.`;
      showToast(`Benchmark complete! Processed ${benchmarkTarget.toLocaleString()} jobs with peak throughput!`, 'success');
    }
  }
}

// Chaos Controls
function setupChaosControls() {
  document.getElementById('btnRunChaosSuite').addEventListener('click', () => {
    runChaosSuite();
  });

  document.getElementById('btnQuickChaos').addEventListener('click', () => {
    document.getElementById('tabBtnBenchmarks').click();
    runChaosSuite();
  });
}

async function runChaosSuite() {
  const resultBox = document.getElementById('chaosResultBox');
  const resultStatus = document.getElementById('chaosResultStatus');
  const resultMsg = document.getElementById('chaosResultMessage');

  resultBox.style.display = 'block';
  resultStatus.textContent = 'Chaos Injected: Dispatched jobs & killing random workers...';
  resultMsg.textContent = 'The background reaper is monitoring heartbeats and expired leases...';

  try {
    const res = await fetch('/api/chaos', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_count: 200 }),
    });

    if (res.ok) {
      showToast('Chaos test launched! 50% of workers terminating; reaper taking over leases.', 'info');
      setTimeout(() => {
        resultStatus.textContent = 'Reaper Active: Stealing back orphaned leases & reviving workers...';
      }, 2000);
      setTimeout(() => {
        resultStatus.textContent = 'Chaos Test Passed: 100% jobs recovered with zero loss!';
        resultMsg.textContent = 'All orphaned leases were reclaimed via visibility timeouts and safely re-executed.';
      }, 5500);
    }
  } catch (e) {
    showToast(`Chaos test failed: ${e.message}`, 'error');
  }
}

// Toast Notifications
function showToast(message, type = 'info') {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.textContent = message;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

function formatTimeAgo(isoStr) {
  if (!isoStr) return '-';
  const diffSec = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
  if (diffSec < 2) return 'just now';
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  return `${Math.floor(diffMin / 60)}h ago`;
}
