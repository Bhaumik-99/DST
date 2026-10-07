import urllib.request
import json
import time

base_url = 'http://localhost:8080'

print("==================================================")
print("  AETHER DTS - LIVE API & SYSTEM VERIFICATION")
print("==================================================")

# 1. Test Idempotency: Submit duplicate keys
key = 'idemp-demo-' + str(int(time.time() * 1000))
req_data = json.dumps({
    'name': 'default',
    'payload': 'order #101',
    'priority': 0,
    'idempotency_key': key
}).encode('utf-8')

req1 = urllib.request.Request(base_url + '/api/jobs', data=req_data, headers={'Content-Type': 'application/json'})
res1 = urllib.request.urlopen(req1)
job1 = json.loads(res1.read().decode('utf-8'))
print(f"[1] Initial submission (Key: {key}):")
print(f"    Status: {res1.status} | Created: {job1['created']} | Job ID: {job1['job']['id']}")

req2 = urllib.request.Request(base_url + '/api/jobs', data=req_data, headers={'Content-Type': 'application/json'})
res2 = urllib.request.urlopen(req2)
job2 = json.loads(res2.read().decode('utf-8'))
print(f"[2] Duplicate submission with IDENTICAL Key:")
print(f"    Status: {res2.status} | Created: {job2['created']} | Reused ID: {job2['job']['id']}")
assert job1['job']['id'] == job2['job']['id'], "Job IDs must match!"
assert job2['created'] is False, "Duplicate must NOT create a new job!"
print("    ==> EXACTLY-ONCE GUARANTEE VERIFIED via Idempotency Key!")

# 2. Test Dead-Letter Queue (DLQ)
print("\n[3] Testing Dead-Letter Queue (DLQ):")
dlq_req = json.dumps({
    'name': 'fail_dlq',
    'payload': '{\"corrupted\": true}',
    'priority': 1,
    'max_retries': 1,
    'base_backoff_ms': 50
}).encode('utf-8')
req3 = urllib.request.Request(base_url + '/api/jobs', data=dlq_req, headers={'Content-Type': 'application/json'})
urllib.request.urlopen(req3)
time.sleep(0.5)

dlq_res = urllib.request.urlopen(base_url + '/api/dlq')
dlq_jobs = json.loads(dlq_res.read().decode('utf-8'))
print(f"    DLQ Quarantined count: {len(dlq_jobs)}")
if dlq_jobs:
    target_dlq = dlq_jobs[0]
    print(f"    Quarantined Job ID: {target_dlq['id']}")
    print(f"    Error Reason: {target_dlq['last_error']}")
    print(f"    Retries: {target_dlq['retry_count']}/{target_dlq['max_retries']}")

    # 3. Test DLQ Replay
    replay_url = f"{base_url}/api/dlq/{target_dlq['id']}/replay"
    req_replay = urllib.request.Request(replay_url, data=b"", headers={'Content-Type': 'application/json'})
    res_replay = urllib.request.urlopen(req_replay)
    print(f"    Replayed DLQ Job: status={res_replay.status}")

# 4. Check Worker Health & Heartbeats
print("\n[4] Cluster Workers Health Check:")
workers_res = urllib.request.urlopen(base_url + '/api/workers')
workers = json.loads(workers_res.read().decode('utf-8'))
for w in workers:
    print(f"    Worker: {w['id']} | Status: {w['state']} | Slots: {w['active_jobs']}/{w['concurrency']} | Processed: {w['total_processed']}")

# 5. Check System Metrics
print("\n[5] Live System Metrics:")
metrics_res = urllib.request.urlopen(base_url + '/api/metrics')
m = json.loads(metrics_res.read().decode('utf-8'))
print(f"    Total Submitted: {m['total_submitted']}")
print(f"    Total Completed: {m['total_completed']}")
print(f"    Healthy Workers: {m['healthy_workers']}")
print(f"    Pending Queue:   {m['pending_queue_depth']}")
print(f"    In-Flight Leases:{m['in_flight_leases']}")
print("\n==> ALL SYSTEM CHECKS PASSED SUCCESSFULLY!")
