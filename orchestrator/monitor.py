import time
from orchestrator.db import get_chunks_for_job

def wait_for_completion(job_id, timeout_minutes=480, poll_interval=60):
    timeout = timeout_minutes * 60
    elapsed = 0

    print(f"\n⏳ Monitoring job {job_id}...")

    while elapsed < timeout:
        chunks = get_chunks_for_job(job_id)
        total = len(chunks)
        done = sum(1 for c in chunks if c['status'] == 'done')
        failed = sum(1 for c in chunks if c['status'] == 'failed')
        running = sum(1 for c in chunks if c['status'] == 'running')

        print(f"  [{elapsed // 60}m elapsed] done={done} running={running} failed={failed} total={total}")

        if done + failed == total:
            if failed:
                failed_chunks = [c for c in chunks if c['status'] == 'failed']
                print(f"⚠ {failed} chunks failed: {[c['id'] for c in failed_chunks]}")
            return chunks

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Job {job_id} timed out after {timeout_minutes}m")
