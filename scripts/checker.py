#!/usr/bin/env python3
"""
Checker script - runs on cron every 30 min.
Reads active_job.json, uses job_started_at as the fixed reference point
so check intervals are always measured from job creation, not from the
last time this script ran.
"""

import os
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests

ACTIVE_JOB_FILE = "active_job.json"
COMPLETE_FILE = "job_complete.json"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

H = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")


def load_active_job():
    if not Path(ACTIVE_JOB_FILE).exists():
        return None
    with open(ACTIVE_JOB_FILE) as f:
        return json.load(f)


def save_active_job(job):
    with open(ACTIVE_JOB_FILE, "w") as f:
        json.dump(job, f, indent=2)


def get_chunks(job_id):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers=H,
        params={"job_id": f"eq.{job_id}", "select": "*"}
    )
    r.raise_for_status()
    return r.json()


def should_check_now(job) -> bool:
    """
    Returns True when the next check window has been reached.

    Uses job_started_at as the fixed origin so intervals are always
    measured from job creation, not from whenever the last cron tick
    happened to update last_checked_at.

    Logic:
      - elapsed_minutes = now - job_started_at
      - due_check_number = floor(elapsed_minutes / interval)  (1-based: first check due at T+interval)
      - checks_completed = how many checks we've already done
      - act if due_check_number > checks_completed
    """
    interval = job.get("check_interval_minutes", 30)
    started_at_raw = job.get("job_started_at") or job.get("created_at")
    checks_completed = job.get("checks_completed", 0)

    if not started_at_raw:
        log("No job_started_at found — checking immediately")
        return True

    started_dt = datetime.fromisoformat(started_at_raw.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    elapsed_minutes = (now - started_dt).total_seconds() / 60
    due_check_number = int(elapsed_minutes // interval)

    log(f"Started at     : {started_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"Elapsed        : {elapsed_minutes:.1f}m")
    log(f"Interval       : {interval}m")
    log(f"Checks due     : {due_check_number} | Checks done: {checks_completed}")

    if due_check_number > checks_completed:
        log(f"✓ Check #{checks_completed + 1} is due — proceeding")
        return True

    next_check_at = started_dt.total_seconds() if False else None
    from datetime import timedelta
    next_due = started_dt + timedelta(minutes=(checks_completed + 1) * interval)
    minutes_until_next = (next_due - now).total_seconds() / 60
    log(f"Next check due : {next_due.strftime('%H:%M:%S UTC')} (~{minutes_until_next:.0f}m from now)")
    return False


def commit_and_push(files, message):
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)

    for f in files:
        subprocess.run(["git", "add", f], check=True)

    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode == 0:
        log("Nothing to commit")
        return

    subprocess.run(["git", "commit", "-m", message], check=True)
    subprocess.run(["git", "push"], check=True)
    log(f"Pushed: {message}")


def main():
    log("Checker started")

    job = load_active_job()
    if not job:
        log("No active_job.json found — idle, exiting")
        return

    log(f"Active job: {job['job_id']} | Series: {job.get('series_url', '?')}")
    log(f"Total chapters: {job.get('total_chapters', '?')}")

    if not should_check_now(job):
        log("Not yet time to check — exiting silently")
        return

    # Poll Supabase
    job_id = job["job_id"]
    log(f"Checking chunks for job {job_id}...")

    chunks = get_chunks(job_id)
    total = len(chunks)
    done = sum(1 for c in chunks if c["status"] == "done")
    failed = sum(1 for c in chunks if c["status"] == "failed")
    running = sum(1 for c in chunks if c["status"] == "running")
    pending = sum(1 for c in chunks if c["status"] == "pending")

    log(f"Chunks → total={total} done={done} running={running} failed={failed} pending={pending}")

    # Increment checks_completed and update last_checked_at
    job["checks_completed"] = job.get("checks_completed", 0) + 1
    job["last_checked_at"] = datetime.now(timezone.utc).isoformat()

    if done + failed == total and total > 0:
        log(f"Job complete! done={done} failed={failed}")

        complete_data = {
            "job_id": job_id,
            "season_name": job.get("season_name", "unknown"),
            "series_url": job.get("series_url", ""),
            "total_chapters": job.get("total_chapters", 0),
            "chunks_done": done,
            "chunks_failed": failed,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }

        with open(COMPLETE_FILE, "w") as f:
            json.dump(complete_data, f, indent=2)

        save_active_job(job)

        commit_and_push(
            [ACTIVE_JOB_FILE, COMPLETE_FILE],
            f"chore: job {job_id} complete — triggering finalize"
        )
        log("job_complete.json committed — cleanup workflow will trigger")

    else:
        log("Job still in progress — updating check counter")
        save_active_job(job)
        commit_and_push(
            [ACTIVE_JOB_FILE],
            f"chore: checker #{job['checks_completed']} — done={done}/{total}"
        )

    log("Checker done")


if __name__ == "__main__":
    main()
