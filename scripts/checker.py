#!/usr/bin/env python3
"""
Checker script - runs on cron every 30 min.
Reads active_job.json, respects check_interval_minutes,
polls Supabase for chunk statuses, commits job_complete.json when done.
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


def should_check_now(job):
    """
    Returns True if enough time has elapsed since last_checked_at
    based on check_interval_minutes stored in the job file.
    """
    interval = job.get("check_interval_minutes", 30)
    last_checked = job.get("last_checked_at")

    if not last_checked:
        return True

    last_dt = datetime.fromisoformat(last_checked.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    elapsed_minutes = (now - last_dt).total_seconds() / 60

    log(f"Interval: {interval}m | Elapsed since last check: {elapsed_minutes:.1f}m")
    return elapsed_minutes >= interval


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

    # No active job → nothing to do
    job = load_active_job()
    if not job:
        log("No active_job.json found — idle, exiting")
        return

    log(f"Active job: {job['job_id']} | Series: {job.get('series_url', '?')}")
    log(f"Total chapters: {job.get('total_chapters', '?')}")

    # Check if enough time has elapsed
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

    # Update last_checked_at
    job["last_checked_at"] = datetime.now(timezone.utc).isoformat()

    if done + failed == total and total > 0:
        # All chunks finished
        log(f"Job complete! done={done} failed={failed}")

        # Write job_complete.json — this triggers the cleanup workflow
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

        # Save updated active_job too (with last_checked_at)
        save_active_job(job)

        commit_and_push(
            [ACTIVE_JOB_FILE, COMPLETE_FILE],
            f"chore: job {job_id} complete — triggering finalize"
        )
        log("job_complete.json committed — cleanup workflow will trigger")

    else:
        # Still running — just update last_checked_at and push
        log("Job still in progress — updating last_checked_at")
        save_active_job(job)
        commit_and_push(
            [ACTIVE_JOB_FILE],
            f"chore: checker update — done={done}/{total}"
        )

    log("Checker done")


if __name__ == "__main__":
    main()
