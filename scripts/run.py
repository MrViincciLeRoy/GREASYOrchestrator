import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone

from orchestrator.db import (
    get_available_workers, create_job, create_chunk,
    update_job, update_worker, update_chunk
)
from orchestrator.dispatcher import trigger_worker
from orchestrator.splitter import split_chapters


ACTIVE_JOB_FILE = "active_job.json"


def get_check_interval(total_chapters: int) -> int:
    if total_chapters <= 5:
        return 30
    elif total_chapters <= 20:
        return 60
    elif total_chapters <= 50:
        return 90
    else:
        return 120


def commit_active_job(job_data: dict):
    with open(ACTIVE_JOB_FILE, "w") as f:
        json.dump(job_data, f, indent=2)

    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git", "add", ACTIVE_JOB_FILE], check=True)

    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode != 0:
        subprocess.run(["git", "commit", "-m", f"chore: start job {job_data['job_id']}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✓ active_job.json committed — checker monitors every {job_data['check_interval_minutes']}m")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--series-url", required=True)
    parser.add_argument("--total-chapters", type=int, required=True)
    parser.add_argument("--start-chapter", type=int, default=1)
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--output-dir", default="final_output")
    args = parser.parse_args()

    check_interval = get_check_interval(args.total_chapters)

    print(f"\n{'='*60}")
    print(f"🎬 GREASY ORCHESTRATOR")
    print(f"{'='*60}")
    print(f"Season         : {args.season}")
    print(f"Series URL     : {args.series_url}")
    print(f"Chapters       : {args.start_chapter} → {args.start_chapter + args.total_chapters - 1}")
    print(f"Total          : {args.total_chapters}")
    print(f"Check interval : every {check_interval}m")

    workers = get_available_workers()
    if not workers:
        print("❌ No idle workers available")
        sys.exit(1)

    if args.workers > 0:
        workers = workers[:args.workers]
    workers = workers[:args.total_chapters]

    print(f"Workers        : {len(workers)}\n")
    for w in workers:
        print(f"  • {w['name']} → {w['owner']}/{w['repo_name']}")

    job_meta = json.dumps({
        "series_url": args.series_url,
        "start_chapter": args.start_chapter,
        "total_chapters": args.total_chapters
    })
    job = create_job(args.season, args.total_chapters, [job_meta])
    job_id = job["id"]
    print(f"\n✓ Job created: {job_id}")

    splits = split_chapters(args.total_chapters, workers, args.start_chapter)

    print(f"\n📦 Chunk plan:")
    for s in splits:
        print(f"  {s['worker']['name']} → ch{s['start_chapter']}-{s['end_chapter']} ({s['chapter_count']} chapters)")

    chunks_created = []
    for split in splits:
        worker = split["worker"]
        chunk = create_chunk(
            job_id=job_id,
            worker_id=worker["id"],
            start_ch=split["start_chapter"],
            end_ch=split["end_chapter"],
            chapter_files=[args.series_url]
        )
        chunks_created.append((worker, chunk))

    update_job(job_id, status="running")

    print(f"\n🚀 Triggering workers...")
    triggered = 0
    for worker, chunk in chunks_created:
        try:
            trigger_worker(worker, chunk)
            update_worker(worker["id"], status="busy", current_job_id=job_id)
            update_chunk(chunk["id"], status="running")
            triggered += 1
        except Exception as e:
            print(f"❌ Failed to trigger {worker['name']}: {e}")
            update_chunk(chunk["id"], status="failed")

    if triggered == 0:
        print("❌ No workers triggered successfully")
        update_job(job_id, status="failed", completed_at=datetime.utcnow().isoformat())
        sys.exit(1)

    active_job = {
        "job_id": job_id,
        "season_name": args.season,
        "series_url": args.series_url,
        "start_chapter": args.start_chapter,
        "total_chapters": args.total_chapters,
        "check_interval_minutes": check_interval,
        "last_checked_at": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "workers_triggered": triggered
    }

    commit_active_job(active_job)

    print(f"\n{'='*60}")
    print(f"✅ ORCHESTRATOR DONE — handed off to checker")
    print(f"{'='*60}")
    print(f"Job ID         : {job_id}")
    print(f"Workers        : {triggered} triggered")
    print(f"Checker runs   : every 30m (acts every {check_interval}m)")
    print(f"active_job.json committed to repo")


if __name__ == "__main__":
    main()
