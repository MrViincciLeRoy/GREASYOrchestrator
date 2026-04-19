#!/usr/bin/env python3
"""
Finalize script - triggered by job_complete.json push.
Downloads chunk videos, stitches them, uploads final release.
"""

import os
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.db import get_chunks_for_job, update_job, update_worker, get_all_workers
from orchestrator.downloader import download_all_chunks
from orchestrator.stitcher import stitch
from orchestrator.dispatcher import create_release, upload_release_asset


COMPLETE_FILE = "job_complete.json"
OUTPUT_DIR = "final_output"


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")


def main():
    log("Finalize started")

    if not Path(COMPLETE_FILE).exists():
        print(f"❌ {COMPLETE_FILE} not found")
        sys.exit(1)

    with open(COMPLETE_FILE) as f:
        complete_data = json.load(f)

    job_id = complete_data["job_id"]
    season_name = complete_data.get("season_name", "output")
    chunks_failed = complete_data.get("chunks_failed", 0)

    log(f"Job ID     : {job_id}")
    log(f"Season     : {season_name}")
    log(f"Done chunks: {complete_data.get('chunks_done', 0)}")
    log(f"Failed     : {chunks_failed}")

    # Get all chunks
    chunks = get_chunks_for_job(job_id)
    done_chunks = [c for c in chunks if c["status"] == "done" and c.get("release_url")]

    if not done_chunks:
        log("❌ No completed chunks with release URLs found")
        update_job(job_id, status="failed", completed_at=datetime.utcnow().isoformat())
        sys.exit(1)

    log(f"Downloading {len(done_chunks)} chunk(s)...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chunk_paths = download_all_chunks(chunks, OUTPUT_DIR)

    if not chunk_paths:
        log("❌ No chunks downloaded")
        sys.exit(1)

    # Stitch or use single chunk
    if len(chunk_paths) == 1:
        final_video = chunk_paths[0]
        log("Single chunk — skipping stitch")
    else:
        final_video = os.path.join(OUTPUT_DIR, f"{season_name}_final.mp4")
        stitch(chunk_paths, final_video)

    # Upload as release
    gh_token = os.environ["GITHUB_TOKEN"]
    gh_owner = os.environ["GITHUB_REPOSITORY_OWNER"]
    gh_repo = os.environ.get("GITHUB_REPOSITORY", "").split("/")[-1]

    tag = f"{season_name}-{datetime.utcnow().strftime('%Y%m%d-%H%M')}"
    log(f"Creating release: {tag}")

    release = create_release(gh_owner, gh_repo, tag, gh_token)
    final_url = upload_release_asset(release["upload_url"], final_video, gh_token)

    log(f"Final video: {final_url}")

    # Mark job done in DB
    update_job(
        job_id,
        status="done",
        final_release_url=final_url,
        completed_at=datetime.utcnow().isoformat()
    )

    # Free all workers that were on this job
    all_workers = get_all_workers()
    for w in all_workers:
        if w.get("current_job_id") == job_id:
            update_worker(w["id"], status="idle", current_job_id=None)
            log(f"Freed worker: {w['name']}")

    log("✅ Finalize complete")
    log(f"Release: {final_url}")


if __name__ == "__main__":
    main()
