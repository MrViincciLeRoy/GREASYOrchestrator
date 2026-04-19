import os
import sys
import json
import argparse
from datetime import datetime

from orchestrator.db import (
    get_available_workers, create_job, create_chunk,
    update_job, update_worker, update_chunk
)
from orchestrator.dispatcher import trigger_worker, create_release, upload_release_asset
from orchestrator.splitter import split_chapters
from orchestrator.monitor import wait_for_completion
from orchestrator.downloader import download_all_chunks
from orchestrator.stitcher import stitch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season', required=True, help='Season name e.g. "Knight_King_S1"')
    parser.add_argument('--chapters', required=True, help='Comma-separated PDF filenames in worker data/ folders')
    parser.add_argument('--workers', type=int, default=0, help='Max workers to use (0 = all available)')
    parser.add_argument('--output-dir', default='final_output')
    args = parser.parse_args()

    chapter_files = [c.strip() for c in args.chapters.split(',')]
    total = len(chapter_files)

    print(f"\n{'='*60}")
    print(f"🎬 GREASY ORCHESTRATOR")
    print(f"{'='*60}")
    print(f"Season : {args.season}")
    print(f"Chapters: {total}")

    # Get workers
    workers = get_available_workers()
    if not workers:
        print("❌ No idle workers available in DB")
        sys.exit(1)

    if args.workers > 0:
        workers = workers[:args.workers]

    print(f"Workers : {len(workers)} available\n")
    for w in workers:
        print(f"  • {w['name']} → {w['owner']}/{w['repo_name']}")

    # Create job
    job = create_job(args.season, total, chapter_files)
    job_id = job['id']
    print(f"\n✓ Job created: {job_id}")

    # Split chapters across workers
    splits = split_chapters(chapter_files, workers)

    print(f"\n📦 Chunk plan:")
    for s in splits:
        print(f"  {s['worker']['name']} → chapters {s['start_chapter']}-{s['end_chapter']} ({len(s['chapter_files'])} files)")

    # Create chunks in DB and trigger workers
    chunks_created = []
    for split in splits:
        worker = split['worker']
        chunk = create_chunk(
            job_id=job_id,
            worker_id=worker['id'],
            start_ch=split['start_chapter'],
            end_ch=split['end_chapter'],
            chapter_files=split['chapter_files']
        )
        chunks_created.append((worker, chunk))

    update_job(job_id, status='running')

    print(f"\n🚀 Triggering workers...")
    for worker, chunk in chunks_created:
        try:
            trigger_worker(worker, chunk)
            update_worker(worker['id'], status='busy', current_job_id=job_id)
            update_chunk(chunk['id'], status='running')
        except Exception as e:
            print(f"❌ Failed to trigger {worker['name']}: {e}")
            update_chunk(chunk['id'], status='failed')

    # Wait for all chunks
    completed_chunks = wait_for_completion(job_id)

    done = [c for c in completed_chunks if c['status'] == 'done']
    if not done:
        print("❌ No chunks completed successfully")
        update_job(job_id, status='failed', completed_at=datetime.utcnow().isoformat())
        sys.exit(1)

    # Download chunks
    print(f"\n⬇ Downloading {len(done)} chunks...")
    os.makedirs(args.output_dir, exist_ok=True)
    chunk_paths = download_all_chunks(completed_chunks, args.output_dir)

    if len(chunk_paths) < 2:
        final_video = chunk_paths[0]
        print(f"Only one chunk, skipping stitch")
    else:
        final_video = os.path.join(args.output_dir, f"{args.season}_final.mp4")
        stitch(chunk_paths, final_video)

    # Upload final video as orchestrator release
    gh_token = os.environ['GITHUB_TOKEN']
    gh_owner = os.environ['GITHUB_REPOSITORY_OWNER']
    gh_repo = os.environ.get('GITHUB_REPOSITORY', '').split('/')[-1] or 'GREASYvideo-Orchestrator'

    tag = f"{args.season}-{datetime.utcnow().strftime('%Y%m%d-%H%M')}"
    release = create_release(gh_owner, gh_repo, tag, gh_token)
    final_url = upload_release_asset(release['upload_url'], final_video, gh_token)

    # Mark job done
    update_job(job_id, status='done', final_release_url=final_url, completed_at=datetime.utcnow().isoformat())

    # Free workers
    for worker, _ in chunks_created:
        update_worker(worker['id'], status='idle', current_job_id=None)

    print(f"\n{'='*60}")
    print(f"✅ SEASON COMPLETE")
    print(f"{'='*60}")
    print(f"Final video : {final_url}")
    print(f"Job ID      : {job_id}")
    print(f"Chapters    : {total}")
    print(f"Workers used: {len(chunks_created)}")


if __name__ == '__main__':
    main()
