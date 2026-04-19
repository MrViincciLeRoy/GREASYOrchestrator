import os
import requests

def download_chunk(release_url, output_dir, chunk_id):
    os.makedirs(output_dir, exist_ok=True)
    filename = f"chunk_{chunk_id:03d}.mp4" if isinstance(chunk_id, int) else f"chunk_{chunk_id}.mp4"
    path = os.path.join(output_dir, filename)

    print(f"  ⬇ chunk {chunk_id}...")
    r = requests.get(release_url, stream=True)
    r.raise_for_status()

    with open(path, 'wb') as f:
        for block in r.iter_content(8192):
            f.write(block)

    print(f"  ✓ {filename}")
    return path

def download_all_chunks(chunks, output_dir):
    completed = [c for c in chunks if c['status'] == 'done' and c.get('release_url')]
    completed.sort(key=lambda c: c['start_chapter'])

    paths = []
    for chunk in completed:
        path = download_chunk(chunk['release_url'], output_dir, chunk['start_chapter'])
        paths.append(path)

    return paths
