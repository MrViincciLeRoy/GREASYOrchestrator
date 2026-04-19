import os
import subprocess

def stitch(chunk_paths, output_path):
    concat_list = output_path.replace('.mp4', '_list.txt')

    with open(concat_list, 'w') as f:
        for path in chunk_paths:
            f.write(f"file '{os.path.abspath(path)}'\n")

    cmd = [
        'ffmpeg', '-y',
        '-f', 'concat',
        '-safe', '0',
        '-i', concat_list,
        '-c', 'copy',
        output_path
    ]

    print(f"\n🎬 Stitching {len(chunk_paths)} chunks → {output_path}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    os.remove(concat_list)

    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg failed:\n{result.stderr}")

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"✓ Done — {size_mb:.1f} MB")
    return output_path
