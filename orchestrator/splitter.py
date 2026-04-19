def split_chapters(chapter_files, workers):
    total = len(chapter_files)
    n = len(workers)
    base = total // n
    remainder = total % n

    chunks = []
    start = 0

    for i, worker in enumerate(workers):
        size = base + (1 if i < remainder else 0)
        end = start + size
        files = chapter_files[start:end]

        chunks.append({
            'worker': worker,
            'chapter_files': files,
            'start_chapter': start + 1,
            'end_chapter': end
        })
        start = end

    return chunks
