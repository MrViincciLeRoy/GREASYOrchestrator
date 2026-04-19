def split_chapters(total_chapters: int, workers: list, start_chapter: int = 1) -> list:
    """
    Split chapter range across available workers.

    Args:
        total_chapters: Total number of chapters to process
        workers: List of worker dicts from DB
        start_chapter: First chapter number (default 1)

    Returns:
        List of dicts: {worker, start_chapter, end_chapter, chapter_count}
    """
    n = len(workers)
    base = total_chapters // n
    remainder = total_chapters % n

    chunks = []
    current = start_chapter

    for i, worker in enumerate(workers):
        size = base + (1 if i < remainder else 0)
        if size == 0:
            continue

        end = current + size - 1
        chunks.append({
            "worker": worker,
            "start_chapter": current,
            "end_chapter": end,
            "chapter_count": size
        })
        current = end + 1

    return chunks
