import os
import requests

GH_TOKEN = os.environ["GITHUB_TOKEN"]

H = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}


def trigger_worker(worker, chunk):
    owner = worker["owner"]
    repo = worker["repo_name"]
    workflow = worker["workflow_file"]

    # chapter_files[0] holds the series URL (set by run.py)
    series_url = chunk["chapter_files"][0] if chunk["chapter_files"] else ""

    payload = {
        "ref": "main",
        "inputs": {
            "chunk_id": str(chunk["id"]),
            "job_id": str(chunk["job_id"]),
            "series_url": series_url,
            "start_chapter": str(chunk["start_chapter"]),
            "end_chapter": str(chunk["end_chapter"])
        }
    }

    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
    r = requests.post(url, headers=H, json=payload)
    r.raise_for_status()
    print(f"✓ Triggered {owner}/{repo} → chunk {chunk['id']} (ch{chunk['start_chapter']}-{chunk['end_chapter']})")


def get_latest_run_id(owner, repo, workflow):
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/runs"
    r = requests.get(url, headers=H, params={"per_page": 1})
    r.raise_for_status()
    runs = r.json().get("workflow_runs", [])
    return runs[0]["id"] if runs else None


def get_run_status(owner, repo, run_id):
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}"
    r = requests.get(url, headers=H)
    r.raise_for_status()
    d = r.json()
    return d["status"], d["conclusion"]


def create_release(owner, repo, tag, token):
    headers = {**H, "Authorization": f"Bearer {token}"}
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    r = requests.post(url, headers=headers, json={
        "tag_name": tag, "name": tag, "draft": False, "prerelease": True
    })
    r.raise_for_status()
    return r.json()


def upload_release_asset(upload_url, video_path, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "video/mp4"
    }
    clean_url = upload_url.replace("{?name,label}", "")
    filename = os.path.basename(video_path)

    with open(video_path, "rb") as f:
        r = requests.post(clean_url, headers=headers, params={"name": filename}, data=f)
    r.raise_for_status()

    asset_url = r.json()["browser_download_url"]
    print(f"✓ Uploaded → {asset_url}")
    return asset_url
