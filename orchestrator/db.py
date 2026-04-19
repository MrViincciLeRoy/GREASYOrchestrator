import os
import requests
from datetime import datetime

URL = os.environ['SUPABASE_URL']
KEY = os.environ['SUPABASE_KEY']

H = {
    'apikey': KEY,
    'Authorization': f'Bearer {KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

def _t(table): return f"{URL}/rest/v1/{table}"

def get_available_workers():
    r = requests.get(_t('workers'), headers=H, params={'status': 'eq.idle', 'select': '*'})
    r.raise_for_status()
    return r.json()

def get_all_workers():
    r = requests.get(_t('workers'), headers=H, params={'select': '*'})
    r.raise_for_status()
    return r.json()

def register_worker(name, owner, repo_name, pat_secret_name, workflow_file='worker_pipeline.yml'):
    data = {
        'name': name, 'owner': owner, 'repo_name': repo_name,
        'pat_secret_name': pat_secret_name, 'workflow_file': workflow_file,
        'status': 'idle', 'registered_at': datetime.utcnow().isoformat()
    }
    r = requests.post(_t('workers'), headers=H, json=data)
    r.raise_for_status()
    return r.json()[0]

def create_job(season_name, total_chapters, chapter_files):
    data = {
        'season_name': season_name, 'total_chapters': total_chapters,
        'chapter_files': chapter_files, 'status': 'pending',
        'created_at': datetime.utcnow().isoformat()
    }
    r = requests.post(_t('jobs'), headers=H, json=data)
    r.raise_for_status()
    return r.json()[0]

def create_chunk(job_id, worker_id, start_ch, end_ch, chapter_files):
    data = {
        'job_id': job_id, 'worker_id': worker_id,
        'start_chapter': start_ch, 'end_chapter': end_ch,
        'chapter_files': chapter_files, 'status': 'pending',
        'created_at': datetime.utcnow().isoformat()
    }
    r = requests.post(_t('chunks'), headers=H, json=data)
    r.raise_for_status()
    return r.json()[0]

def update_job(job_id, **kwargs):
    r = requests.patch(_t('jobs'), headers=H, json=kwargs, params={'id': f'eq.{job_id}'})
    r.raise_for_status()

def update_chunk(chunk_id, **kwargs):
    r = requests.patch(_t('chunks'), headers=H, json=kwargs, params={'id': f'eq.{chunk_id}'})
    r.raise_for_status()

def update_worker(worker_id, **kwargs):
    r = requests.patch(_t('workers'), headers=H, json=kwargs, params={'id': f'eq.{worker_id}'})
    r.raise_for_status()

def get_chunks_for_job(job_id):
    r = requests.get(_t('chunks'), headers=H, params={'job_id': f'eq.{job_id}', 'select': '*'})
    r.raise_for_status()
    return r.json()

def get_chunk(chunk_id):
    r = requests.get(_t('chunks'), headers=H, params={'id': f'eq.{chunk_id}', 'select': '*'})
    r.raise_for_status()
    return r.json()[0]
