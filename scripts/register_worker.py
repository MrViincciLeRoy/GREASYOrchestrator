import argparse
from orchestrator.db import register_worker, get_all_workers

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='cmd')

    add = sub.add_parser('add')
    add.add_argument('--name', required=True, help='Friendly name e.g. node-1')
    add.add_argument('--owner', required=True, help='GitHub username of the worker account')
    add.add_argument('--repo', required=True, help='Repo name e.g. GREASYvideo')
    add.add_argument('--pat-secret', required=True, help='Secret name in orchestrator that holds this worker PAT')
    add.add_argument('--workflow', default='worker_pipeline.yml')

    sub.add_parser('list')

    args = parser.parse_args()

    if args.cmd == 'add':
        worker = register_worker(
            name=args.name,
            owner=args.owner,
            repo_name=args.repo,
            pat_secret_name=args.pat_secret,
            workflow_file=args.workflow
        )
        print(f"✓ Registered worker: {worker['name']} ({worker['id']})")

    elif args.cmd == 'list':
        workers = get_all_workers()
        if not workers:
            print("No workers registered")
            return
        print(f"\n{'Name':<20} {'Owner':<20} {'Repo':<25} {'Status':<10}")
        print("-" * 75)
        for w in workers:
            print(f"{w['name']:<20} {w['owner']:<20} {w['repo_name']:<25} {w['status']:<10}")

if __name__ == '__main__':
    main()
