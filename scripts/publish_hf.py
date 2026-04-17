#!/usr/bin/env python3
"""Upload a date-partitioned Parquet tree to a HuggingFace dataset repo.

Timestamped layout means each run uploads ONE new date= directory;
previous days stay in the repo untouched. History is preserved for
time-series consumers.

Env: HF_TOKEN (required), HF_REPO_ID (default: edwarddgao/open-apply-jobs)
"""
import argparse, os, sys
from pathlib import Path
from datetime import datetime, timezone
from huggingface_hub import HfApi

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('data_dir', help='parquet root (contains date=YYYY-MM-DD/...)')
    ap.add_argument('--date', default=datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    ap.add_argument('--repo-id', default=os.environ.get('HF_REPO_ID', 'edwarddgao/open-apply-jobs'))
    args = ap.parse_args()

    token = os.environ.get('HF_TOKEN')
    if not token:
        sys.exit('HF_TOKEN env var required')

    src = Path(args.data_dir) / f'date={args.date}'
    if not src.exists():
        sys.exit(f'{src} does not exist')

    api = HfApi(token=token)
    api.create_repo(repo_id=args.repo_id, repo_type='dataset', exist_ok=True)
    api.upload_folder(
        folder_path=str(src),
        path_in_repo=f'data/date={args.date}',
        repo_id=args.repo_id,
        repo_type='dataset',
        commit_message=f'daily refresh {args.date}',
    )
    print(f'published → https://huggingface.co/datasets/{args.repo_id}')

if __name__ == '__main__':
    main()
