"""
Local-filesystem storage shim.

Drop-in replacement for the Supabase-backed core/storage.py in the source
repo (karben_ff_solver/backend), so build_priors.py can be copied here
verbatim and run unmodified. Same function signatures; "buckets" map to
local directories instead of Supabase Storage buckets.
"""

import json
from pathlib import Path

PRIORS_PIPELINE_DIR = Path(__file__).resolve().parent.parent
INPUT_DATA_DIR       = PRIORS_PIPELINE_DIR.parent / "input_data"
PIPELINE_DATA_DIR    = PRIORS_PIPELINE_DIR / "pipeline_data"
SETTINGS_DIR          = PRIORS_PIPELINE_DIR / "settings"

# Of everything build_priors.py writes under the "input-data" bucket, only these
# two are the actual priors deliverables — they land in input_data/, overwriting
# the previous versions. Everything else the pipeline writes there (tm_id_map.csv,
# player_absence_history.csv, run-tracking JSON, fbref_debug.html) is pipeline
# support data rather than a priors file, so it's kept out of input_data/ in
# pipeline_data/ instead.
INPUT_DATA_FILES = {"pl_player_priors.csv", "pl_team_priors.csv"}


def _path(bucket: str, path: str) -> Path:
    if bucket == "input-data":
        root = INPUT_DATA_DIR if path in INPUT_DATA_FILES else PIPELINE_DATA_DIR
    elif bucket == "settings":
        root = SETTINGS_DIR
    else:
        raise ValueError(f"Unknown bucket: {bucket}")
    root.mkdir(parents=True, exist_ok=True)
    return root / path


def upload(bucket: str, path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    _path(bucket, path).write_bytes(data)


def download(bucket: str, path: str) -> bytes:
    p = _path(bucket, path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    return p.read_bytes()


def upload_json(bucket: str, path: str, obj: dict) -> None:
    upload(bucket, path, json.dumps(obj).encode(), "application/json")


def download_json(bucket: str, path: str) -> dict:
    return json.loads(download(bucket, path))
