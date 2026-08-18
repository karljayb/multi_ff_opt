"""
Run the player priors pipeline, reading/writing local CSVs in ../input_data/.

Copied from karben_ff_solver/backend/scripts/run_priors.py + backend/data/build_priors.py.
build_priors.py itself is an unmodified copy; only core/storage.py is swapped
for a local-filesystem shim (see core/storage.py) in place of the source
repo's Supabase-backed one, so this pipeline can run standalone here.

Run from anywhere:
    python priors_pipeline/run_priors.py
    python priors_pipeline/run_priors.py --skip-fbref
    python priors_pipeline/run_priors.py --skip-tm

Overwrites input_data/pl_player_priors.csv and input_data/pl_team_priors.csv
in place (plus input_data/tm_id_map.csv and input_data/player_absence_history.csv,
which build_priors.py also maintains).
"""

import argparse
import sys
import uuid
import logging
from pathlib import Path

# Ensure this directory (priors_pipeline/) is on sys.path regardless of cwd,
# so `core` and `build_priors` resolve the same way they do in the source repo.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.jobs import _jobs
from build_priors import run_build_priors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-fbref", action="store_true", help="Skip fbref scraping; p60_gstart/p90_gstart carry forward unchanged.")
    parser.add_argument("--skip-tm", action="store_true", help="Skip Transfermarkt scraping; absence history unchanged.")
    args = parser.parse_args()

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "running",
        "result": None,
        "error": None,
        "logs": [],
        "failures": [],
        "cancelled": False,
        "process": None,
    }

    log.info("Starting priors pipeline (job_id=%s, skip_fbref=%s, skip_tm=%s)", job_id, args.skip_fbref, args.skip_tm)
    try:
        run_build_priors(job_id, skip_fbref=args.skip_fbref, skip_tm=args.skip_tm)
        status = _jobs[job_id]["status"]
        log.info("Pipeline finished — status: %s", status)
        if status == "failed":
            log.error("Error: %s", _jobs[job_id].get("error"))
            sys.exit(1)
    except Exception as exc:
        log.exception("Unhandled error: %s", exc)
        sys.exit(1)
