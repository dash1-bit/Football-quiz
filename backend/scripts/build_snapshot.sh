#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

USE_VENV="${USE_VENV:-0}"
MAX_PLAYERS="${MAX_PLAYERS:-50000}"

if [[ "$USE_VENV" == "1" ]]; then
  python -m venv .venv
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

python -m pip install -r requirements.txt
python backend/scripts/etl_snapshot.py --max-players "$MAX_PLAYERS"

python - <<'PY'
import json
import sqlite3
from pathlib import Path

db_path = Path("backend/data/football_quiz.sqlite")
meta_path = Path("backend/data/snapshot_meta.json")
if not db_path.exists():
    raise SystemExit("Snapshot DB not found.")
if not meta_path.exists():
    raise SystemExit("snapshot_meta.json not found.")

with sqlite3.connect(db_path) as conn:
    total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    playable = conn.execute("SELECT COUNT(*) FROM playable_players").fetchone()[0]
    clubs = conn.execute("SELECT COUNT(*) FROM clubs").fetchone()[0]

meta = json.loads(meta_path.read_text(encoding="utf-8"))
print("Snapshot validation OK")
print(f"DB: {db_path}")
print(f"Total players: {total}")
print(f"Playable players: {playable}")
print(f"Clubs: {clubs}")
print(f"Snapshot time: {meta.get('snapshot_time')}")
PY

