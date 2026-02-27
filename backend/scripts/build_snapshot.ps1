param(
    [int]$MaxPlayers = 0,
    [switch]$CreateVenv
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

if ($CreateVenv) {
    python -m venv .venv
    $pythonExe = Join-Path $root ".venv\Scripts\python.exe"
} else {
    $pythonExe = "python"
}

if ($MaxPlayers -le 0) {
    if ($env:MAX_PLAYERS) {
        $MaxPlayers = [int]$env:MAX_PLAYERS
    } else {
        $MaxPlayers = 50000
    }
}

& $pythonExe -m pip install -r requirements.txt
& $pythonExe backend/scripts/etl_snapshot.py --max-players $MaxPlayers

& $pythonExe -c @"
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
"@

