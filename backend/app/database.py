from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS players(
    id INTEGER PRIMARY KEY,
    wikidata_id TEXT UNIQUE,
    name TEXT,
    birth_date TEXT,
    birth_year INTEGER,
    birth_place TEXT,
    citizenship TEXT,
    citizenship_qid TEXT,
    position TEXT,
    position_group TEXT,
    height_cm INTEGER
);

CREATE TABLE IF NOT EXISTS clubs(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    qid TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS player_clubs(
    player_id INTEGER,
    club_id INTEGER,
    start_year INTEGER,
    end_year INTEGER,
    FOREIGN KEY(player_id) REFERENCES players(id),
    FOREIGN KEY(club_id) REFERENCES clubs(id)
);

CREATE TABLE IF NOT EXISTS national_teams(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    qid TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS player_national_teams(
    player_id INTEGER,
    national_team_id INTEGER,
    FOREIGN KEY(player_id) REFERENCES players(id),
    FOREIGN KEY(national_team_id) REFERENCES national_teams(id)
);

CREATE TABLE IF NOT EXISTS stats_cache(
    key TEXT PRIMARY KEY,
    value_json TEXT
);

CREATE TABLE IF NOT EXISTS snapshot_meta(
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_clubs_unique
ON player_clubs(player_id, club_id, start_year, end_year);

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_national_teams_unique
ON player_national_teams(player_id, national_team_id);

CREATE INDEX IF NOT EXISTS idx_players_birth_year ON players(birth_year);
CREATE INDEX IF NOT EXISTS idx_players_citizenship_qid ON players(citizenship_qid);
CREATE INDEX IF NOT EXISTS idx_players_position_group ON players(position_group);
CREATE INDEX IF NOT EXISTS idx_player_clubs_player ON player_clubs(player_id);
CREATE INDEX IF NOT EXISTS idx_player_clubs_club ON player_clubs(club_id);
CREATE INDEX IF NOT EXISTS idx_player_national_teams_player ON player_national_teams(player_id);
CREATE INDEX IF NOT EXISTS idx_player_national_teams_team ON player_national_teams(national_team_id);

DROP VIEW IF EXISTS playable_players;
CREATE VIEW playable_players AS
SELECT p.*
FROM players p
WHERE p.position_group IS NOT NULL
  AND p.citizenship IS NOT NULL
  AND p.birth_year IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM player_clubs pc
      WHERE pc.player_id = p.id
  );
"""


def connect(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

