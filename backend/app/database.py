from __future__ import annotations

import re
import sqlite3
import unicodedata
from pathlib import Path


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS players(
    id INTEGER PRIMARY KEY,
    wikidata_id TEXT UNIQUE,
    name TEXT,
    name_norm TEXT,
    birth_date TEXT,
    birth_year INTEGER,
    birth_place TEXT,
    citizenship TEXT,
    citizenship_qid TEXT,
    position TEXT,
    position_group TEXT,
    height_cm INTEGER,
    popularity INTEGER DEFAULT 0
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

DROP VIEW IF EXISTS famous_players;
CREATE VIEW famous_players AS
SELECT p.*
FROM playable_players p
WHERE p.birth_year >= 1950
ORDER BY COALESCE(p.popularity, 0) DESC, p.id ASC
LIMIT 2500;
"""


def connect(db_path: Path | str, read_only: bool = False) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    if read_only:
        db_uri = f"file:{path.as_posix()}?mode=ro"
        conn = sqlite3.connect(db_uri, uri=True, check_same_thread=False)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _migrate_players_table(conn)
    _backfill_players(conn)
    create_or_replace_famous_view(conn, famous_pool_size=2500, min_birth_year=1950)
    conn.commit()


_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    no_accents = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    cleaned = _NON_ALNUM_RE.sub(" ", no_accents)
    return " ".join(cleaned.split())


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _migrate_players_table(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "players", "name_norm"):
        conn.execute("ALTER TABLE players ADD COLUMN name_norm TEXT")
    if not _column_exists(conn, "players", "popularity"):
        conn.execute("ALTER TABLE players ADD COLUMN popularity INTEGER DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_name_norm ON players(name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_popularity ON players(popularity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_players_birth_year_popularity ON players(birth_year, popularity DESC)")


def _backfill_players(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, name, name_norm, popularity FROM players",
    ).fetchall()
    updates: list[tuple[str, int, int]] = []
    for row in rows:
        normalized = str(row["name_norm"] or "").strip()
        popularity = row["popularity"]
        next_norm = normalized or _normalize_name(str(row["name"] or ""))
        next_popularity = int(popularity) if popularity is not None else 0
        if normalized != next_norm or popularity is None:
            updates.append((next_norm, next_popularity, int(row["id"])))
    if updates:
        conn.executemany(
            "UPDATE players SET name_norm = ?, popularity = ? WHERE id = ?",
            updates,
        )


def create_or_replace_famous_view(
    conn: sqlite3.Connection,
    famous_pool_size: int,
    min_birth_year: int,
) -> None:
    size = max(100, int(famous_pool_size))
    year = max(1800, int(min_birth_year))
    conn.execute("DROP VIEW IF EXISTS famous_players")
    conn.execute(
        f"""
        CREATE VIEW famous_players AS
        SELECT p.*
        FROM playable_players p
        WHERE p.birth_year >= {year}
        ORDER BY COALESCE(p.popularity, 0) DESC, p.id ASC
        LIMIT {size}
        """,
    )
