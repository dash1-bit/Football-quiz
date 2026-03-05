from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = PROJECT_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.append(str(BACKEND_DIR))

from app.config import load_settings  # noqa: E402
from app.database import connect, init_db  # noqa: E402


PLAYER_URI_PREFIX = "http://www.wikidata.org/entity/"
DISCOVERY_COUNTRY_PRIORITY = [
    "Q145",  # United Kingdom
    "Q183",  # Germany
    "Q38",   # Italy
    "Q29",   # Spain
    "Q142",  # France
    "Q55",   # Netherlands
    "Q39",   # Switzerland
    "Q40",   # Austria
    "Q36",   # Poland
    "Q213",  # Czech Republic
    "Q214",  # Slovakia
    "Q218",  # Romania
    "Q219",  # Bulgaria
    "Q224",  # Croatia
    "Q225",  # Bosnia and Herzegovina
    "Q227",  # Azerbaijan
    "Q228",  # Andorra
    "Q229",  # Cyprus
    "Q232",  # Kazakhstan
    "Q233",  # Malta
    "Q236",  # Montenegro
]


@dataclass
class PlayerRecord:
    qid: str
    name: str | None = None
    name_norm: str | None = None
    birth_date: str | None = None
    birth_year: int | None = None
    birth_place: str | None = None
    citizenship: str | None = None
    citizenship_qid: str | None = None
    position: str | None = None
    position_qid: str | None = None
    position_group: str | None = None
    height_cm: int | None = None
    popularity: int = 0


class SparqlClient:
    def __init__(
        self,
        endpoint: str,
        user_agent: str,
        min_interval_seconds: float = 1.0,
        max_retries: int = 6,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.endpoint = endpoint
        self.min_interval_seconds = min_interval_seconds
        self.max_retries = max_retries
        self.last_request_ts = 0.0
        self.client = httpx.Client(
            timeout=timeout_seconds,
            headers={
                "Accept": "application/sparql-results+json",
                "User-Agent": user_agent,
            },
        )

    def query(self, sparql: str) -> list[dict[str, Any]]:
        backoff_seconds = 1.0
        for attempt in range(1, self.max_retries + 1):
            elapsed = time.monotonic() - self.last_request_ts
            if elapsed < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - elapsed)

            try:
                response = self.client.get(
                    self.endpoint,
                    params={"query": sparql, "format": "json"},
                )
                self.last_request_ts = time.monotonic()
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError("SPARQL request failed after retries.") from exc
                sleep_for = backoff_seconds + random.uniform(0, 0.3)
                logging.warning(
                    "SPARQL transport error attempt=%s/%s sleep=%.2fs",
                    attempt,
                    self.max_retries,
                    sleep_for,
                )
                time.sleep(sleep_for)
                backoff_seconds *= 2
                continue

            if response.status_code in (429, 502, 503, 504):
                if attempt >= self.max_retries:
                    response.raise_for_status()
                sleep_for = backoff_seconds + random.uniform(0, 0.3)
                logging.warning(
                    "SPARQL status=%s attempt=%s/%s sleep=%.2fs",
                    response.status_code,
                    attempt,
                    self.max_retries,
                    sleep_for,
                )
                time.sleep(sleep_for)
                backoff_seconds *= 2
                continue

            response.raise_for_status()
            payload = response.json()
            return payload.get("results", {}).get("bindings", [])

        raise RuntimeError("SPARQL query failed unexpectedly.")

    def close(self) -> None:
        self.client.close()


def build_discovery_query(limit: int, offset: int, country_qid: str | None = None) -> str:
    if country_qid:
        country_filter = f"?club wdt:P17 wd:{country_qid} ."
    else:
        country_filter = """
  ?club wdt:P17 ?clubCountry .
  ?clubCountry wdt:P30 wd:Q46 .
"""
    return f"""
SELECT DISTINCT ?player WHERE {{
  ?player wdt:P31 wd:Q5 ;
          wdt:P106 wd:Q937857 ;
          wdt:P54 ?club .
  {country_filter}
}}
LIMIT {limit}
OFFSET {offset}
"""


def build_european_countries_query() -> str:
    return """
SELECT DISTINCT ?country WHERE {
  ?country wdt:P31/wdt:P279* wd:Q6256 .
  ?country wdt:P30 wd:Q46 .
}
"""


def build_player_details_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{qid}" for qid in qids)
    return f"""
SELECT DISTINCT ?player ?playerLabel ?birthDate ?birthPlaceLabel ?citizenship ?citizenshipLabel ?position ?positionLabel ?height ?sitelinks WHERE {{
  VALUES ?player {{ {values} }}
  OPTIONAL {{ ?player wdt:P569 ?birthDate . }}
  OPTIONAL {{ ?player wdt:P19 ?birthPlace . }}
  OPTIONAL {{ ?player wdt:P27 ?citizenship . }}
  OPTIONAL {{ ?player wdt:P413 ?position . }}
  OPTIONAL {{ ?player wdt:P2048 ?height . }}
  OPTIONAL {{ ?player wikibase:sitelinks ?sitelinks . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
"""


def build_player_clubs_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{qid}" for qid in qids)
    return f"""
SELECT DISTINCT ?player ?club ?clubLabel ?clubStart ?clubEnd ?clubCountry ?clubCountryLabel WHERE {{
  VALUES ?player {{ {values} }}
  ?player p:P54 ?clubStatement .
  ?clubStatement ps:P54 ?club .
  OPTIONAL {{ ?clubStatement pq:P580 ?clubStart . }}
  OPTIONAL {{ ?clubStatement pq:P582 ?clubEnd . }}
  OPTIONAL {{ ?club wdt:P17 ?clubCountry . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
"""


def build_player_national_teams_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{qid}" for qid in qids)
    return f"""
SELECT DISTINCT ?player ?nationalTeam ?nationalTeamLabel WHERE {{
  VALUES ?player {{ {values} }}
  ?player wdt:P1532 ?nationalTeam .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
"""


def discover_player_qids(
    client: SparqlClient,
    output_file: Path,
    page_size: int,
    max_players: int | None = None,
) -> list[str]:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    existing = load_qids(output_file)
    known = set(existing)
    new_count = 0

    country_bindings = client.query(build_european_countries_query())
    country_qids: list[str] = []
    for binding in country_bindings:
        country_qid = to_qid(binding_value(binding, "country"))
        if country_qid:
            country_qids.append(country_qid)
    if not country_qids:
        country_qids = [None]
    else:
        country_set = set(country_qids)
        prioritized = [qid for qid in DISCOVERY_COUNTRY_PRIORITY if qid in country_set]
        prioritized_set = set(prioritized)
        remaining = [qid for qid in country_qids if qid not in prioritized_set]
        country_qids = prioritized + remaining

    logging.info(
        "Phase 1: discovery started (existing=%s, countries=%s)",
        len(existing),
        len(country_qids),
    )
    with output_file.open("a", encoding="utf-8") as handle:
        for country_qid in country_qids:
            if max_players and len(known) >= max_players:
                break
            offset = 0
            while True:
                query = build_discovery_query(
                    limit=page_size,
                    offset=offset,
                    country_qid=country_qid,
                )
                bindings = client.query(query)
                if not bindings:
                    break

                discovered_now: list[str] = []
                for binding in bindings:
                    qid = to_qid(binding_value(binding, "player"))
                    if not qid or qid in known:
                        continue
                    known.add(qid)
                    discovered_now.append(qid)

                for qid in discovered_now:
                    handle.write(f"{qid}\n")

                new_count += len(discovered_now)
                logging.info(
                    "Phase 1 country=%s offset=%s rows=%s new=%s total=%s",
                    country_qid or "ALL_EUROPE",
                    offset,
                    len(bindings),
                    len(discovered_now),
                    len(known),
                )

                offset += page_size
                if max_players and len(known) >= max_players:
                    break

    qids = load_qids(output_file)
    if max_players:
        qids = qids[:max_players]
    logging.info("Phase 1 complete discovered=%s (new_this_run=%s)", len(qids), new_count)
    return qids


def hydrate_players(
    client: SparqlClient,
    db_path: Path,
    qids: list[str],
    batch_size: int,
    replace_snapshot: bool,
) -> dict[str, Any]:
    with connect(db_path) as conn:
        init_db(conn)
        if replace_snapshot:
            clear_snapshot_tables(conn)

        club_id_cache: dict[str, int] = {}
        team_id_cache: dict[str, int] = {}
        player_id_cache: dict[str, int] = {}

        total_batches = (len(qids) + batch_size - 1) // batch_size
        logging.info("Phase 2: hydration started (players=%s, batches=%s)", len(qids), total_batches)
        for batch_index, batch in enumerate(iter_batches(qids, batch_size), start=1):
            details_bindings = client.query(build_player_details_query(batch))
            records = parse_player_records(batch, details_bindings)
            for qid in batch:
                record = records.get(qid) or PlayerRecord(qid=qid, name=qid)
                player_id_cache[qid] = upsert_player(conn, record)

            clubs_bindings = client.query(build_player_clubs_query(batch))
            upsert_player_clubs(conn, clubs_bindings, player_id_cache, club_id_cache)

            national_bindings = client.query(build_player_national_teams_query(batch))
            upsert_player_national_teams(conn, national_bindings, player_id_cache, team_id_cache)

            conn.commit()
            logging.info(
                "Phase 2 batch %s/%s complete (batch_size=%s)",
                batch_index,
                total_batches,
                len(batch),
            )

        compute_stats_cache(conn)
        snapshot_time = datetime.now(UTC).isoformat()
        set_snapshot_meta(conn, "snapshot_generated_at", snapshot_time)
        set_snapshot_meta(conn, "discovered_qids_count", str(len(qids)))
        hydrated_players = scalar(conn, "SELECT COUNT(*) FROM players")
        playable_players = scalar(conn, "SELECT COUNT(*) FROM playable_players")
        clubs_count = scalar(conn, "SELECT COUNT(*) FROM clubs")
        national_teams_count = scalar(conn, "SELECT COUNT(*) FROM national_teams")
        set_snapshot_meta(conn, "hydrated_players_count", str(hydrated_players))
        set_snapshot_meta(conn, "playable_players_count", str(playable_players))
        conn.commit()
        logging.info(
            "Phase 2 complete hydrated=%s playable=%s clubs=%s national_teams=%s",
            hydrated_players,
            playable_players,
            clubs_count,
            national_teams_count,
        )
        return {
            "snapshot_time": snapshot_time,
            "hydrated_players_count": hydrated_players,
            "playable_players_count": playable_players,
            "clubs_count": clubs_count,
            "national_teams_count": national_teams_count,
            "discovered_qids_count": len(qids),
            "db_path": str(db_path),
        }


def parse_player_records(
    batch_qids: list[str],
    bindings: list[dict[str, Any]],
) -> dict[str, PlayerRecord]:
    records: dict[str, PlayerRecord] = {qid: PlayerRecord(qid=qid) for qid in batch_qids}
    for binding in bindings:
        qid = to_qid(binding_value(binding, "player"))
        if not qid:
            continue
        record = records.setdefault(qid, PlayerRecord(qid=qid))

        label = binding_value(binding, "playerLabel")
        if label and not record.name:
            record.name = label

        birth_date = normalize_date(binding_value(binding, "birthDate"))
        if birth_date and not record.birth_date:
            record.birth_date = birth_date
            record.birth_year = parse_year(birth_date)

        birth_place = binding_value(binding, "birthPlaceLabel")
        if birth_place and not record.birth_place:
            record.birth_place = birth_place

        citizenship_qid = to_qid(binding_value(binding, "citizenship"))
        citizenship_label = binding_value(binding, "citizenshipLabel")
        if citizenship_label and not record.citizenship:
            record.citizenship = citizenship_label
            record.citizenship_qid = citizenship_qid

        position_qid = to_qid(binding_value(binding, "position"))
        position_label = binding_value(binding, "positionLabel")
        if position_label and not record.position:
            record.position = position_label
            record.position_qid = position_qid
            record.position_group = map_position_group(position_label)

        height_raw = binding_value(binding, "height")
        height_cm = parse_height_cm(height_raw)
        if height_cm and not record.height_cm:
            record.height_cm = height_cm

        sitelinks_raw = binding_value(binding, "sitelinks")
        sitelinks = parse_non_negative_int(sitelinks_raw)
        if sitelinks is not None:
            record.popularity = max(record.popularity, sitelinks)

    for record in records.values():
        if not record.name:
            record.name = record.qid
        record.name_norm = normalize_name(record.name)
        if record.position and not record.position_group:
            record.position_group = map_position_group(record.position)
    return records


def upsert_player(conn: Any, record: PlayerRecord) -> int:
    conn.execute(
        """
        INSERT INTO players(
            wikidata_id, name, name_norm, birth_date, birth_year, birth_place,
            citizenship, citizenship_qid, position, position_group, height_cm, popularity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wikidata_id) DO UPDATE SET
            name = excluded.name,
            name_norm = excluded.name_norm,
            birth_date = excluded.birth_date,
            birth_year = excluded.birth_year,
            birth_place = excluded.birth_place,
            citizenship = excluded.citizenship,
            citizenship_qid = excluded.citizenship_qid,
            position = excluded.position,
            position_group = excluded.position_group,
            height_cm = excluded.height_cm,
            popularity = excluded.popularity
        """,
        (
            record.qid,
            record.name,
            record.name_norm,
            record.birth_date,
            record.birth_year,
            record.birth_place,
            record.citizenship,
            record.citizenship_qid,
            record.position,
            record.position_group,
            record.height_cm,
            record.popularity,
        ),
    )
    row = conn.execute(
        "SELECT id FROM players WHERE wikidata_id = ?",
        (record.qid,),
    ).fetchone()
    return int(row["id"])


def upsert_player_clubs(
    conn: Any,
    bindings: list[dict[str, Any]],
    player_id_cache: dict[str, int],
    club_id_cache: dict[str, int],
) -> None:
    for binding in bindings:
        player_qid = to_qid(binding_value(binding, "player"))
        club_qid = to_qid(binding_value(binding, "club"))
        club_name = binding_value(binding, "clubLabel")
        if not player_qid or not club_qid or not club_name:
            continue
        player_id = player_id_cache.get(player_qid)
        if player_id is None:
            continue

        club_id = get_or_create_entity(
            conn=conn,
            cache=club_id_cache,
            table_name="clubs",
            qid=club_qid,
            label=club_name,
        )
        if club_id is None:
            continue

        start_year = parse_year(binding_value(binding, "clubStart"))
        end_year = parse_year(binding_value(binding, "clubEnd"))
        conn.execute(
            """
            INSERT OR IGNORE INTO player_clubs(player_id, club_id, start_year, end_year)
            VALUES (?, ?, ?, ?)
            """,
            (player_id, club_id, start_year, end_year),
        )

        country_qid = to_qid(binding_value(binding, "clubCountry"))
        country_label = binding_value(binding, "clubCountryLabel")
        if country_qid and country_label:
            conn.execute(
                """
                INSERT INTO stats_cache(key, value_json)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
                """,
                (
                    f"club_country::{club_qid}",
                    json.dumps(
                        {"country": country_label, "country_qid": country_qid},
                        ensure_ascii=True,
                    ),
                ),
            )


def upsert_player_national_teams(
    conn: Any,
    bindings: list[dict[str, Any]],
    player_id_cache: dict[str, int],
    team_id_cache: dict[str, int],
) -> None:
    for binding in bindings:
        player_qid = to_qid(binding_value(binding, "player"))
        team_qid = to_qid(binding_value(binding, "nationalTeam"))
        team_name = binding_value(binding, "nationalTeamLabel")
        if not player_qid or not team_qid or not team_name:
            continue
        player_id = player_id_cache.get(player_qid)
        if player_id is None:
            continue
        team_id = get_or_create_entity(
            conn=conn,
            cache=team_id_cache,
            table_name="national_teams",
            qid=team_qid,
            label=team_name,
        )
        if team_id is None:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO player_national_teams(player_id, national_team_id)
            VALUES (?, ?)
            """,
            (player_id, team_id),
        )


def get_or_create_entity(
    conn: Any,
    cache: dict[str, int],
    table_name: str,
    qid: str,
    label: str,
) -> int | None:
    cached = cache.get(qid)
    if cached is not None:
        return cached

    row = conn.execute(
        f"SELECT id FROM {table_name} WHERE qid = ?",
        (qid,),
    ).fetchone()
    if row:
        entity_id = int(row["id"])
        cache[qid] = entity_id
        return entity_id

    try:
        conn.execute(
            f"INSERT INTO {table_name}(name, qid) VALUES (?, ?)",
            (label, qid),
        )
    except Exception:
        # Name uniqueness can clash for homonymous clubs/teams.
        fallback_label = f"{label} ({qid})"
        conn.execute(
            f"INSERT OR IGNORE INTO {table_name}(name, qid) VALUES (?, ?)",
            (fallback_label, qid),
        )

    row = conn.execute(
        f"SELECT id FROM {table_name} WHERE qid = ?",
        (qid,),
    ).fetchone()
    if not row:
        return None
    entity_id = int(row["id"])
    cache[qid] = entity_id
    return entity_id


def compute_stats_cache(conn: Any) -> None:
    playable_count = scalar(conn, "SELECT COUNT(*) FROM playable_players")
    persist_stat(conn, "distribution::playable_count", {"count": playable_count})

    citizenship_rows = conn.execute(
        """
        SELECT citizenship, COUNT(*) AS cnt
        FROM playable_players
        GROUP BY citizenship
        ORDER BY cnt DESC
        LIMIT 2000
        """
    ).fetchall()
    persist_stat(
        conn,
        "distribution::citizenship",
        {row["citizenship"]: int(row["cnt"]) for row in citizenship_rows if row["citizenship"]},
    )

    position_rows = conn.execute(
        """
        SELECT position_group, COUNT(*) AS cnt
        FROM playable_players
        GROUP BY position_group
        ORDER BY cnt DESC
        """
    ).fetchall()
    persist_stat(
        conn,
        "distribution::position_group",
        {row["position_group"]: int(row["cnt"]) for row in position_rows if row["position_group"]},
    )

    decade_rows = conn.execute(
        """
        SELECT (birth_year / 10) * 10 AS decade, COUNT(*) AS cnt
        FROM playable_players
        GROUP BY decade
        ORDER BY cnt DESC
        """
    ).fetchall()
    persist_stat(
        conn,
        "distribution::birth_decade",
        {str(int(row["decade"])): int(row["cnt"]) for row in decade_rows},
    )

    club_band_rows = conn.execute(
        """
        SELECT
          CASE
            WHEN club_count BETWEEN 1 AND 2 THEN '1-2 clubs'
            WHEN club_count BETWEEN 3 AND 5 THEN '3-5 clubs'
            ELSE '6+ clubs'
          END AS band,
          COUNT(*) AS cnt
        FROM (
          SELECT p.id, COUNT(DISTINCT pc.club_id) AS club_count
          FROM playable_players p
          JOIN player_clubs pc ON pc.player_id = p.id
          GROUP BY p.id
        ) t
        GROUP BY band
        """
    ).fetchall()
    persist_stat(
        conn,
        "distribution::club_count_band",
        {row["band"]: int(row["cnt"]) for row in club_band_rows},
    )

    quantiles = compute_popularity_quantiles(conn)
    persist_stat(conn, "difficulty::quantiles", quantiles)


def persist_stat(conn: Any, key: str, value: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO stats_cache(key, value_json)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
        """,
        (key, json.dumps(value, ensure_ascii=True)),
    )


def compute_popularity_quantiles(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT popularity FROM playable_players ORDER BY popularity DESC, id ASC",
    ).fetchall()
    if not rows:
        return {
            "playable_count": 0,
            "easy_min_popularity": 0,
            "normal_min_popularity": 0,
            "insane_max_popularity": 0,
            "easy_count_target": 0,
            "normal_count_target": 0,
            "insane_count_target": 0,
        }

    popularity = [int(row["popularity"] or 0) for row in rows]
    total = len(popularity)

    easy_count = max(1, int(math.ceil(total * 0.2)))
    normal_count = max(1, int(math.ceil(total * 0.5)))
    insane_count = max(1, int(math.ceil(total * 0.3)))

    easy_min = popularity[easy_count - 1]
    normal_min = popularity[normal_count - 1]
    insane_start_idx = max(0, total - insane_count)
    insane_max = popularity[insane_start_idx]

    return {
        "playable_count": total,
        "easy_min_popularity": int(easy_min),
        "normal_min_popularity": int(normal_min),
        "insane_max_popularity": int(insane_max),
        "easy_count_target": easy_count,
        "normal_count_target": normal_count,
        "insane_count_target": insane_count,
    }


def clear_snapshot_tables(conn: Any) -> None:
    conn.execute("DELETE FROM player_national_teams")
    conn.execute("DELETE FROM national_teams")
    conn.execute("DELETE FROM player_clubs")
    conn.execute("DELETE FROM clubs")
    conn.execute("DELETE FROM players")
    conn.execute("DELETE FROM stats_cache")
    conn.execute("DELETE FROM snapshot_meta")
    conn.commit()


def set_snapshot_meta(conn: Any, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO snapshot_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def write_snapshot_meta_json(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def load_qids(path: Path) -> list[str]:
    if not path.exists():
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        qid = raw.strip()
        if not qid or qid in seen:
            continue
        seen.add(qid)
        ordered.append(qid)
    return ordered


def binding_value(binding: dict[str, Any], field: str) -> str | None:
    obj = binding.get(field)
    if not obj:
        return None
    return obj.get("value")


def to_qid(uri: str | None) -> str | None:
    if not uri:
        return None
    if uri.startswith(PLAYER_URI_PREFIX):
        return uri.rsplit("/", 1)[-1]
    return uri if uri.startswith("Q") else None


_YEAR_RE = re.compile(r"[-+]?\d{1,6}")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")


def parse_year(raw: str | None) -> int | None:
    if not raw:
        return None
    match = _YEAR_RE.match(raw)
    if not match:
        return None
    try:
        year = int(match.group(0))
    except ValueError:
        return None
    if year < 0:
        return None
    if year > 3000:
        return None
    return year


def parse_non_negative_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def normalize_name(value: str | None) -> str:
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


def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    clean = raw.lstrip("+")
    if "T" in clean:
        clean = clean.split("T", 1)[0]
    if len(clean) >= 10:
        return clean[:10]
    return clean


def parse_height_cm(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    if value < 3.5:
        return int(round(value * 100))
    if value <= 300:
        return int(round(value))
    return None


def map_position_group(position_label: str | None) -> str | None:
    if not position_label:
        return None
    text = position_label.lower()
    if "goalkeeper" in text:
        return "GK"
    if (
        "defender" in text
        or "fullback" in text
        or "centre-back" in text
        or "center-back" in text
        or "wing-back" in text
    ):
        return "DEF"
    if "midfielder" in text or "winger" in text:
        return "MID"
    if "forward" in text or "striker" in text or "attacker" in text:
        return "FWD"
    return "OTHER"


def iter_batches(items: list[str], batch_size: int) -> Iterable[list[str]]:
    for idx in range(0, len(items), batch_size):
        yield items[idx : idx + batch_size]


def scalar(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return 0
    return int(row[0])


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Build a local Wikidata snapshot for Football Quiz.")
    parser.add_argument(
        "--db-path",
        default=str(settings.db_path),
        help="SQLite output path.",
    )
    parser.add_argument(
        "--discovery-file",
        default=str(PROJECT_ROOT / "backend" / "data" / "discovered_qids.txt"),
        help="File for discovered player QIDs.",
    )
    parser.add_argument(
        "--snapshot-meta-path",
        default=str(settings.snapshot_meta_path),
        help="JSON metadata output path.",
    )
    parser.add_argument("--page-size", type=int, default=1000, help="Phase 1 LIMIT/OFFSET page size.")
    parser.add_argument("--batch-size", type=int, default=150, help="Phase 2 batch size.")
    parser.add_argument(
        "--max-players",
        type=int,
        default=settings.max_players,
        help="Discovery/hydration limit.",
    )
    parser.add_argument(
        "--full-snapshot",
        action="store_true",
        help="Use FULL_SNAPSHOT_MAX_PLAYERS limit from environment.",
    )
    parser.add_argument(
        "--skip-discovery",
        action="store_true",
        help="Skip phase 1 and use existing discovery file.",
    )
    parser.add_argument(
        "--skip-hydration",
        action="store_true",
        help="Skip phase 2 data hydration.",
    )
    parser.add_argument(
        "--no-replace",
        action="store_true",
        help="Append/update snapshot instead of clearing data tables first.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()

    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    discovery_file = Path(args.discovery_file)
    if not discovery_file.is_absolute():
        discovery_file = PROJECT_ROOT / discovery_file
    snapshot_meta_path = Path(args.snapshot_meta_path)
    if not snapshot_meta_path.is_absolute():
        snapshot_meta_path = PROJECT_ROOT / snapshot_meta_path

    max_players_limit = args.max_players
    if args.full_snapshot:
        max_players_limit = settings.full_snapshot_max_players

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info("Using endpoint=%s", settings.wikidata_endpoint)
    logging.info("Using db_path=%s", db_path)
    logging.info("Using discovery_file=%s", discovery_file)
    logging.info("Using snapshot_meta_path=%s", snapshot_meta_path)
    logging.info("Using max_players_limit=%s", max_players_limit)

    client = SparqlClient(
        endpoint=settings.wikidata_endpoint,
        user_agent=settings.wikidata_user_agent,
    )
    try:
        if args.skip_discovery:
            qids = load_qids(discovery_file)
            if max_players_limit:
                qids = qids[:max_players_limit]
            logging.info("Phase 1 skipped, loaded qids=%s from file.", len(qids))
        else:
            qids = discover_player_qids(
                client=client,
                output_file=discovery_file,
                page_size=args.page_size,
                max_players=max_players_limit,
            )

        if args.skip_hydration:
            logging.info("Phase 2 skipped.")
            return

        if not qids:
            raise RuntimeError("No player QIDs found. Cannot hydrate empty dataset.")

        metadata = hydrate_players(
            client=client,
            db_path=db_path,
            qids=qids,
            batch_size=args.batch_size,
            replace_snapshot=not args.no_replace,
        )
        write_snapshot_meta_json(snapshot_meta_path, metadata)
        logging.info("Snapshot metadata written to %s", snapshot_meta_path)
    finally:
        client.close()


if __name__ == "__main__":
    main()
