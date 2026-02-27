from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    db_path: Path
    snapshot_meta_path: Path
    scoring_curve: tuple[int, ...]
    cors_allow_origins: tuple[str, ...]
    max_players: int
    full_snapshot_max_players: int
    wikidata_endpoint: str
    wikidata_user_agent: str


def _parse_scoring_curve(raw: str) -> tuple[int, ...]:
    values: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except ValueError as exc:
            raise ValueError(f"Invalid SCORING_CURVE value: {part!r}") from exc
    if not values:
        return (100, 85, 72, 60, 50, 40, 32, 24, 16, 10)
    sorted_desc = sorted(values, reverse=True)
    if sorted_desc != values:
        return tuple(sorted_desc)
    return tuple(values)


def _parse_allow_origins(raw: str) -> tuple[str, ...]:
    origins = [item.strip() for item in raw.split(",") if item.strip()]
    if not origins:
        return ("*",)
    return tuple(origins)


def _resolve_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path


def load_settings() -> Settings:
    db_path = _resolve_path(
        os.getenv("DB_PATH") or os.getenv("DATABASE_PATH", "backend/data/football_quiz.sqlite"),
    )
    snapshot_meta_default = db_path.with_name("snapshot_meta.json")
    snapshot_meta_path = _resolve_path(
        os.getenv("SNAPSHOT_META_PATH", str(snapshot_meta_default)),
    )

    scoring_curve_raw = os.getenv("SCORING_CURVE", "100,85,72,60,50,40,32,24,16,10")
    scoring_curve = _parse_scoring_curve(scoring_curve_raw)
    cors_allow_origins = _parse_allow_origins(os.getenv("CORS_ALLOW_ORIGINS", "*"))
    max_players = int(os.getenv("MAX_PLAYERS", "20000"))
    full_snapshot_max_players = int(os.getenv("FULL_SNAPSHOT_MAX_PLAYERS", "200000"))

    return Settings(
        db_path=db_path,
        snapshot_meta_path=snapshot_meta_path,
        scoring_curve=scoring_curve,
        cors_allow_origins=cors_allow_origins,
        max_players=max_players,
        full_snapshot_max_players=full_snapshot_max_players,
        wikidata_endpoint=os.getenv("WIKIDATA_ENDPOINT", "https://query.wikidata.org/sparql"),
        wikidata_user_agent=os.getenv(
            "WIKIDATA_USER_AGENT",
            "FootballQuizETL/1.0 (https://example.com/contact)",
        ),
    )
