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
    scoring_curve: tuple[int, ...]
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


def load_settings() -> Settings:
    db_path_raw = os.getenv("DB_PATH", "backend/data/football_quiz.db")
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path

    scoring_curve_raw = os.getenv("SCORING_CURVE", "100,85,72,60,50,40,32,24,16,10")
    scoring_curve = _parse_scoring_curve(scoring_curve_raw)

    return Settings(
        db_path=db_path,
        scoring_curve=scoring_curve,
        wikidata_endpoint=os.getenv("WIKIDATA_ENDPOINT", "https://query.wikidata.org/sparql"),
        wikidata_user_agent=os.getenv(
            "WIKIDATA_USER_AGENT",
            "FootballQuizETL/1.0 (https://example.com/contact)",
        ),
    )

