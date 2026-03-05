from __future__ import annotations

import json
import math
import random
import sqlite3
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ClueCandidate:
    clue_type: str
    key: str
    text: str
    predicate: dict[str, Any]
    match_count: int


class ClueEngine:
    def __init__(
        self,
        conn: sqlite3.Connection,
        rng: random.Random | None = None,
        pool_where_sql: str = "1 = 1",
        pool_params: tuple[Any, ...] = (),
    ) -> None:
        self.conn = conn
        self.rng = rng or random.Random()
        self.pool_where_sql = pool_where_sql
        self.pool_params = pool_params

    def generate_for_player(self, player_id: int, clue_count: int = 10) -> list[dict[str, Any]]:
        target = self.conn.execute(
            f"SELECT p.* FROM playable_players p WHERE p.id = ? AND ({self.pool_where_sql})",
            (player_id, *self.pool_params),
        ).fetchone()
        if target is None:
            raise ValueError("Target player is not in playable pool.")

        playable_count = self._count_pool()
        if playable_count <= 0:
            raise ValueError("Playable pool is empty.")

        candidates: list[ClueCandidate] = []
        candidates.extend(self._attribute_candidates(target))
        candidates.extend(self._club_candidates(target["id"]))
        candidates.extend(self._national_team_candidates(target["id"]))
        candidates.extend(self._club_count_band_candidates(target["id"]))
        candidates.extend(self._club_country_candidates(target["id"]))

        filtered: dict[str, ClueCandidate] = {}
        for candidate in candidates:
            if candidate.match_count <= 0:
                continue
            if candidate.text.strip() == "":
                continue
            if candidate.key not in filtered:
                filtered[candidate.key] = candidate

        if not filtered:
            raise ValueError("No clues could be generated for this player.")

        selected = self._select_candidates(
            list(filtered.values()),
            clue_count=clue_count,
            playable_count=playable_count,
        )

        if not selected:
            raise ValueError("Failed to select clues.")

        most_specific = min(filtered.values(), key=lambda clue: clue.match_count)
        if most_specific not in selected:
            selected[-1] = most_specific

        selected.sort(key=lambda clue: clue.match_count, reverse=True)

        clues: list[dict[str, Any]] = []
        for index, clue in enumerate(selected, start=1):
            clues.append(
                {
                    "index": index,
                    "text": clue.text,
                    "predicate": clue.predicate,
                    "match_count": clue.match_count,
                    "difficulty_score": round(math.log(max(1, clue.match_count)), 4),
                }
            )
        return clues

    def _select_candidates(
        self,
        candidates: list[ClueCandidate],
        clue_count: int,
        playable_count: int,
    ) -> list[ClueCandidate]:
        schedule = [0.88, 0.74, 0.62, 0.5, 0.4, 0.31, 0.23, 0.15, 0.09, 0.04]
        self.rng.shuffle(candidates)
        remaining = candidates[:]
        selected: list[ClueCandidate] = []
        previous_count = float("inf")
        used_types: set[str] = set()

        for ratio in schedule[:clue_count]:
            target_count = max(1, int(playable_count * ratio))
            eligible = [clue for clue in remaining if clue.match_count <= previous_count]
            if not eligible:
                break

            diverse = [clue for clue in eligible if clue.clue_type not in used_types]
            pool = diverse if diverse else eligible
            pool.sort(
                key=lambda clue: (abs(clue.match_count - target_count), -clue.match_count),
            )
            window = pool[: min(5, len(pool))]
            picked = self.rng.choice(window)
            selected.append(picked)
            remaining.remove(picked)
            previous_count = picked.match_count
            used_types.add(picked.clue_type)

        if len(selected) < clue_count:
            eligible = [clue for clue in remaining if clue.match_count <= previous_count]
            eligible.sort(key=lambda clue: clue.match_count, reverse=True)
            for clue in eligible:
                selected.append(clue)
                previous_count = clue.match_count
                if len(selected) >= clue_count:
                    break

        selected.sort(key=lambda clue: clue.match_count, reverse=True)
        return selected[:clue_count]

    def _attribute_candidates(self, target: sqlite3.Row) -> list[ClueCandidate]:
        out: list[ClueCandidate] = []

        citizenship_qid = target["citizenship_qid"]
        citizenship = target["citizenship"]
        if citizenship:
            if citizenship_qid:
                match_count = self._count_pool("p.citizenship_qid = ?", (citizenship_qid,))
            else:
                match_count = self._count_pool("p.citizenship = ?", (citizenship,))
            out.append(
                ClueCandidate(
                    clue_type="citizenship",
                    key=f"citizenship:{citizenship_qid or citizenship}",
                    text=self._render(
                        [
                            "This player is eligible for {value}.",
                            "Internationally, this player is tied to {value}.",
                            "Nationality clue: {value}.",
                        ],
                        value=citizenship,
                    ),
                    predicate={
                        "type": "citizenship",
                        "citizenship": citizenship,
                        "citizenship_qid": citizenship_qid,
                    },
                    match_count=match_count,
                )
            )

        birth_year = target["birth_year"]
        if birth_year:
            decade_start = (int(birth_year) // 10) * 10
            decade_end = decade_start + 9
            out.append(
                ClueCandidate(
                    clue_type="birth_decade",
                    key=f"birth_decade:{decade_start}",
                    text=self._render(
                        [
                            "This player was born in the {decade}s.",
                            "Birth decade: {decade}s.",
                            "The player is from the {decade}s generation.",
                        ],
                        decade=decade_start,
                    ),
                    predicate={"type": "birth_decade", "start": decade_start, "end": decade_end},
                    match_count=self._count_pool(
                        "p.birth_year BETWEEN ? AND ?",
                        (decade_start, decade_end),
                    ),
                )
            )
            out.append(
                ClueCandidate(
                    clue_type="birth_year",
                    key=f"birth_year:{birth_year}",
                    text=self._render(
                        [
                            "This player was born in {year}.",
                            "Birth year clue: {year}.",
                            "The year of birth is {year}.",
                        ],
                        year=birth_year,
                    ),
                    predicate={"type": "birth_year", "year": birth_year},
                    match_count=self._count_pool("p.birth_year = ?", (birth_year,)),
                )
            )

        position_group = target["position_group"]
        if position_group:
            out.append(
                ClueCandidate(
                    clue_type="position_group",
                    key=f"position_group:{position_group}",
                    text=self._render(
                        [
                            "Primary role group: {group}.",
                            "This player belongs to the {group} position group.",
                            "On the pitch, this player is mainly {group}.",
                        ],
                        group=position_group,
                    ),
                    predicate={"type": "position_group", "value": position_group},
                    match_count=self._count_pool("p.position_group = ?", (position_group,)),
                )
            )

        position = target["position"]
        if position:
            out.append(
                ClueCandidate(
                    clue_type="position",
                    key=f"position:{position.lower()}",
                    text=self._render(
                        [
                            "Specific role clue: {position}.",
                            "This player has been listed as {position}.",
                            "Position detail: {position}.",
                        ],
                        position=position,
                    ),
                    predicate={"type": "position", "value": position},
                    match_count=self._count_pool("lower(p.position) = lower(?)", (position,)),
                )
            )

        height_cm = target["height_cm"]
        if height_cm:
            band = self._height_band(int(height_cm))
            if band is not None:
                label, min_cm, max_cm = band
                if max_cm is None:
                    count = self._count_pool("p.height_cm >= ?", (min_cm,))
                else:
                    count = self._count_pool("p.height_cm BETWEEN ? AND ?", (min_cm, max_cm))
                out.append(
                    ClueCandidate(
                        clue_type="height_band",
                        key=f"height_band:{label}",
                        text=self._render(
                            [
                                "Height clue: {label}.",
                                "This player falls in the {label} height band.",
                                "Stature hint: {label}.",
                            ],
                            label=label,
                        ),
                        predicate={
                            "type": "height_band",
                            "label": label,
                            "min_cm": min_cm,
                            "max_cm": max_cm,
                        },
                        match_count=count,
                    )
                )

        return out

    def _club_candidates(self, player_id: int) -> list[ClueCandidate]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT c.id, c.name, c.qid
            FROM player_clubs pc
            JOIN clubs c ON c.id = pc.club_id
            WHERE pc.player_id = ?
            """,
            (player_id,),
        ).fetchall()
        out: list[ClueCandidate] = []
        for row in rows:
            match_count = self._count(
                f"""
                SELECT COUNT(DISTINCT p.id)
                FROM playable_players p
                JOIN player_clubs pc ON pc.player_id = p.id
                WHERE pc.club_id = ?
                  AND ({self.pool_where_sql})
                """,
                (row["id"], *self.pool_params),
            )
            out.append(
                ClueCandidate(
                    clue_type="club",
                    key=f"club:{row['id']}",
                    text=self._render(
                        [
                            "This player has played for {club}.",
                            "Club clue: {club} was part of this player's career.",
                            "One of the clubs in this player's career is {club}.",
                        ],
                        club=row["name"],
                    ),
                    predicate={
                        "type": "club",
                        "club_id": row["id"],
                        "club_qid": row["qid"],
                        "club_name": row["name"],
                    },
                    match_count=match_count,
                )
            )
        return out

    def _national_team_candidates(self, player_id: int) -> list[ClueCandidate]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT nt.id, nt.name, nt.qid
            FROM player_national_teams pnt
            JOIN national_teams nt ON nt.id = pnt.national_team_id
            WHERE pnt.player_id = ?
            """,
            (player_id,),
        ).fetchall()
        out: list[ClueCandidate] = []
        for row in rows:
            match_count = self._count(
                f"""
                SELECT COUNT(DISTINCT p.id)
                FROM playable_players p
                JOIN player_national_teams pnt ON pnt.player_id = p.id
                WHERE pnt.national_team_id = ?
                  AND ({self.pool_where_sql})
                """,
                (row["id"], *self.pool_params),
            )
            out.append(
                ClueCandidate(
                    clue_type="national_team",
                    key=f"national_team:{row['id']}",
                    text=self._render(
                        [
                            "National team clue: {team}.",
                            "This player has represented {team}.",
                            "International clue: appearances for {team}.",
                        ],
                        team=row["name"],
                    ),
                    predicate={
                        "type": "national_team",
                        "national_team_id": row["id"],
                        "national_team_qid": row["qid"],
                        "national_team_name": row["name"],
                    },
                    match_count=match_count,
                )
            )
        return out

    def _club_count_band_candidates(self, player_id: int) -> list[ClueCandidate]:
        clubs_count = self._count(
            "SELECT COUNT(DISTINCT club_id) FROM player_clubs WHERE player_id = ?",
            (player_id,),
        )
        if clubs_count <= 0:
            return []

        if clubs_count <= 2:
            label = "1-2 clubs"
            min_count, max_count = 1, 2
        elif clubs_count <= 5:
            label = "3-5 clubs"
            min_count, max_count = 3, 5
        else:
            label = "6+ clubs"
            min_count, max_count = 6, None

        if max_count is None:
            match_count = self._count(
                f"""
                SELECT COUNT(*)
                FROM playable_players p
                WHERE ({self.pool_where_sql})
                  AND (
                    SELECT COUNT(DISTINCT pc.club_id)
                    FROM player_clubs pc
                    WHERE pc.player_id = p.id
                  ) >= ?
                """,
                (*self.pool_params, min_count),
            )
        else:
            match_count = self._count(
                f"""
                SELECT COUNT(*)
                FROM playable_players p
                WHERE ({self.pool_where_sql})
                  AND (
                    SELECT COUNT(DISTINCT pc.club_id)
                    FROM player_clubs pc
                    WHERE pc.player_id = p.id
                  ) BETWEEN ? AND ?
                """,
                (*self.pool_params, min_count, max_count),
            )

        return [
            ClueCandidate(
                clue_type="club_count_band",
                key=f"club_count_band:{label}",
                text=self._render(
                    [
                        "Career breadth clue: {label}.",
                        "This player has been at {label} in the data snapshot.",
                        "Number of clubs hint: {label}.",
                    ],
                    label=label,
                ),
                predicate={
                    "type": "club_count_band",
                    "label": label,
                    "min_count": min_count,
                    "max_count": max_count,
                },
                match_count=match_count,
            )
        ]

    def _club_country_candidates(self, player_id: int) -> list[ClueCandidate]:
        club_country_map, country_to_clubs = self._load_club_country_map()
        if not club_country_map:
            return []

        target_club_rows = self.conn.execute(
            """
            SELECT DISTINCT c.qid
            FROM player_clubs pc
            JOIN clubs c ON c.id = pc.club_id
            WHERE pc.player_id = ?
            """,
            (player_id,),
        ).fetchall()

        seen_country_qids: set[str] = set()
        out: list[ClueCandidate] = []
        for row in target_club_rows:
            club_qid = row["qid"]
            country_payload = club_country_map.get(club_qid)
            if not country_payload:
                continue
            country_qid = country_payload.get("country_qid")
            country_name = country_payload.get("country")
            if not country_qid or not country_name:
                continue
            if country_qid in seen_country_qids:
                continue
            seen_country_qids.add(country_qid)
            related_clubs = sorted(country_to_clubs.get(country_qid, set()))
            if not related_clubs:
                continue

            placeholders = ",".join("?" for _ in related_clubs)
            match_count = self._count(
                f"""
                SELECT COUNT(DISTINCT p.id)
                FROM playable_players p
                JOIN player_clubs pc ON pc.player_id = p.id
                JOIN clubs c ON c.id = pc.club_id
                WHERE c.qid IN ({placeholders})
                  AND ({self.pool_where_sql})
                """,
                (*related_clubs, *self.pool_params),
            )

            out.append(
                ClueCandidate(
                    clue_type="club_country",
                    key=f"club_country:{country_qid}",
                    text=self._render(
                        [
                            "This player has played club football in {country}.",
                            "Club-country clue: at least one club spell in {country}.",
                            "One career stop was in {country}.",
                        ],
                        country=country_name,
                    ),
                    predicate={
                        "type": "club_country",
                        "country": country_name,
                        "country_qid": country_qid,
                    },
                    match_count=match_count,
                )
            )
        return out

    def _load_club_country_map(self) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]]]:
        rows = self.conn.execute(
            "SELECT key, value_json FROM stats_cache WHERE key LIKE 'club_country::%'",
        ).fetchall()
        club_country_map: dict[str, dict[str, Any]] = {}
        country_to_clubs: dict[str, set[str]] = {}
        for row in rows:
            key = row["key"]
            club_qid = key.replace("club_country::", "", 1)
            try:
                payload = json.loads(row["value_json"] or "{}")
            except json.JSONDecodeError:
                continue
            country_qid = payload.get("country_qid")
            if not country_qid:
                continue
            club_country_map[club_qid] = payload
            country_to_clubs.setdefault(country_qid, set()).add(club_qid)
        return club_country_map, country_to_clubs

    def _height_band(self, height_cm: int) -> tuple[str, int, int | None] | None:
        if height_cm < 165:
            return ("below 165 cm", 0, 164)
        if height_cm <= 174:
            return ("165-174 cm", 165, 174)
        if height_cm <= 184:
            return ("175-184 cm", 175, 184)
        if height_cm <= 194:
            return ("185-194 cm", 185, 194)
        return ("195+ cm", 195, None)

    def _render(self, templates: list[str], **kwargs: Any) -> str:
        template = self.rng.choice(templates)
        return template.format(**kwargs)

    def _count_pool(self, extra_sql: str = "", extra_params: tuple[Any, ...] = ()) -> int:
        query = f"SELECT COUNT(*) FROM playable_players p WHERE ({self.pool_where_sql})"
        params: tuple[Any, ...] = self.pool_params
        if extra_sql:
            query += f" AND ({extra_sql})"
            params = params + extra_params
        return self._count(query, params)

    def _count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        row = self.conn.execute(sql, params).fetchone()
        if row is None:
            return 0
        return int(row[0])
