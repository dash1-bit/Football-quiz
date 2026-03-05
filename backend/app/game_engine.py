from __future__ import annotations

import json
import math
import random
import re
import sqlite3
import threading
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from .clue_engine import ClueEngine
from .database import connect, init_db


DIFFICULTY_MODES = {"EASY", "NORMAL", "HARD", "INSANE"}
MAX_PLAYERS_PER_LOBBY = 10
MAX_CLUES_PER_GAME = 10
ROUND_SECONDS_TOTAL = 60


class GameError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class DifficultyPool:
    difficulty: str
    where_sql: str
    params: tuple[Any, ...]
    candidate_count: int
    thresholds: dict[str, Any]


@dataclass
class PlayerSession:
    token: str
    name: str
    score: int = 0
    has_solved: bool = False
    solved_on_clue: int | None = None
    last_submitted_round: int = 0
    last_guess: str | None = None


@dataclass
class LobbyState:
    id: str
    host_token: str
    difficulty: str
    max_players: int
    share_path: str
    players: dict[str, PlayerSession]
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    started: bool = False
    game_over: bool = False
    target_player_id: int | None = None
    target_name: str | None = None
    clues: list[dict[str, Any]] = field(default_factory=list)
    clue_index: int = 0
    round_start_ts: float = 0.0
    round_seconds_total: int = ROUND_SECONDS_TOTAL
    recent_target_ids: list[int] = field(default_factory=list)


class GameManager:
    def __init__(self, db_path: Path, scoring_curve: tuple[int, ...]) -> None:
        self.db_path = Path(db_path)
        self.scoring_curve = scoring_curve
        self.lobbies: dict[str, LobbyState] = {}
        self.lock = threading.Lock()
        self.rng = random.Random()
        self._ensure_db()

    def create_lobby(self, host_name: str, difficulty: str) -> dict[str, Any]:
        clean_name = self._clean_name(host_name)
        difficulty_mode = self._normalize_difficulty(difficulty)
        with self.lock:
            host_token = self._make_token()
            lobby_id = self._make_lobby_id()
            player = PlayerSession(token=host_token, name=clean_name)
            lobby = LobbyState(
                id=lobby_id,
                host_token=host_token,
                difficulty=difficulty_mode,
                max_players=MAX_PLAYERS_PER_LOBBY,
                share_path=f"/?lobby={lobby_id}",
                players={host_token: player},
            )
            self.lobbies[lobby_id] = lobby
            return {
                "lobby_id": lobby_id,
                "host_token": host_token,
                "share_url": lobby.share_path,
                "state": self._serialize_lobby(lobby, host_token),
            }

    def join_lobby(self, lobby_id: str, player_name: str) -> dict[str, Any]:
        clean_name = self._clean_name(player_name)
        with self.lock:
            lobby = self._get_lobby(lobby_id)
            if lobby.started:
                raise GameError("Game already started. Joining is closed.", status_code=409)
            if len(lobby.players) >= lobby.max_players:
                raise GameError("Lobby is full.", status_code=409)

            token = self._make_token()
            lobby.players[token] = PlayerSession(token=token, name=clean_name)
            return {
                "player_token": token,
                "state": self._serialize_lobby(lobby, token),
            }

    def get_lobby_state(self, lobby_id: str, token: str | None = None) -> dict[str, Any]:
        with self.lock:
            lobby = self._get_lobby(lobby_id)
            return self._serialize_lobby(lobby, token)

    def start_game(self, lobby_id: str, host_token: str) -> dict[str, Any]:
        with self.lock:
            lobby = self._get_lobby(lobby_id)
            self._ensure_host(lobby, host_token)
            if len(lobby.players) < 1:
                raise GameError("At least one player is required to start.")
            if lobby.started and not lobby.game_over:
                raise GameError("Game already in progress.", status_code=409)

            with connect(self.db_path, read_only=True) as conn:
                pool = self._build_pool(conn, lobby.difficulty)
                target_id, target_name = self._pick_target_player(
                    conn,
                    pool=pool,
                    recent_targets=lobby.recent_target_ids,
                )
                clue_engine = ClueEngine(
                    conn=conn,
                    rng=random.Random(self.rng.random()),
                    pool_where_sql=pool.where_sql,
                    pool_params=pool.params,
                )
                clues = clue_engine.generate_for_player(target_id, clue_count=MAX_CLUES_PER_GAME)

            lobby.started = True
            lobby.game_over = False
            lobby.target_player_id = target_id
            lobby.target_name = target_name
            lobby.clues = clues[:MAX_CLUES_PER_GAME]
            lobby.clue_index = 1
            lobby.round_start_ts = time.time()
            for player in lobby.players.values():
                player.score = 0
                player.has_solved = False
                player.solved_on_clue = None
                player.last_submitted_round = 0
                player.last_guess = None
            lobby.recent_target_ids.append(target_id)
            if len(lobby.recent_target_ids) > 20:
                lobby.recent_target_ids = lobby.recent_target_ids[-20:]
            return self._serialize_game(lobby, host_token)

    def get_game_state(self, lobby_id: str, token: str | None = None) -> dict[str, Any]:
        with self.lock:
            lobby = self._get_lobby(lobby_id)
            self._auto_advance_if_needed(lobby, now_ts=time.time())
            return self._serialize_game(lobby, token)

    def submit_guess(self, lobby_id: str, player_token: str, guess_text: str) -> dict[str, Any]:
        clean_guess = guess_text.strip()
        if not clean_guess:
            raise GameError("Guess cannot be empty.")

        with self.lock:
            lobby = self._get_lobby(lobby_id)
            player = self._ensure_player(lobby, player_token)
            if not lobby.started:
                raise GameError("Game has not started yet.", status_code=409)

            self._auto_advance_if_needed(lobby, now_ts=time.time())

            if lobby.game_over:
                return {
                    "accepted": False,
                    "correct": False,
                    "has_solved": player.has_solved,
                    "points_awarded": 0,
                    "reason": "Game is over.",
                }
            if player.has_solved:
                return {
                    "accepted": False,
                    "correct": True,
                    "has_solved": True,
                    "points_awarded": 0,
                    "reason": "Already solved.",
                }
            if player.last_submitted_round == lobby.clue_index:
                return {
                    "accepted": False,
                    "correct": False,
                    "has_solved": False,
                    "points_awarded": 0,
                    "reason": "Already submitted for this round.",
                }

            is_correct = self._is_correct_guess(clean_guess, lobby.target_name or "")
            points_awarded = 0
            player.last_submitted_round = lobby.clue_index
            player.last_guess = clean_guess

            if is_correct:
                points_awarded = self._points_for_clue(lobby.clue_index)
                player.score += points_awarded
                player.has_solved = True
                player.solved_on_clue = lobby.clue_index

            self._auto_advance_if_needed(lobby, now_ts=time.time())

            return {
                "accepted": True,
                "correct": is_correct,
                "has_solved": player.has_solved,
                "points_awarded": points_awarded,
            }

    def advance_if_needed(self, lobby_id: str, host_token: str | None = None) -> dict[str, Any]:
        with self.lock:
            lobby = self._get_lobby(lobby_id)
            if host_token:
                self._ensure_host(lobby, host_token)
            self._auto_advance_if_needed(lobby, now_ts=time.time())
            return self._serialize_game(lobby, host_token)

    def autocomplete(self, lobby_id: str, query: str, limit: int = 10) -> list[dict[str, str]]:
        normalized = _normalize_name(query)
        if len(normalized) < 3:
            return []
        limit = max(1, min(10, int(limit)))

        with self.lock:
            lobby = self._get_lobby(lobby_id)
            with connect(self.db_path, read_only=True) as conn:
                pool = self._build_pool(conn, lobby.difficulty)
                has_name_norm = self._has_player_column(conn, "name_norm")
                has_popularity = self._has_player_column(conn, "popularity")
                name_norm_expr = self._name_norm_expr("p", has_name_norm)
                popularity_expr = self._popularity_expr("p", has_popularity)
                rows = conn.execute(
                    f"""
                    SELECT p.name, p.wikidata_id
                    FROM playable_players p
                    WHERE ({pool.where_sql})
                      AND ({name_norm_expr} LIKE ? OR {name_norm_expr} LIKE ?)
                    ORDER BY
                      CASE WHEN {name_norm_expr} LIKE ? THEN 0 ELSE 1 END,
                      {popularity_expr} DESC,
                      p.name ASC
                    LIMIT ?
                    """,
                    (
                        *pool.params,
                        f"{normalized}%",
                        f"%{normalized}%",
                        f"{normalized}%",
                        limit,
                    ),
                ).fetchall()
        return [
            {"name": str(row["name"]), "wikidata_id": str(row["wikidata_id"])}
            for row in rows
        ]

    def create_game(self, player_name: str) -> dict[str, Any]:
        payload = self.create_lobby(host_name=player_name, difficulty="NORMAL")
        lobby_id = payload["lobby_id"]
        token = payload["host_token"]
        return {
            "game_id": lobby_id,
            "player_id": token,
            "state": self.get_game_state(lobby_id, token),
        }

    def join_game(self, game_id: str, player_name: str) -> dict[str, Any]:
        payload = self.join_lobby(lobby_id=game_id, player_name=player_name)
        token = payload["player_token"]
        return {
            "game_id": game_id,
            "player_id": token,
            "state": self.get_game_state(game_id, token),
        }

    def start_round(self, game_id: str, requester_player_id: str) -> dict[str, Any]:
        state = self.start_game(game_id, requester_player_id)
        return self._legacy_state(state)

    def get_state(self, game_id: str) -> dict[str, Any]:
        state = self.get_game_state(game_id)
        return self._legacy_state(state)

    def next_clue(self, game_id: str, player_id: str) -> dict[str, Any]:
        with self.lock:
            lobby = self._get_lobby(game_id)
            self._ensure_player(lobby, player_id)
            if not lobby.started:
                raise GameError("Game has not started yet.", status_code=409)
            if lobby.game_over:
                raise GameError("Game is over.", status_code=409)
            if lobby.clue_index >= min(MAX_CLUES_PER_GAME, len(lobby.clues)):
                lobby.game_over = True
            else:
                lobby.clue_index += 1
                lobby.round_start_ts = time.time()
            return self._legacy_state(self._serialize_game(lobby, player_id))

    def _legacy_state(self, state: dict[str, Any]) -> dict[str, Any]:
        clue_index = int(state.get("clue_index") or 0)
        all_clues = state.get("all_clues") or []
        revealed_clues = all_clues[: clue_index] if clue_index > 0 else []
        return {
            "game_id": state.get("lobby_id"),
            "status": "round_finished" if state.get("game_over") else ("in_round" if state.get("started") else "lobby"),
            "host_player_id": state.get("host_token"),
            "players": [
                {"player_id": player["token"], "name": player["name"]}
                for player in state.get("players", [])
            ],
            "scoreboard": [
                {
                    "player_id": row["token"],
                    "name": row["name"],
                    "score": row["score"],
                }
                for row in state.get("scoreboard", [])
            ],
            "round": (
                None
                if not state.get("started")
                else {
                    "round_number": clue_index,
                    "status": "finished" if state.get("game_over") else "active",
                    "revealed_count": len(revealed_clues),
                    "total_clues": len(all_clues),
                    "revealed_clues": revealed_clues,
                    "winner_player_id": None,
                    "winner_name": None,
                    "answer": state.get("answer_name"),
                }
            ),
        }

    def _serialize_lobby(self, lobby: LobbyState, token: str | None) -> dict[str, Any]:
        players = [
            {"name": player.name}
            for player in sorted(lobby.players.values(), key=lambda part: part.name.lower())
        ]
        return {
            "lobby_id": lobby.id,
            "started": lobby.started,
            "game_over": lobby.game_over,
            "difficulty": lobby.difficulty.lower(),
            "max_players": lobby.max_players,
            "player_count": len(lobby.players),
            "players": players,
            "share_url": lobby.share_path,
            "is_host": token is not None and token == lobby.host_token,
            "can_start": token is not None and token == lobby.host_token and len(lobby.players) >= 1,
            "rule_text": "No one can join after the game starts.",
        }

    def _serialize_game(self, lobby: LobbyState, token: str | None) -> dict[str, Any]:
        now_ts = time.time()
        round_seconds_left = 0
        if lobby.started and not lobby.game_over and lobby.round_start_ts > 0:
            elapsed = max(0.0, now_ts - lobby.round_start_ts)
            round_seconds_left = max(0, int(math.ceil(lobby.round_seconds_total - elapsed)))

        players_sorted = sorted(lobby.players.values(), key=lambda part: part.name.lower())
        players_payload = [
            {
                "token": player.token,
                "name": player.name,
                "score": player.score,
                "has_solved": player.has_solved,
                "has_submitted_this_round": (
                    bool(lobby.started)
                    and not lobby.game_over
                    and player.last_submitted_round == lobby.clue_index
                ),
            }
            for player in players_sorted
        ]
        scoreboard = sorted(
            players_payload,
            key=lambda row: (-int(row["score"]), str(row["name"]).lower()),
        )

        me = lobby.players.get(token) if token else None
        current_clue_text = None
        if lobby.started and not lobby.game_over and 1 <= lobby.clue_index <= len(lobby.clues):
            current_clue_text = lobby.clues[lobby.clue_index - 1]["text"]

        return {
            "lobby_id": lobby.id,
            "host_token": lobby.host_token,
            "started": lobby.started,
            "game_over": lobby.game_over,
            "difficulty": lobby.difficulty.lower(),
            "clue_index": lobby.clue_index,
            "current_clue_text": current_clue_text,
            "round_seconds_total": lobby.round_seconds_total,
            "round_seconds_left": round_seconds_left,
            "players": players_payload,
            "scoreboard": scoreboard,
            "answer_name": lobby.target_name if lobby.game_over else None,
            "you_token": token,
            "you_name": me.name if me else None,
            "you_has_solved": me.has_solved if me else False,
            "you_has_submitted_this_round": (
                bool(me)
                and lobby.started
                and not lobby.game_over
                and me.last_submitted_round == lobby.clue_index
            ),
            "can_guess": bool(me) and lobby.started and not lobby.game_over and (not me.has_solved) and (
                me.last_submitted_round != lobby.clue_index
            ),
            "all_clues": lobby.clues,
        }

    def _auto_advance_if_needed(self, lobby: LobbyState, now_ts: float) -> None:
        if not lobby.started or lobby.game_over:
            return
        if not lobby.clues:
            lobby.game_over = True
            return
        if lobby.clue_index < 1:
            lobby.clue_index = 1
        if lobby.clue_index > min(MAX_CLUES_PER_GAME, len(lobby.clues)):
            lobby.game_over = True
            return

        unresolved = [player for player in lobby.players.values() if not player.has_solved]
        if not unresolved:
            lobby.game_over = True
            return

        elapsed = max(0.0, now_ts - lobby.round_start_ts)
        timed_out = elapsed >= lobby.round_seconds_total
        all_submitted = all(player.last_submitted_round == lobby.clue_index for player in unresolved)

        if not timed_out and not all_submitted:
            return

        if lobby.clue_index >= min(MAX_CLUES_PER_GAME, len(lobby.clues)):
            lobby.game_over = True
            return

        lobby.clue_index += 1
        lobby.round_start_ts = now_ts

    def _build_pool(self, conn: sqlite3.Connection, difficulty: str) -> DifficultyPool:
        thresholds = self._load_or_compute_thresholds(conn)
        mode = self._normalize_difficulty(difficulty)
        has_popularity = self._has_player_column(conn, "popularity")
        popularity_expr = self._popularity_expr("p", has_popularity)
        if mode == "EASY":
            where_sql = f"{popularity_expr} >= ?"
            params = (int(thresholds["easy_min_popularity"]),)
        elif mode == "NORMAL":
            where_sql = f"{popularity_expr} >= ?"
            params = (int(thresholds["normal_min_popularity"]),)
        elif mode == "INSANE":
            where_sql = f"{popularity_expr} <= ?"
            params = (int(thresholds["insane_max_popularity"]),)
        else:
            where_sql = "1 = 1"
            params = ()

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM playable_players p WHERE ({where_sql})",
            params,
        ).fetchone()
        candidate_count = int(count_row[0]) if count_row else 0
        if candidate_count <= 0:
            raise GameError("No candidates available for this difficulty.", status_code=409)
        return DifficultyPool(
            difficulty=mode,
            where_sql=where_sql,
            params=params,
            candidate_count=candidate_count,
            thresholds=thresholds,
        )

    def _load_or_compute_thresholds(self, conn: sqlite3.Connection) -> dict[str, Any]:
        row = conn.execute(
            "SELECT value_json FROM stats_cache WHERE key = 'difficulty::quantiles'",
        ).fetchone()
        if row and row["value_json"]:
            try:
                payload = json.loads(str(row["value_json"]))
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict) and payload.get("playable_count", 0) > 0:
                return payload

        payload = self._compute_thresholds(conn)
        try:
            conn.execute(
                """
                INSERT INTO stats_cache(key, value_json)
                VALUES ('difficulty::quantiles', ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
                """,
                (json.dumps(payload, ensure_ascii=True),),
            )
        except sqlite3.OperationalError:
            # Read-only deployments can still compute thresholds at runtime.
            pass
        return payload

    def _compute_thresholds(self, conn: sqlite3.Connection) -> dict[str, Any]:
        has_popularity = self._has_player_column(conn, "popularity")
        popularity_expr = self._popularity_expr("p", has_popularity)
        rows = conn.execute(
            f"""
            SELECT {popularity_expr} AS popularity
            FROM playable_players p
            ORDER BY popularity DESC, p.id ASC
            """,
        ).fetchall()
        if not rows:
            raise GameError("Playable pool is empty. Run ETL first.", status_code=409)
        popularity = [int(row["popularity"] or 0) for row in rows]
        n = len(popularity)

        easy_count = max(1, int(math.ceil(n * 0.2)))
        normal_count = max(1, int(math.ceil(n * 0.5)))
        insane_count = max(1, int(math.ceil(n * 0.3)))

        easy_min = popularity[easy_count - 1]
        normal_min = popularity[normal_count - 1]
        insane_start_idx = max(0, n - insane_count)
        insane_max = popularity[insane_start_idx]

        return {
            "playable_count": n,
            "easy_min_popularity": int(easy_min),
            "normal_min_popularity": int(normal_min),
            "insane_max_popularity": int(insane_max),
            "easy_count_target": easy_count,
            "normal_count_target": normal_count,
            "insane_count_target": insane_count,
        }

    def _pick_target_player(
        self,
        conn: sqlite3.Connection,
        pool: DifficultyPool,
        recent_targets: list[int],
    ) -> tuple[int, str]:
        base_where = f"({pool.where_sql})"
        params: list[Any] = list(pool.params)

        if recent_targets:
            placeholders = ",".join("?" for _ in recent_targets)
            base_where = f"{base_where} AND p.id NOT IN ({placeholders})"
            params.extend(recent_targets[-10:])

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM playable_players p WHERE {base_where}",
            tuple(params),
        ).fetchone()
        candidate_count = int(count_row[0]) if count_row else 0

        if candidate_count <= 0:
            base_where = f"({pool.where_sql})"
            params = list(pool.params)
            count_row = conn.execute(
                f"SELECT COUNT(*) FROM playable_players p WHERE {base_where}",
                tuple(params),
            ).fetchone()
            candidate_count = int(count_row[0]) if count_row else 0
        if candidate_count <= 0:
            raise GameError("Unable to pick a target from the selected pool.", status_code=409)

        random_offset = self.rng.randint(0, candidate_count - 1)
        row = conn.execute(
            f"SELECT p.id, p.name FROM playable_players p WHERE {base_where} LIMIT 1 OFFSET ?",
            (*params, random_offset),
        ).fetchone()
        if row is None:
            raise GameError("Failed to choose a target player.", status_code=500)
        return int(row["id"]), str(row["name"])

    def _has_player_column(self, conn: sqlite3.Connection, column_name: str) -> bool:
        rows = conn.execute("PRAGMA table_info(players)").fetchall()
        return any(str(row["name"]) == column_name for row in rows)

    def _popularity_expr(self, alias: str, has_popularity: bool) -> str:
        if has_popularity:
            return f"COALESCE({alias}.popularity, 0)"
        return "0"

    def _name_norm_expr(self, alias: str, has_name_norm: bool) -> str:
        if has_name_norm:
            return f"COALESCE({alias}.name_norm, lower({alias}.name))"
        return f"lower({alias}.name)"

    def _ensure_db(self) -> None:
        try:
            with connect(self.db_path) as conn:
                init_db(conn)
        except sqlite3.OperationalError:
            with connect(self.db_path, read_only=True) as conn:
                conn.execute("SELECT 1").fetchone()

    def _get_lobby(self, lobby_id: str) -> LobbyState:
        lobby = self.lobbies.get(lobby_id.upper())
        if lobby is None:
            raise GameError("Lobby not found.", status_code=404)
        return lobby

    def _ensure_host(self, lobby: LobbyState, host_token: str) -> None:
        if host_token != lobby.host_token:
            raise GameError("Only the host can perform this action.", status_code=403)

    def _ensure_player(self, lobby: LobbyState, token: str) -> PlayerSession:
        player = lobby.players.get(token)
        if player is None:
            raise GameError("Player is not part of this lobby.", status_code=403)
        return player

    def _points_for_clue(self, clue_index: int) -> int:
        return max(1, 11 - clue_index)

    def _make_lobby_id(self) -> str:
        while True:
            token = uuid.uuid4().hex[:6].upper()
            if token not in self.lobbies:
                return token

    def _make_token(self) -> str:
        return uuid.uuid4().hex[:12]

    def _clean_name(self, value: str) -> str:
        clean = " ".join(value.strip().split())
        if not clean:
            raise GameError("Player name cannot be empty.")
        if len(clean) > 50:
            raise GameError("Player name is too long.")
        return clean

    def _normalize_difficulty(self, value: str) -> str:
        mode = (value or "").strip().upper()
        if mode not in DIFFICULTY_MODES:
            raise GameError("Difficulty must be one of: easy, normal, hard, insane.")
        return mode

    def _is_correct_guess(self, guess: str, target_name: str) -> bool:
        guess_norm = _normalize_name(guess)
        target_norm = _normalize_name(target_name)
        if not guess_norm:
            return False
        if guess_norm == target_norm:
            return True

        aliases = {target_norm, target_norm.replace(" ", "")}
        parts = target_norm.split()
        if len(parts) >= 2 and len(parts[-1]) >= 4:
            aliases.add(parts[-1])
        if len(parts) >= 2:
            aliases.add(f"{parts[0]} {parts[-1]}")

        for alias in aliases:
            if guess_norm == alias:
                return True
            if abs(len(guess_norm) - len(alias)) <= 1 and min(len(guess_norm), len(alias)) >= 5:
                if _bounded_levenshtein(guess_norm, alias, max_distance=1) <= 1:
                    return True
            if min(len(guess_norm), len(alias)) >= 5:
                ratio = SequenceMatcher(None, guess_norm, alias).ratio()
                if ratio >= 0.92:
                    return True
        return False


_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")


def _normalize_name(value: str) -> str:
    no_accents = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    cleaned = _NON_ALNUM_RE.sub(" ", no_accents)
    return " ".join(cleaned.split())


def _bounded_levenshtein(a: str, b: str, max_distance: int) -> int:
    if abs(len(a) - len(b)) > max_distance:
        return max_distance + 1
    prev = list(range(len(b) + 1))
    for i, char_a in enumerate(a, start=1):
        curr = [i]
        row_min = curr[0]
        for j, char_b in enumerate(b, start=1):
            cost = 0 if char_a == char_b else 1
            cell = min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost)
            curr.append(cell)
            row_min = min(row_min, cell)
        if row_min > max_distance:
            return max_distance + 1
        prev = curr
    return prev[-1]
