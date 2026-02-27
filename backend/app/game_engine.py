from __future__ import annotations

import random
import re
import sqlite3
import threading
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from .clue_engine import ClueEngine
from .database import connect, init_db


class GameError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class Participant:
    id: str
    name: str
    score: int = 0


@dataclass
class RoundState:
    target_player_id: int
    target_name: str
    clues: list[dict[str, Any]]
    revealed_count: int
    is_finished: bool = False
    winner_player_id: str | None = None
    guesses: list[dict[str, Any]] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


@dataclass
class GameState:
    id: str
    host_player_id: str
    players: dict[str, Participant]
    status: str = "lobby"
    round_number: int = 0
    current_round: RoundState | None = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    recent_targets: list[int] = field(default_factory=list)


class GameManager:
    def __init__(self, db_path: Path, scoring_curve: tuple[int, ...]) -> None:
        self.db_path = Path(db_path)
        self.scoring_curve = scoring_curve
        self.games: dict[str, GameState] = {}
        self.lock = threading.Lock()
        self.rng = random.Random()
        self._ensure_db()

    def create_game(self, player_name: str) -> dict[str, Any]:
        clean_name = self._clean_name(player_name)
        with self.lock:
            player_id = self._make_token()
            game_id = self._make_game_id()
            player = Participant(id=player_id, name=clean_name)
            game = GameState(id=game_id, host_player_id=player_id, players={player_id: player})
            self.games[game_id] = game
            return {
                "game_id": game_id,
                "player_id": player_id,
                "state": self._serialize_game(game),
            }

    def join_game(self, game_id: str, player_name: str) -> dict[str, Any]:
        clean_name = self._clean_name(player_name)
        with self.lock:
            game = self._get_game(game_id)
            player_id = self._make_token()
            game.players[player_id] = Participant(id=player_id, name=clean_name)
            return {
                "game_id": game.id,
                "player_id": player_id,
                "state": self._serialize_game(game),
            }

    def start_round(self, game_id: str, requester_player_id: str) -> dict[str, Any]:
        with self.lock:
            game = self._get_game(game_id)
            self._ensure_player(game, requester_player_id)
            if requester_player_id != game.host_player_id:
                raise GameError("Only the host can start a round.", status_code=403)
            if game.current_round and not game.current_round.is_finished:
                raise GameError("A round is already in progress.", status_code=409)

            with connect(self.db_path) as conn:
                init_db(conn)
                target_id, target_name = self._pick_target_player(conn, game.recent_targets)
                clue_engine = ClueEngine(conn=conn, rng=random.Random(self.rng.random()))
                clues = clue_engine.generate_for_player(target_id, clue_count=len(self.scoring_curve))

            game.round_number += 1
            game.status = "in_round"
            game.current_round = RoundState(
                target_player_id=target_id,
                target_name=target_name,
                clues=clues,
                revealed_count=1,
            )
            game.recent_targets.append(target_id)
            if len(game.recent_targets) > 20:
                game.recent_targets = game.recent_targets[-20:]
            return self._serialize_game(game)

    def get_state(self, game_id: str) -> dict[str, Any]:
        with self.lock:
            game = self._get_game(game_id)
            return self._serialize_game(game)

    def submit_guess(self, game_id: str, player_id: str, guess: str) -> dict[str, Any]:
        clean_guess = guess.strip()
        if not clean_guess:
            raise GameError("Guess cannot be empty.")

        with self.lock:
            game = self._get_game(game_id)
            player = self._ensure_player(game, player_id)
            current_round = game.current_round
            if current_round is None:
                raise GameError("No active round.")
            if current_round.is_finished:
                return {
                    "correct": False,
                    "message": "Round already finished.",
                    "state": self._serialize_game(game),
                }

            is_correct = self._is_correct_guess(clean_guess, current_round.target_name)
            points_awarded = 0
            if is_correct:
                points_awarded = self._score_for_clue(current_round.revealed_count)
                player.score += points_awarded
                current_round.is_finished = True
                current_round.winner_player_id = player.id
                game.status = "round_finished"
            current_round.guesses.append(
                {
                    "player_id": player.id,
                    "guess": clean_guess,
                    "correct": is_correct,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )

            return {
                "correct": is_correct,
                "points_awarded": points_awarded,
                "state": self._serialize_game(game),
            }

    def next_clue(self, game_id: str, player_id: str) -> dict[str, Any]:
        with self.lock:
            game = self._get_game(game_id)
            self._ensure_player(game, player_id)
            current_round = game.current_round
            if current_round is None:
                raise GameError("No active round.")
            if current_round.is_finished:
                raise GameError("Round already finished.")
            if current_round.revealed_count < len(current_round.clues):
                current_round.revealed_count += 1
            return self._serialize_game(game)

    def _serialize_game(self, game: GameState) -> dict[str, Any]:
        scoreboard = sorted(
            (
                {"player_id": p.id, "name": p.name, "score": p.score}
                for p in game.players.values()
            ),
            key=lambda row: (-row["score"], row["name"].lower()),
        )

        round_payload: dict[str, Any] | None = None
        if game.current_round:
            round_state = game.current_round
            round_payload = {
                "round_number": game.round_number,
                "status": "finished" if round_state.is_finished else "active",
                "revealed_count": round_state.revealed_count,
                "total_clues": len(round_state.clues),
                "revealed_clues": round_state.clues[: round_state.revealed_count],
                "winner_player_id": round_state.winner_player_id,
                "winner_name": (
                    game.players[round_state.winner_player_id].name
                    if round_state.winner_player_id
                    and round_state.winner_player_id in game.players
                    else None
                ),
                "answer": round_state.target_name if round_state.is_finished else None,
            }

        return {
            "game_id": game.id,
            "status": game.status,
            "host_player_id": game.host_player_id,
            "created_at": game.created_at,
            "players": [
                {"player_id": p.id, "name": p.name}
                for p in sorted(game.players.values(), key=lambda part: part.name.lower())
            ],
            "scoreboard": scoreboard,
            "round": round_payload,
        }

    def _pick_target_player(self, conn: sqlite3.Connection, recent_targets: list[int]) -> tuple[int, str]:
        filtered_ids = tuple(recent_targets[-10:])
        where_clause = ""
        params: tuple[Any, ...] = ()
        if filtered_ids:
            placeholders = ",".join("?" for _ in filtered_ids)
            where_clause = f"WHERE id NOT IN ({placeholders})"
            params = filtered_ids

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM playable_players {where_clause}",
            params,
        ).fetchone()
        candidate_count = int(count_row[0]) if count_row else 0

        if candidate_count == 0:
            count_row = conn.execute("SELECT COUNT(*) FROM playable_players").fetchone()
            candidate_count = int(count_row[0]) if count_row else 0
            where_clause = ""
            params = ()

        if candidate_count == 0:
            raise GameError(
                "Playable pool is empty. Run the ETL snapshot first.",
                status_code=409,
            )

        random_offset = self.rng.randint(0, candidate_count - 1)
        row = conn.execute(
            f"SELECT id, name FROM playable_players {where_clause} LIMIT 1 OFFSET ?",
            (*params, random_offset),
        ).fetchone()
        if row is None:
            raise GameError("Failed to choose a target player.", status_code=500)
        return int(row["id"]), str(row["name"])

    def _ensure_db(self) -> None:
        with connect(self.db_path) as conn:
            init_db(conn)

    def _get_game(self, game_id: str) -> GameState:
        game = self.games.get(game_id)
        if game is None:
            raise GameError("Game not found.", status_code=404)
        return game

    def _ensure_player(self, game: GameState, player_id: str) -> Participant:
        player = game.players.get(player_id)
        if player is None:
            raise GameError("Player is not part of this game.", status_code=403)
        return player

    def _score_for_clue(self, clue_index: int) -> int:
        if clue_index <= 0:
            return self.scoring_curve[-1]
        if clue_index > len(self.scoring_curve):
            return self.scoring_curve[-1]
        return self.scoring_curve[clue_index - 1]

    def _make_game_id(self) -> str:
        while True:
            token = uuid.uuid4().hex[:6].upper()
            if token not in self.games:
                return token

    def _make_token(self) -> str:
        return uuid.uuid4().hex[:10]

    def _clean_name(self, value: str) -> str:
        clean = " ".join(value.strip().split())
        if not clean:
            raise GameError("Player name cannot be empty.")
        if len(clean) > 50:
            raise GameError("Player name is too long.")
        return clean

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

