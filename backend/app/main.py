from __future__ import annotations

import logging
import sqlite3

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import load_settings
from .database import connect, create_or_replace_famous_view, init_db
from .game_engine import GameError, GameManager


settings = load_settings()
game_manager = GameManager(
    db_path=settings.db_path,
    scoring_curve=settings.scoring_curve,
    famous_pool_size=settings.famous_pool_size,
    min_birth_year=settings.min_birth_year,
)
logger = logging.getLogger("football_quiz.api")

app = FastAPI(title="Football Quiz API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_allow_origins),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LobbyCreateRequest(BaseModel):
    host_name: str = Field(min_length=1, max_length=50)
    difficulty: str = Field(min_length=1, max_length=20)


class LobbyJoinRequest(BaseModel):
    lobby_id: str = Field(min_length=1, max_length=12)
    player_name: str = Field(min_length=1, max_length=50)


class LobbyStartRequest(BaseModel):
    host_token: str = Field(min_length=1, max_length=64)


class SubmitGuessRequest(BaseModel):
    player_token: str = Field(min_length=1, max_length=64)
    guess_text: str = Field(min_length=1, max_length=120)


class AdvanceRequest(BaseModel):
    host_token: str | None = Field(default=None, min_length=1, max_length=64)


class LegacyCreateGameRequest(BaseModel):
    player_name: str = Field(min_length=1, max_length=50)


class LegacyJoinGameRequest(BaseModel):
    game_id: str = Field(min_length=1, max_length=12)
    player_name: str = Field(min_length=1, max_length=50)


class LegacyStartGameRequest(BaseModel):
    game_id: str = Field(min_length=1, max_length=12)
    player_id: str = Field(min_length=1, max_length=64)


class LegacyGuessRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=64)
    guess: str = Field(min_length=1, max_length=120)


class LegacyNextClueRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=64)


@app.on_event("startup")
def on_startup() -> None:
    _ensure_schema_or_read_only()


@app.get("/api/health")
def health() -> dict[str, str | int | None]:
    snapshot_time: str | None = None
    total_players = 0
    playable_players = 0
    try:
        with connect(settings.db_path, read_only=True) as conn:
            row = conn.execute(
                "SELECT value FROM snapshot_meta WHERE key = 'snapshot_generated_at'",
            ).fetchone()
            if row:
                snapshot_time = row["value"]
            total_players = int(conn.execute("SELECT COUNT(*) FROM players").fetchone()[0])
            playable_players = int(conn.execute("SELECT COUNT(*) FROM playable_players").fetchone()[0])
    except sqlite3.OperationalError:
        with connect(settings.db_path) as conn:
            init_db(conn)
            create_or_replace_famous_view(
                conn,
                famous_pool_size=settings.famous_pool_size,
                min_birth_year=settings.min_birth_year,
            )
            conn.commit()
            row = conn.execute(
                "SELECT value FROM snapshot_meta WHERE key = 'snapshot_generated_at'",
            ).fetchone()
            if row:
                snapshot_time = row["value"]
            total_players = int(conn.execute("SELECT COUNT(*) FROM players").fetchone()[0])
            playable_players = int(conn.execute("SELECT COUNT(*) FROM playable_players").fetchone()[0])
    return {
        "status": "ok",
        "snapshot_time": snapshot_time,
        "total_players": total_players,
        "playable_players": playable_players,
    }


@app.post("/api/lobby/create")
def create_lobby(payload: LobbyCreateRequest, request: Request) -> dict:
    try:
        result = game_manager.create_lobby(payload.host_name, payload.difficulty)
        share_url = str(request.base_url).rstrip("/") + result["share_url"]
        result["share_url"] = share_url
        return result
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/lobby/join")
def join_lobby(payload: LobbyJoinRequest) -> dict:
    try:
        return game_manager.join_lobby(payload.lobby_id.upper(), payload.player_name)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/api/lobby/{lobby_id}/state")
def lobby_state(lobby_id: str, token: str | None = Query(default=None, max_length=64)) -> dict:
    try:
        return game_manager.get_lobby_state(lobby_id.upper(), token=token)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/lobby/{lobby_id}/start")
def start_lobby_game(lobby_id: str, payload: LobbyStartRequest) -> dict:
    try:
        state = game_manager.start_game(lobby_id.upper(), payload.host_token)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/api/game/{lobby_id}/state")
def game_state(lobby_id: str, token: str | None = Query(default=None, max_length=64)) -> dict:
    try:
        return game_manager.get_game_state(lobby_id.upper(), token=token)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{lobby_id}/submit_guess")
def submit_guess(lobby_id: str, payload: SubmitGuessRequest) -> dict:
    try:
        return game_manager.submit_guess(lobby_id.upper(), payload.player_token, payload.guess_text)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{lobby_id}/advance_if_needed")
def advance_if_needed(lobby_id: str, payload: AdvanceRequest) -> dict:
    try:
        state = game_manager.advance_if_needed(lobby_id.upper(), host_token=payload.host_token)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/api/autocomplete")
def autocomplete(
    q: str = Query(min_length=1, max_length=120),
    lobby_id: str = Query(min_length=1, max_length=12),
    limit: int = Query(default=10, ge=1, le=10),
) -> dict:
    try:
        suggestions = game_manager.autocomplete(lobby_id=lobby_id.upper(), query=q, limit=limit)
        return {"suggestions": suggestions}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/create")
def legacy_create_game(payload: LegacyCreateGameRequest) -> dict:
    try:
        return game_manager.create_game(payload.player_name)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/join")
def legacy_join_game(payload: LegacyJoinGameRequest) -> dict:
    try:
        return game_manager.join_game(payload.game_id.upper(), payload.player_name)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/start")
def legacy_start_game(payload: LegacyStartGameRequest) -> dict:
    try:
        state = game_manager.start_round(payload.game_id.upper(), payload.player_id)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{game_id}/guess")
def legacy_guess(game_id: str, payload: LegacyGuessRequest) -> dict:
    try:
        return game_manager.submit_guess(game_id.upper(), payload.player_id, payload.guess)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{game_id}/next_clue")
def legacy_next_clue(game_id: str, payload: LegacyNextClueRequest) -> dict:
    try:
        state = game_manager.next_clue(game_id.upper(), payload.player_id)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


def _ensure_schema_or_read_only() -> None:
    if not settings.db_path.exists():
        logger.warning("Database file not found at startup: %s", settings.db_path)
    try:
        with connect(settings.db_path) as conn:
            init_db(conn)
            create_or_replace_famous_view(
                conn,
                famous_pool_size=settings.famous_pool_size,
                min_birth_year=settings.min_birth_year,
            )
            conn.commit()
    except sqlite3.OperationalError:
        with connect(settings.db_path, read_only=True) as conn:
            conn.execute("SELECT 1").fetchone()
