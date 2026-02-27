from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import load_settings
from .database import connect, init_db
from .game_engine import GameError, GameManager


settings = load_settings()
game_manager = GameManager(db_path=settings.db_path, scoring_curve=settings.scoring_curve)

app = FastAPI(title="Football Quiz API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_allow_origins),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CreateGameRequest(BaseModel):
    player_name: str = Field(min_length=1, max_length=50)


class JoinGameRequest(BaseModel):
    game_id: str = Field(min_length=1, max_length=12)
    player_name: str = Field(min_length=1, max_length=50)


class StartGameRequest(BaseModel):
    game_id: str = Field(min_length=1, max_length=12)
    player_id: str = Field(min_length=1, max_length=32)


class GuessRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=32)
    guess: str = Field(min_length=1, max_length=120)


class NextClueRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=32)


@app.on_event("startup")
def on_startup() -> None:
    with connect(settings.db_path) as conn:
        init_db(conn)


@app.get("/api/health")
def health() -> dict[str, str | int | None]:
    snapshot_time: str | None = None
    total_players = 0
    playable_players = 0
    with connect(settings.db_path) as conn:
        init_db(conn)
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


@app.post("/api/game/create")
def create_game(payload: CreateGameRequest) -> dict:
    try:
        return game_manager.create_game(payload.player_name)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/join")
def join_game(payload: JoinGameRequest) -> dict:
    try:
        return game_manager.join_game(payload.game_id.upper(), payload.player_name)
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/start")
def start_game(payload: StartGameRequest) -> dict:
    try:
        state = game_manager.start_round(payload.game_id.upper(), payload.player_id)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/api/game/{game_id}/state")
def game_state(game_id: str) -> dict:
    try:
        return game_manager.get_state(game_id.upper())
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{game_id}/guess")
def submit_guess(game_id: str, payload: GuessRequest) -> dict:
    try:
        return game_manager.submit_guess(
            game_id.upper(),
            payload.player_id,
            payload.guess,
        )
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/game/{game_id}/next_clue")
def next_clue(game_id: str, payload: NextClueRequest) -> dict:
    try:
        state = game_manager.next_clue(game_id.upper(), payload.player_id)
        return {"state": state}
    except GameError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
