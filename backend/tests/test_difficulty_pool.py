from __future__ import annotations

from pathlib import Path

from app.database import connect, init_db
from app.game_engine import GameManager


def seed_pool(conn) -> None:
    conn.execute("INSERT INTO clubs(id, name, qid) VALUES (1, 'Test Club', 'QTESTCLUB')")
    players = []
    player_clubs = []
    for idx in range(1, 11):
        players.append(
            (
                idx,
                f"Q{idx}",
                f"Player {idx}",
                f"player {idx}",
                1990,
                "Testland",
                "QTEST",
                "midfielder",
                "MID",
                180,
                idx,
            )
        )
        player_clubs.append((idx, 1, 2010, 2011))

    conn.executemany(
        """
        INSERT INTO players(
            id, wikidata_id, name, name_norm, birth_year, citizenship, citizenship_qid,
            position, position_group, height_cm, popularity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        players,
    )
    conn.executemany(
        "INSERT INTO player_clubs(player_id, club_id, start_year, end_year) VALUES (?, ?, ?, ?)",
        player_clubs,
    )
    conn.commit()


def test_easy_pool_uses_top_20_percent_popularity(tmp_path: Path) -> None:
    db_path = tmp_path / "difficulty.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        seed_pool(conn)

    manager = GameManager(db_path=db_path, scoring_curve=(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
    with connect(db_path) as conn:
        pool = manager._build_pool(conn, "easy")
        assert pool.candidate_count == 2
        rows = conn.execute(
            f"SELECT popularity FROM playable_players p WHERE ({pool.where_sql}) ORDER BY popularity DESC",
            pool.params,
        ).fetchall()

    values = [int(row["popularity"]) for row in rows]
    assert values == [10, 9]
    assert min(values) >= 9
