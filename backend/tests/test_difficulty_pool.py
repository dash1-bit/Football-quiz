from __future__ import annotations

from pathlib import Path

from app.database import connect, init_db
from app.game_engine import GameManager


def seed_pool(conn) -> None:
    conn.execute("INSERT INTO clubs(id, name, qid) VALUES (1, 'Test Club', 'QTESTCLUB')")
    players = []
    player_clubs = []
    for idx in range(1, 301):
        birth_year = 1940 if idx <= 30 else 1950 + (idx % 40)
        players.append(
            (
                idx,
                f"Q{idx}",
                f"Player {idx}",
                f"player {idx}",
                birth_year,
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


def test_famous_pool_and_easy_top_5_percent(tmp_path: Path) -> None:
    db_path = tmp_path / "difficulty.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        seed_pool(conn)

    manager = GameManager(
        db_path=db_path,
        scoring_curve=(10, 9, 8, 7, 6, 5, 4, 3, 2, 1),
        famous_pool_size=120,
        min_birth_year=1950,
    )
    with connect(db_path) as conn:
        famous_count = int(conn.execute("SELECT COUNT(*) FROM famous_players").fetchone()[0])
        min_famous_birth_year = int(
            conn.execute("SELECT MIN(birth_year) FROM famous_players").fetchone()[0],
        )
        pool = manager._build_pool(conn, "easy")
        normal_pool = manager._build_pool(conn, "normal")
        assert normal_pool.candidate_count == 120
        assert pool.candidate_count == 6
        rows = conn.execute(
            f"SELECT popularity FROM {pool.source_view} p WHERE ({pool.where_sql}) ORDER BY popularity DESC",
            pool.params,
        ).fetchall()

    assert famous_count == 120
    assert min_famous_birth_year >= 1950
    values = [int(row["popularity"]) for row in rows]
    assert values == [300, 299, 298, 297, 296, 295]
