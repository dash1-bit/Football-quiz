from __future__ import annotations

import random
import sqlite3

from app.clue_engine import ClueEngine
from app.database import init_db


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def seed_data(conn: sqlite3.Connection) -> None:
    players = [
        (1, "Q1", "Lionel Messi", "1987-06-24", 1987, "Rosario", "Argentina", "Q414", "forward", "FWD", 170),
        (2, "Q2", "Cristiano Ronaldo", "1985-02-05", 1985, "Funchal", "Portugal", "Q45", "forward", "FWD", 187),
        (3, "Q3", "Luka Modric", "1985-09-09", 1985, "Zadar", "Croatia", "Q224", "midfielder", "MID", 172),
        (4, "Q4", "Gianluigi Buffon", "1978-01-28", 1978, "Carrara", "Italy", "Q38", "goalkeeper", "GK", 192),
        (5, "Q5", "Sergio Ramos", "1986-03-30", 1986, "Camas", "Spain", "Q29", "defender", "DEF", 184),
        (6, "Q6", "Kylian Mbappe", "1998-12-20", 1998, "Paris", "France", "Q142", "forward", "FWD", 178),
        (7, "Q7", "Manuel Neuer", "1986-03-27", 1986, "Gelsenkirchen", "Germany", "Q183", "goalkeeper", "GK", 193),
        (8, "Q8", "Kevin De Bruyne", "1991-06-28", 1991, "Drongen", "Belgium", "Q31", "midfielder", "MID", 181),
    ]
    conn.executemany(
        """
        INSERT INTO players(
          id, wikidata_id, name, birth_date, birth_year, birth_place,
          citizenship, citizenship_qid, position, position_group, height_cm
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        players,
    )

    clubs = [
        (1, "FC Barcelona", "Q7156"),
        (2, "Paris Saint-Germain", "Q483020"),
        (3, "Real Madrid", "Q8682"),
        (4, "Manchester United", "Q18656"),
        (5, "Tottenham Hotspur", "Q703"),
        (6, "Juventus", "Q191"),
        (7, "AS Monaco", "Q170332"),
        (8, "FC Bayern Munich", "Q1573"),
        (9, "Schalke 04", "Q152223"),
        (10, "Manchester City", "Q50602"),
        (11, "Wolfsburg", "Q47662"),
    ]
    conn.executemany("INSERT INTO clubs(id, name, qid) VALUES (?, ?, ?)", clubs)

    player_clubs = [
        (1, 1, 2004, 2021),
        (1, 2, 2021, 2023),
        (2, 4, 2003, 2009),
        (2, 3, 2009, 2018),
        (3, 3, 2012, None),
        (3, 5, 2008, 2012),
        (4, 6, 2001, 2018),
        (4, 2, 2018, 2019),
        (5, 3, 2005, 2021),
        (5, 2, 2021, 2023),
        (6, 2, 2017, None),
        (6, 7, 2015, 2017),
        (7, 8, 2011, None),
        (7, 9, 2005, 2011),
        (8, 10, 2015, None),
        (8, 11, 2014, 2015),
    ]
    conn.executemany(
        "INSERT INTO player_clubs(player_id, club_id, start_year, end_year) VALUES (?, ?, ?, ?)",
        player_clubs,
    )

    national_teams = [
        (1, "Argentina", "Q42267"),
        (2, "Portugal", "Q49802"),
        (3, "Croatia", "Q232614"),
        (4, "Italy", "Q477808"),
        (5, "Spain", "Q477810"),
        (6, "France", "Q477812"),
        (7, "Germany", "Q477813"),
        (8, "Belgium", "Q477814"),
    ]
    conn.executemany("INSERT INTO national_teams(id, name, qid) VALUES (?, ?, ?)", national_teams)
    conn.executemany(
        "INSERT INTO player_national_teams(player_id, national_team_id) VALUES (?, ?)",
        [(idx, idx) for idx in range(1, 9)],
    )

    conn.executemany(
        "INSERT INTO stats_cache(key, value_json) VALUES (?, ?)",
        [
            ("club_country::Q7156", '{"country":"Spain","country_qid":"Q29"}'),
            ("club_country::Q483020", '{"country":"France","country_qid":"Q142"}'),
            ("club_country::Q8682", '{"country":"Spain","country_qid":"Q29"}'),
            ("club_country::Q18656", '{"country":"United Kingdom","country_qid":"Q145"}'),
            ("club_country::Q703", '{"country":"United Kingdom","country_qid":"Q145"}'),
            ("club_country::Q191", '{"country":"Italy","country_qid":"Q38"}'),
            ("club_country::Q170332", '{"country":"France","country_qid":"Q142"}'),
            ("club_country::Q1573", '{"country":"Germany","country_qid":"Q183"}'),
            ("club_country::Q152223", '{"country":"Germany","country_qid":"Q183"}'),
            ("club_country::Q50602", '{"country":"United Kingdom","country_qid":"Q145"}'),
            ("club_country::Q47662", '{"country":"Germany","country_qid":"Q183"}'),
        ],
    )
    conn.commit()


def test_clue_match_counts_are_monotonic_decreasing() -> None:
    conn = make_conn()
    seed_data(conn)

    engine = ClueEngine(conn=conn, rng=random.Random(7))
    clues = engine.generate_for_player(player_id=1, clue_count=10)
    assert len(clues) >= 5

    counts = [clue["match_count"] for clue in clues]
    assert all(counts[idx] >= counts[idx + 1] for idx in range(len(counts) - 1))
    assert counts[0] > counts[-1]

