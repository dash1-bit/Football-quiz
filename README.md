# Football Quiz

Multiplayer football player guessing game with progressive, dynamic clues generated from a local Wikidata snapshot.

## Stack
- Backend: Python 3.11+, FastAPI, SQLite, httpx, Uvicorn
- Frontend: single static HTML/CSS/JS page (no build tools)
- Data source: Wikidata SPARQL endpoint only (`https://query.wikidata.org/sparql`)

## Repository Layout
```text
backend/
  app/
  data/
  scripts/
  tests/
frontend/
README.md
requirements.txt
.env.example
```

## Exact Run Steps
1. Create and activate a virtual environment:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:
```powershell
pip install -r requirements.txt
```

3. Create env file:
```powershell
Copy-Item .env.example .env
```
Edit `.env` and set a descriptive `WIKIDATA_USER_AGENT` with your contact.

4. Run ETL snapshot (phase 1 + phase 2):
```powershell
python backend/scripts/etl_snapshot.py
```
Optional smaller debug snapshot:
```powershell
python backend/scripts/etl_snapshot.py --max-players 5000
```

5. Run backend API:
```powershell
uvicorn backend.app.main:app --reload
```

6. Open frontend:
- Open `frontend/index.html` directly in the browser, or
- Serve it locally:
```powershell
cd frontend
python -m http.server 5173
```
Then visit `http://127.0.0.1:5173`.

## Multiplayer API Endpoints
- `POST /api/game/create`
- `POST /api/game/join`
- `POST /api/game/start`
- `GET /api/game/{id}/state`
- `POST /api/game/{id}/guess`
- `POST /api/game/{id}/next_clue`

## Render Deploy
1. In Render, create a new Web Service from this GitHub repository.
2. Keep `render.yaml` enabled (native Python deploy, no manual commands needed).
3. Set environment variable `WIKIDATA_USER_AGENT`.
4. Deploy the service and copy the public backend URL (used by Cloudflare Pages config).

## Database Schema (SQLite)
Tables:
- `players`
- `clubs`
- `player_clubs`
- `national_teams`
- `player_national_teams`
- `stats_cache`
- `snapshot_meta`

View:
- `playable_players` (`position_group`, `citizenship`, `birth_year` present and has at least one club)

## ETL Pipeline
### Phase 1: Player discovery
- Finds all association football players (`Q937857`) with at least one club (`P54`) in a country on continent Europe (`Q46`)
- Uses `LIMIT/OFFSET` pagination
- Throttles to 1 request/second
- Exponential backoff for HTTP 429/503
- Stores discovered player QIDs in `backend/data/discovered_qids.txt`

### Phase 2: Player hydration
- Batches discovered QIDs and fetches:
  - name
  - birth date/year/place
  - citizenship
  - position + mapped position group (`GK`, `DEF`, `MID`, `FWD`, `OTHER`)
  - height
  - clubs (+ start/end years)
  - national teams
- Stores snapshot metadata + distributions in `stats_cache`

## SPARQL Queries Used
### Phase 1 query (discovery)
```sparql
SELECT DISTINCT ?player WHERE {
  ?player wdt:P31 wd:Q5 ;
          wdt:P106 wd:Q937857 ;
          p:P54 ?clubStatement .
  ?clubStatement ps:P54 ?club .
  ?club wdt:P17 ?clubCountry .
  ?clubCountry wdt:P30 wd:Q46 .
}
ORDER BY ?player
LIMIT 2000
OFFSET 0
```

### Phase 2 query (player core details)
```sparql
SELECT DISTINCT ?player ?playerLabel ?birthDate ?birthPlaceLabel ?citizenship ?citizenshipLabel ?position ?positionLabel ?height WHERE {
  VALUES ?player { wd:Q42 wd:Q9372 }
  OPTIONAL { ?player wdt:P569 ?birthDate . }
  OPTIONAL { ?player wdt:P19 ?birthPlace . }
  OPTIONAL { ?player wdt:P27 ?citizenship . }
  OPTIONAL { ?player wdt:P413 ?position . }
  OPTIONAL { ?player wdt:P2048 ?height . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
```

### Phase 2 query (clubs + qualifiers)
```sparql
SELECT DISTINCT ?player ?club ?clubLabel ?clubStart ?clubEnd ?clubCountry ?clubCountryLabel WHERE {
  VALUES ?player { wd:Q42 wd:Q9372 }
  ?player p:P54 ?clubStatement .
  ?clubStatement ps:P54 ?club .
  OPTIONAL { ?clubStatement pq:P580 ?clubStart . }
  OPTIONAL { ?clubStatement pq:P582 ?clubEnd . }
  OPTIONAL { ?club wdt:P17 ?clubCountry . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
```

### Phase 2 query (national teams)
```sparql
SELECT DISTINCT ?player ?nationalTeam ?nationalTeamLabel WHERE {
  VALUES ?player { wd:Q42 wd:Q9372 }
  ?player wdt:P1532 ?nationalTeam .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
```

## Clue Engine Notes
- Candidate predicates are created from multiple attribute types (citizenship, birth decade/year, position group/specific position, clubs, national teams, club-count band, height band, club-country)
- Each candidate computes `match_count` over `playable_players`
- Clues are selected with a broad-to-specific schedule so `match_count` decreases over time
- Template randomization and mixed clue type ordering prevents fixed clue memorization

## Test
Run:
```powershell
pytest -q
```

Included test:
- `backend/tests/test_clue_engine.py` validates clue `match_count` monotonic decrease.
