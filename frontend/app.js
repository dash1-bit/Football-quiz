const appState = {
  gameId: null,
  playerId: null,
  pollHandle: null,
};

const statusBox = document.getElementById("statusBox");
const sessionInfo = document.getElementById("sessionInfo");
const clueList = document.getElementById("clueList");
const scoreboardBody = document.getElementById("scoreboardBody");

function apiBase() {
  return document.getElementById("apiBase").value.trim().replace(/\/+$/, "");
}

function setStatus(message, payload = null) {
  const lines = [message];
  if (payload) {
    lines.push(JSON.stringify(payload, null, 2));
  }
  statusBox.textContent = lines.join("\n\n");
}

async function apiRequest(path, method = "GET", body = null) {
  const url = `${apiBase()}${path}`;
  const options = { method, headers: {} };
  if (body) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  const response = await fetch(url, options);
  const text = await response.text();
  let payload = {};
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }
  if (!response.ok) {
    const detail = payload.detail || response.statusText || "Request failed";
    throw new Error(detail);
  }
  return payload;
}

function renderGameState(game) {
  sessionInfo.textContent = appState.gameId
    ? `Game ${appState.gameId} | You ${appState.playerId}`
    : "Not connected.";

  clueList.innerHTML = "";
  const round = game.round;
  if (round && Array.isArray(round.revealed_clues)) {
    for (const clue of round.revealed_clues) {
      const li = document.createElement("li");
      li.textContent = clue.text;
      clueList.appendChild(li);
    }
  }

  scoreboardBody.innerHTML = "";
  if (Array.isArray(game.scoreboard)) {
    for (const row of game.scoreboard) {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      const tdScore = document.createElement("td");
      tdName.textContent = row.name;
      tdScore.textContent = String(row.score);
      tr.append(tdName, tdScore);
      scoreboardBody.appendChild(tr);
    }
  }

  const summary = {
    game_status: game.status,
    round_status: round ? round.status : null,
    revealed: round ? `${round.revealed_count}/${round.total_clues}` : null,
    winner: round ? round.winner_name : null,
    answer: round ? round.answer : null,
  };
  setStatus("State updated.", summary);
}

async function refreshState() {
  if (!appState.gameId) {
    return;
  }
  const game = await apiRequest(`/api/game/${appState.gameId}/state`);
  renderGameState(game);
}

async function createGame() {
  const playerName = document.getElementById("createName").value.trim();
  if (!playerName) {
    setStatus("Enter your name first.");
    return;
  }
  const payload = await apiRequest("/api/game/create", "POST", { player_name: playerName });
  appState.gameId = payload.game_id;
  appState.playerId = payload.player_id;
  document.getElementById("joinGameId").value = payload.game_id;
  renderGameState(payload.state);
}

async function joinGame() {
  const gameId = document.getElementById("joinGameId").value.trim().toUpperCase();
  const playerName = document.getElementById("joinName").value.trim();
  if (!gameId || !playerName) {
    setStatus("Provide game ID and name.");
    return;
  }
  const payload = await apiRequest("/api/game/join", "POST", {
    game_id: gameId,
    player_name: playerName,
  });
  appState.gameId = payload.game_id;
  appState.playerId = payload.player_id;
  renderGameState(payload.state);
}

async function startRound() {
  if (!appState.gameId || !appState.playerId) {
    setStatus("Create or join a game first.");
    return;
  }
  const payload = await apiRequest("/api/game/start", "POST", {
    game_id: appState.gameId,
    player_id: appState.playerId,
  });
  renderGameState(payload.state);
}

async function revealNextClue() {
  if (!appState.gameId || !appState.playerId) {
    setStatus("Create or join a game first.");
    return;
  }
  const payload = await apiRequest(`/api/game/${appState.gameId}/next_clue`, "POST", {
    player_id: appState.playerId,
  });
  renderGameState(payload.state);
}

async function submitGuess() {
  if (!appState.gameId || !appState.playerId) {
    setStatus("Create or join a game first.");
    return;
  }
  const guess = document.getElementById("guessInput").value.trim();
  if (!guess) {
    setStatus("Enter a guess first.");
    return;
  }
  const payload = await apiRequest(`/api/game/${appState.gameId}/guess`, "POST", {
    player_id: appState.playerId,
    guess,
  });
  document.getElementById("guessInput").value = "";
  renderGameState(payload.state);
  if (payload.correct) {
    setStatus(`Correct guess! +${payload.points_awarded} points`, payload.state.round);
  } else {
    setStatus("Incorrect guess. Keep trying.");
  }
}

function attachHandlers() {
  document.getElementById("createBtn").addEventListener("click", () => runAction(createGame));
  document.getElementById("joinBtn").addEventListener("click", () => runAction(joinGame));
  document.getElementById("startBtn").addEventListener("click", () => runAction(startRound));
  document.getElementById("nextClueBtn").addEventListener("click", () => runAction(revealNextClue));
  document.getElementById("guessBtn").addEventListener("click", () => runAction(submitGuess));
  document.getElementById("refreshBtn").addEventListener("click", () => runAction(refreshState));
}

async function runAction(fn) {
  try {
    await fn();
  } catch (error) {
    setStatus(`Error: ${error.message}`);
  }
}

function startPolling() {
  if (appState.pollHandle) {
    clearInterval(appState.pollHandle);
  }
  appState.pollHandle = setInterval(() => {
    runAction(refreshState);
  }, 3000);
}

attachHandlers();
startPolling();
setStatus("Ready. Set API URL, then create or join a game.");

