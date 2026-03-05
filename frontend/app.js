const screens = {
  menu: document.getElementById("screenMenu"),
  setup: document.getElementById("screenSetup"),
  lobby: document.getElementById("screenLobby"),
  game: document.getElementById("screenGame"),
  end: document.getElementById("screenEnd"),
};

const dom = {
  playBtn: document.getElementById("playBtn"),
  backToMenuBtn: document.getElementById("backToMenuBtn"),
  lobbyBackBtn: document.getElementById("lobbyBackBtn"),
  endBackToMenuBtn: document.getElementById("endBackToMenuBtn"),
  apiBase: document.getElementById("apiBase"),
  createHostName: document.getElementById("createHostName"),
  difficultySelect: document.getElementById("difficultySelect"),
  createLobbyBtn: document.getElementById("createLobbyBtn"),
  joinLobbyCode: document.getElementById("joinLobbyCode"),
  joinPlayerName: document.getElementById("joinPlayerName"),
  joinLobbyBtn: document.getElementById("joinLobbyBtn"),
  lobbyCodeText: document.getElementById("lobbyCodeText"),
  shareLinkBtn: document.getElementById("shareLinkBtn"),
  playerCountText: document.getElementById("playerCountText"),
  lobbyPlayersList: document.getElementById("lobbyPlayersList"),
  startGameBtn: document.getElementById("startGameBtn"),
  clueIndexText: document.getElementById("clueIndexText"),
  timerText: document.getElementById("timerText"),
  clueText: document.getElementById("clueText"),
  guessInput: document.getElementById("guessInput"),
  guessBtn: document.getElementById("guessBtn"),
  guessStatusText: document.getElementById("guessStatusText"),
  autocompleteList: document.getElementById("autocompleteList"),
  scoreboardList: document.getElementById("scoreboardList"),
  answerText: document.getElementById("answerText"),
  finalScoreList: document.getElementById("finalScoreList"),
  playAgainBtn: document.getElementById("playAgainBtn"),
  globalMessage: document.getElementById("globalMessage"),
};

const state = {
  lobbyId: null,
  token: null,
  isHost: false,
  started: false,
  gameOver: false,
  clueIndex: 0,
  pollHandle: null,
  autocompleteHandle: null,
  latestLobby: null,
  latestGame: null,
};

function resolveDefaultApiBase() {
  const value = window.__API_BASE_URL__;
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  return "https://football-quiz-t7m8.onrender.com";
}

function apiBase() {
  return dom.apiBase.value.trim().replace(/\/+$/, "");
}

function setMessage(message = "", isError = true) {
  dom.globalMessage.style.color = isError ? "var(--danger)" : "var(--muted)";
  dom.globalMessage.textContent = message;
}

function showScreen(name) {
  Object.entries(screens).forEach(([key, element]) => {
    element.classList.toggle("active", key === name);
  });
}

function buildShareUrl(lobbyId) {
  return `${window.location.origin}/?lobby=${encodeURIComponent(lobbyId)}`;
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

function renderPlayerList(listElement, rows) {
  listElement.innerHTML = "";
  for (const row of rows) {
    const li = document.createElement("li");
    const left = document.createElement("span");
    const right = document.createElement("span");
    left.textContent = row.name;
    right.textContent = row.rightText || "";
    li.append(left, right);
    listElement.appendChild(li);
  }
}

function renderLobby(lobby) {
  state.latestLobby = lobby;
  state.started = Boolean(lobby.started);
  dom.lobbyCodeText.textContent = lobby.lobby_id;
  dom.playerCountText.textContent = `${lobby.player_count}/${lobby.max_players} players`;
  renderPlayerList(
    dom.lobbyPlayersList,
    (lobby.players || []).map((player) => ({ name: player.name, rightText: "" })),
  );
  const canStart = Boolean(lobby.can_start);
  dom.startGameBtn.disabled = !canStart;
  state.isHost = Boolean(lobby.is_host);
}

function renderGame(game) {
  state.latestGame = game;
  state.started = Boolean(game.started);
  state.gameOver = Boolean(game.game_over);

  if (state.gameOver) {
    renderEnd(game);
    return;
  }

  showScreen("game");
  dom.clueIndexText.textContent = String(game.clue_index || 1);
  dom.timerText.textContent = `${game.round_seconds_left || 0}s`;
  dom.clueText.textContent = game.current_clue_text || "Waiting for clue...";

  const scoreboardRows = (game.scoreboard || []).map((row) => ({
    name: row.name,
    rightText: `${row.score} pts${row.has_solved ? " • solved" : ""}`,
  }));
  renderPlayerList(dom.scoreboardList, scoreboardRows);

  const clueChanged = state.clueIndex !== game.clue_index;
  const canGuess = Boolean(game.can_guess);
  dom.guessInput.disabled = !canGuess;
  dom.guessBtn.disabled = !canGuess;

  if (clueChanged && canGuess) {
    dom.guessInput.value = "";
    hideAutocomplete();
  }
  state.clueIndex = game.clue_index;

  if (game.you_has_solved) {
    dom.guessStatusText.textContent = "Correct! You're locked for the rest of this game.";
  } else if (game.you_has_submitted_this_round) {
    dom.guessStatusText.textContent = "Waiting for others...";
  } else if (canGuess) {
    dom.guessStatusText.textContent = "Type a player name and submit.";
  } else {
    dom.guessStatusText.textContent = "";
  }
}

function renderEnd(game) {
  showScreen("end");
  dom.answerText.textContent = game.answer_name || "Unknown";
  const rows = (game.scoreboard || []).map((row) => ({
    name: row.name,
    rightText: `${row.score} pts`,
  }));
  renderPlayerList(dom.finalScoreList, rows);

  dom.playAgainBtn.disabled = !state.isHost;
  if (!state.isHost) {
    setMessage("Waiting for host to start another game.", false);
  } else {
    setMessage("");
  }
}

async function refreshState() {
  if (!state.lobbyId) {
    return;
  }

  if (!state.started) {
    const lobby = await apiRequest(
      `/api/lobby/${encodeURIComponent(state.lobbyId)}/state?token=${encodeURIComponent(state.token || "")}`,
    );
    renderLobby(lobby);
    if (lobby.started) {
      state.started = true;
      const game = await apiRequest(
        `/api/game/${encodeURIComponent(state.lobbyId)}/state?token=${encodeURIComponent(state.token || "")}`,
      );
      renderGame(game);
    } else {
      showScreen("lobby");
    }
    return;
  }

  const game = await apiRequest(
    `/api/game/${encodeURIComponent(state.lobbyId)}/state?token=${encodeURIComponent(state.token || "")}`,
  );
  renderGame(game);
}

function startPolling() {
  stopPolling();
  state.pollHandle = setInterval(() => {
    runAction(refreshState);
  }, 1000);
}

function stopPolling() {
  if (state.pollHandle) {
    clearInterval(state.pollHandle);
    state.pollHandle = null;
  }
}

function hideAutocomplete() {
  dom.autocompleteList.innerHTML = "";
  dom.autocompleteList.classList.remove("visible");
}

function renderAutocomplete(items) {
  dom.autocompleteList.innerHTML = "";
  if (!items.length) {
    dom.autocompleteList.classList.remove("visible");
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = item.name;
    button.addEventListener("click", () => {
      dom.guessInput.value = item.name;
      hideAutocomplete();
      dom.guessInput.focus();
    });
    li.appendChild(button);
    dom.autocompleteList.appendChild(li);
  }
  dom.autocompleteList.classList.add("visible");
}

async function fetchAutocomplete() {
  const query = dom.guessInput.value.trim();
  if (!state.lobbyId || query.length < 3 || dom.guessInput.disabled) {
    hideAutocomplete();
    return;
  }
  const params = new URLSearchParams({
    q: query,
    lobby_id: state.lobbyId,
    limit: "10",
  });
  const payload = await apiRequest(`/api/autocomplete?${params.toString()}`);
  renderAutocomplete(payload.suggestions || []);
}

function scheduleAutocomplete() {
  if (state.autocompleteHandle) {
    clearTimeout(state.autocompleteHandle);
  }
  state.autocompleteHandle = setTimeout(() => {
    runAction(fetchAutocomplete);
  }, 180);
}

async function createLobby() {
  const hostName = dom.createHostName.value.trim();
  const difficulty = dom.difficultySelect.value;
  if (!hostName) {
    setMessage("Enter a host name.");
    return;
  }

  const payload = await apiRequest("/api/lobby/create", "POST", {
    host_name: hostName,
    difficulty,
  });

  state.lobbyId = payload.lobby_id;
  state.token = payload.host_token;
  state.isHost = true;
  state.started = false;
  state.gameOver = false;
  state.clueIndex = 0;
  dom.joinLobbyCode.value = payload.lobby_id;

  showScreen("lobby");
  if (payload.state) {
    renderLobby(payload.state);
  }
  startPolling();
}

async function joinLobby() {
  const lobbyId = dom.joinLobbyCode.value.trim().toUpperCase();
  const playerName = dom.joinPlayerName.value.trim();
  if (!lobbyId || !playerName) {
    setMessage("Enter lobby code and player name.");
    return;
  }

  const payload = await apiRequest("/api/lobby/join", "POST", {
    lobby_id: lobbyId,
    player_name: playerName,
  });

  state.lobbyId = lobbyId;
  state.token = payload.player_token;
  state.isHost = Boolean(payload.state && payload.state.is_host);
  state.started = Boolean(payload.state && payload.state.started);
  state.gameOver = false;
  state.clueIndex = 0;

  showScreen("lobby");
  if (payload.state) {
    renderLobby(payload.state);
  }
  startPolling();
}

async function startGame() {
  if (!state.lobbyId || !state.token) {
    setMessage("Lobby session missing.");
    return;
  }
  const payload = await apiRequest(
    `/api/lobby/${encodeURIComponent(state.lobbyId)}/start`,
    "POST",
    { host_token: state.token },
  );
  if (payload.state) {
    renderGame(payload.state);
  }
}

async function submitGuess() {
  if (!state.lobbyId || !state.token || dom.guessInput.disabled) {
    return;
  }
  const guessText = dom.guessInput.value.trim();
  if (!guessText) {
    setMessage("Type a guess first.");
    return;
  }
  const payload = await apiRequest(
    `/api/game/${encodeURIComponent(state.lobbyId)}/submit_guess`,
    "POST",
    {
      player_token: state.token,
      guess_text: guessText,
    },
  );

  if (!payload.accepted && payload.reason) {
    dom.guessStatusText.textContent = payload.reason;
  } else if (payload.correct) {
    dom.guessStatusText.textContent = "Correct! You're locked for the rest of this game.";
  } else {
    dom.guessStatusText.textContent = "Waiting for others...";
  }
  hideAutocomplete();
  await refreshState();
}

async function shareLobbyLink() {
  if (!state.lobbyId) {
    return;
  }
  const shareUrl = buildShareUrl(state.lobbyId);
  try {
    await navigator.clipboard.writeText(shareUrl);
    setMessage("Share link copied.", false);
  } catch {
    setMessage("Clipboard blocked. Copy manually: " + shareUrl);
  }
}

async function playAgain() {
  if (!state.isHost || !state.lobbyId || !state.token) {
    return;
  }
  const payload = await apiRequest(
    `/api/lobby/${encodeURIComponent(state.lobbyId)}/start`,
    "POST",
    { host_token: state.token },
  );
  state.started = true;
  state.gameOver = false;
  state.clueIndex = 0;
  if (payload.state) {
    renderGame(payload.state);
  }
}

function resetSession() {
  stopPolling();
  hideAutocomplete();
  state.lobbyId = null;
  state.token = null;
  state.isHost = false;
  state.started = false;
  state.gameOver = false;
  state.clueIndex = 0;
  state.latestLobby = null;
  state.latestGame = null;
  dom.guessInput.value = "";
  dom.guessStatusText.textContent = "";
  dom.answerText.textContent = "-";
  dom.scoreboardList.innerHTML = "";
  dom.finalScoreList.innerHTML = "";
  history.replaceState({}, "", window.location.pathname);
}

async function runAction(action) {
  try {
    setMessage("");
    await action();
  } catch (error) {
    setMessage(`Error: ${error.message}`);
  }
}

function attachEvents() {
  dom.playBtn.addEventListener("click", () => showScreen("setup"));
  dom.backToMenuBtn.addEventListener("click", () => {
    resetSession();
    showScreen("menu");
  });
  dom.lobbyBackBtn.addEventListener("click", () => {
    resetSession();
    showScreen("setup");
  });
  dom.endBackToMenuBtn.addEventListener("click", () => {
    resetSession();
    showScreen("menu");
  });

  dom.createLobbyBtn.addEventListener("click", () => runAction(createLobby));
  dom.joinLobbyBtn.addEventListener("click", () => runAction(joinLobby));
  dom.startGameBtn.addEventListener("click", () => runAction(startGame));
  dom.shareLinkBtn.addEventListener("click", () => runAction(shareLobbyLink));
  dom.guessBtn.addEventListener("click", () => runAction(submitGuess));
  dom.playAgainBtn.addEventListener("click", () => runAction(playAgain));

  dom.guessInput.addEventListener("input", () => {
    if (dom.guessInput.value.trim().length < 3) {
      hideAutocomplete();
      return;
    }
    scheduleAutocomplete();
  });
  dom.guessInput.addEventListener("blur", () => {
    setTimeout(() => hideAutocomplete(), 120);
  });
}

function init() {
  dom.apiBase.value = resolveDefaultApiBase();
  attachEvents();

  const lobbyFromUrl = new URLSearchParams(window.location.search).get("lobby");
  if (lobbyFromUrl) {
    dom.joinLobbyCode.value = lobbyFromUrl.toUpperCase();
    showScreen("setup");
    setMessage("Enter your name to join the shared lobby.", false);
  } else {
    showScreen("menu");
  }
}

init();
