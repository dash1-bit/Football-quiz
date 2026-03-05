const { chromium } = require("playwright");

const FRONTEND_URL = process.env.FRONTEND_URL || "https://football-quiz-7oi.pages.dev/";
const EXPECTED_API_BASE = "https://football-quiz-t7m8.onrender.com";
const API_BASE_OVERRIDE = process.env.API_BASE_OVERRIDE || "";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function run() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
    await page.goto(FRONTEND_URL, { waitUntil: "domcontentloaded", timeout: 30000 });

    await page.click('[data-testid="play-btn"]');
    await page.waitForSelector('[data-testid="screen-setup"].active', { timeout: 10000 });

    const apiBase = await page.$eval('[data-testid="api-base-input"]', (el) => el.value.trim());
    assert(
      apiBase === EXPECTED_API_BASE,
      `API base mismatch: expected ${EXPECTED_API_BASE}, got ${apiBase || "<empty>"}`,
    );
    if (API_BASE_OVERRIDE) {
      await page.fill('[data-testid="api-base-input"]', API_BASE_OVERRIDE);
    }

    const hostName = `codex-host-${Date.now().toString().slice(-5)}`;
    await page.fill('[data-testid="create-host-name"]', hostName);
    await page.selectOption('[data-testid="difficulty-select"]', "normal");
    await page.click('[data-testid="create-lobby-btn"]');

    await page.waitForSelector('[data-testid="screen-lobby"].active', { timeout: 15000 });
    const lobbyCode = await page.$eval('[data-testid="lobby-code-text"]', (el) =>
      (el.textContent || "").trim(),
    );
    assert(/^[A-Z0-9]{6,12}$/.test(lobbyCode), `Unexpected lobby code: ${lobbyCode}`);

    const playerCount = await page.$eval('[data-testid="player-count-text"]', (el) =>
      (el.textContent || "").trim(),
    );
    assert(playerCount.includes("/10"), `Player count text invalid: ${playerCount}`);

    await page.click('[data-testid="share-link-btn"]');

    await page.waitForFunction(
      () => {
        const btn = document.querySelector('[data-testid="start-game-btn"]');
        return Boolean(btn && !btn.disabled);
      },
      undefined,
      { timeout: 20000 },
    );
    await page.click('[data-testid="start-game-btn"]');

    await page.waitForSelector('[data-testid="screen-game"].active', { timeout: 15000 });
    const clueText = await page.$eval('[data-testid="clue-text"]', (el) =>
      (el.textContent || "").trim(),
    );
    assert(clueText.length > 4 && !/waiting/i.test(clueText), `Invalid clue text: ${clueText}`);

    const timerText = await page.$eval('[data-testid="timer-text"]', (el) =>
      (el.textContent || "").trim(),
    );
    assert(/\d+s/.test(timerText), `Timer not visible: ${timerText}`);

    console.log(
      JSON.stringify(
        {
          ok: true,
          frontend_url: FRONTEND_URL,
          api_base: apiBase,
          lobby_code: lobbyCode,
          player_count_text: playerCount,
          timer_text: timerText,
        },
        null,
        2,
      ),
    );
  } finally {
    await browser.close();
  }
}

run().catch((error) => {
  console.error(`E2E failure: ${error.message}`);
  process.exit(1);
});
