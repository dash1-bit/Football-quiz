const { chromium } = require("playwright");

const FRONTEND_URL = process.env.FRONTEND_URL || "https://football-quiz-7oi.pages.dev/";
const EXPECTED_API_BASE = "https://football-quiz-t7m8.onrender.com";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function readValue(page, selector) {
  await page.waitForSelector(selector, { timeout: 15000 });
  return page.$eval(selector, (el) => el.value.trim());
}

async function waitForText(page, selector, matcher, timeout = 15000) {
  await page.waitForFunction(
    ({ sel, expected }) => {
      const node = document.querySelector(sel);
      if (!node) return false;
      const text = (node.textContent || "").trim();
      return new RegExp(expected, "i").test(text);
    },
    { sel: selector, expected: matcher.source },
    { timeout },
  );
}

async function run() {
  const browser = await chromium.launch({ headless: true });
  const hostContext = await browser.newContext();
  const joinContext = await browser.newContext();
  const hostPage = await hostContext.newPage();
  const joinPage = await joinContext.newPage();

  try {
    await Promise.all([
      hostPage.goto(FRONTEND_URL, { waitUntil: "domcontentloaded", timeout: 30000 }),
      joinPage.goto(FRONTEND_URL, { waitUntil: "domcontentloaded", timeout: 30000 }),
    ]);

    const hostApiBase = await readValue(hostPage, "#apiBase");
    const joinApiBase = await readValue(joinPage, "#apiBase");
    assert(
      hostApiBase === EXPECTED_API_BASE,
      `Host page API base mismatch: expected ${EXPECTED_API_BASE}, got ${hostApiBase || "<empty>"}`,
    );
    assert(
      joinApiBase === EXPECTED_API_BASE,
      `Join page API base mismatch: expected ${EXPECTED_API_BASE}, got ${joinApiBase || "<empty>"}`,
    );
    assert(!hostApiBase.includes("localhost"), `Host page still points to localhost: ${hostApiBase}`);

    const hostName = `codex-host-${Date.now().toString().slice(-6)}`;
    await hostPage.fill("#createName", hostName);
    await hostPage.click("#createBtn");

    await hostPage.waitForFunction(
      () => {
        const input = document.querySelector("#joinGameId");
        return Boolean(input && input.value && input.value.trim().length >= 6);
      },
      undefined,
      { timeout: 20000 },
    );

    const gameId = await readValue(hostPage, "#joinGameId");
    assert(/^[A-Z0-9]{6,12}$/.test(gameId), `Unexpected game ID format: ${gameId}`);
    await waitForText(hostPage, "#sessionInfo", new RegExp(`Game\\s+${gameId}`), 15000);

    await joinPage.fill("#joinGameId", gameId);
    await joinPage.fill("#joinName", "codex-player");
    await joinPage.click("#joinBtn");
    await waitForText(joinPage, "#sessionInfo", new RegExp(`Game\\s+${gameId}`), 15000);

    await hostPage.click("#startBtn");
    await hostPage.waitForFunction(
      () => document.querySelectorAll("#clueList li").length >= 1,
      undefined,
      { timeout: 15000 },
    );
    const firstClues = await hostPage.$$eval("#clueList li", (els) =>
      els.map((el) => (el.textContent || "").trim()).filter(Boolean),
    );
    assert(firstClues.length >= 1, "No clues shown after starting round.");

    const initialCount = firstClues.length;
    await hostPage.click("#nextClueBtn");
    await hostPage.waitForFunction(
      (prev) => document.querySelectorAll("#clueList li").length > prev,
      initialCount,
      { timeout: 15000 },
    );

    await joinPage.fill("#guessInput", "Lionel Messi");
    await joinPage.click("#guessBtn");
    await waitForText(joinPage, "#statusBox", /(Incorrect guess|Correct guess)/i, 15000);
    const finalStatus = await joinPage.$eval("#statusBox", (el) => (el.textContent || "").trim());

    console.log(
      JSON.stringify(
        {
          ok: true,
          frontend_url: FRONTEND_URL,
          api_base: hostApiBase,
          game_id: gameId,
          clues_before_next: initialCount,
          status_after_guess: finalStatus,
        },
        null,
        2,
      ),
    );
  } finally {
    await joinContext.close();
    await hostContext.close();
    await browser.close();
  }
}

run().catch((error) => {
  console.error(`E2E failure: ${error.message}`);
  process.exit(1);
});
