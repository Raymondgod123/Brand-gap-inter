"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const { chromium } = require("playwright");

function detectRobotCheck(pageTitle, loweredHtml) {
  const loweredTitle = (pageTitle || "").toLowerCase();
  if (loweredTitle.includes("robot check")) {
    return true;
  }

  const markers = [
    "captchacharacters",
    "enter the characters you see below",
    "type the characters you see in this image",
    "sorry, we just need to make sure you're not a robot",
    "to discuss automated access to amazon data please contact",
  ];
  return markers.some((marker) => loweredHtml.includes(marker));
}

function summarizeOfferSignals(loweredHtml) {
  return {
    has_no_featured_offers: loweredHtml.includes("no featured offers available"),
    has_buying_options: loweredHtml.includes("see all buying options"),
    has_currently_unavailable: loweredHtml.includes("currently unavailable"),
    has_price_to_pay_block: loweredHtml.includes('id="pricetopay"') || loweredHtml.includes('id="pricetopay_feature_div"'),
    has_core_price_block:
      loweredHtml.includes('id="corepricedisplay_desktop_feature_div"') ||
      loweredHtml.includes('id="coreprice_feature_div"'),
  };
}

function resolveChromiumExecutablePath() {
  const explicit = process.env.BRAND_GAP_CHROMIUM_EXECUTABLE;
  if (explicit && fs.existsSync(explicit)) {
    return explicit;
  }

  const defaultPath = chromium.executablePath();
  if (defaultPath && fs.existsSync(defaultPath)) {
    return defaultPath;
  }

  const localAppData = process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local");
  const playwrightRoot = path.join(localAppData, "ms-playwright");
  if (fs.existsSync(playwrightRoot)) {
    const chromiumDirs = fs
      .readdirSync(playwrightRoot, { withFileTypes: true })
      .filter((entry) => entry.isDirectory() && entry.name.startsWith("chromium-"))
      .map((entry) => entry.name)
      .sort()
      .reverse();

    for (const dirName of chromiumDirs) {
      const candidate = path.join(playwrightRoot, dirName, "chrome-win64", "chrome.exe");
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }

  const installedBrowserCandidates = [
    path.join("C:", "Program Files", "Google", "Chrome", "Application", "chrome.exe"),
    path.join("C:", "Program Files (x86)", "Google", "Chrome", "Application", "chrome.exe"),
    path.join(localAppData, "Google", "Chrome", "Application", "chrome.exe"),
    path.join("C:", "Program Files", "Microsoft", "Edge", "Application", "msedge.exe"),
    path.join("C:", "Program Files (x86)", "Microsoft", "Edge", "Application", "msedge.exe"),
    path.join(localAppData, "Microsoft", "Edge", "Application", "msedge.exe"),
  ];
  for (const candidate of installedBrowserCandidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return undefined;
}

async function main() {
  const targetUrl = process.argv[2];
  const timeoutMs = Number.parseInt(process.argv[3] || "45000", 10);
  if (!targetUrl) {
    throw new Error("expected target URL");
  }

  const executablePath = resolveChromiumExecutablePath();
  const browser = await chromium.launch({ headless: true, executablePath });
  const context = await browser.newContext();
  const page = await context.newPage();

  const startedAt = Date.now();
  let gotoDurationMs = 0;
  let settleDurationMs = 0;

  try {
    const gotoStartedAt = Date.now();
    const response = await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
    gotoDurationMs = Date.now() - gotoStartedAt;

    await page.waitForSelector("body", { timeout: Math.min(timeoutMs, 10000) });

    const settleStartedAt = Date.now();
    let waitStrategy = "domcontentloaded+body";
    try {
      await page.waitForLoadState("networkidle", { timeout: 5000 });
      waitStrategy = "domcontentloaded+body+networkidle";
    } catch (error) {
      await page.waitForTimeout(1200);
      waitStrategy = "domcontentloaded+body+bounded_settle";
    }
    settleDurationMs = Date.now() - settleStartedAt;

    const pageTitle = await page.title();
    const html = await page.content();
    const loweredHtml = html.toLowerCase();
    const readyState = await page.evaluate(() => document.readyState);

    const payload = {
      final_url: page.url(),
      status_code: response ? response.status() : null,
      page_title: pageTitle || null,
      html,
      is_robot_check: detectRobotCheck(pageTitle, loweredHtml),
      capture_diagnostics: {
        navigation_ok: Boolean(response),
        ready_state: readyState,
        wait_strategy: waitStrategy,
        timing_ms: {
          goto: gotoDurationMs,
          settle: settleDurationMs,
          total: Date.now() - startedAt,
        },
        visible_offer_signals: summarizeOfferSignals(loweredHtml),
      },
    };

    process.stdout.write(JSON.stringify(payload));
  } finally {
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  const message = error && error.stack ? error.stack : String(error);
  process.stderr.write(message);
  process.exit(1);
});
