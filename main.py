import asyncio
import json
import logging
import os
import random
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ───────────────────────── ENV (как на скрине) ─────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

LIMIT = int(os.getenv("LIMIT", "200"))          # окно сделок
POLL_SEC = float(os.getenv("POLL_SEC", "2.0"))  # период опроса (сек)

# Эти переменные у тебя есть в env, но они этому воркеру не нужны:
_ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
_ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

# ───────────────────────── GRINEX CONFIG ─────────────────────────
BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")

SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

# Должно совпадать с unique index в Supabase:
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

# локальная дедупликация (чтобы не слать одно и то же постоянно)
SEEN_MAX = int(os.getenv("SEEN_MAX", "30000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

# если хочешь полностью запретить runtime install — поставь 1
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"

# сколько подряд не-JSON/ошибок нужно, чтобы включить playwright fallback
NON_JSON_BEFORE_FALLBACK = int(os.getenv("NON_JSON_BEFORE_FALLBACK", "2"))

TRADES_URL = f"{BASE_URL}/api/v2/trades?market={MARKET}&limit={LIMIT}&order_by=desc"
TRADING_PAGE_URL = f"{BASE_URL}/trading/{MARKET}?lang=ru"

UA = os.getenv(
    "UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": TRADING_PAGE_URL,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("grinex-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

Q8 = Decimal("0.00000001")


# ───────────────────────── TYPES ─────────────────────────
@dataclass(frozen=True)
class TradeKey:
    tid: Optional[str]
    trade_time: str
    price: str
    volume_usdt: str


# ───────────────────────── HELPERS ─────────────────────────
def _as_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def _get_tid(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "tid", "trade_id", "tradeId"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    return None


def _parse_ts(obj: Dict[str, Any]) -> datetime:
    for k in ("created_at", "createdAt", "timestamp", "ts", "time", "at", "date"):
        v = obj.get(k)
        if v is None:
            continue

        if isinstance(v, (int, float)):
            sec = float(v) / 1000.0 if v > 10_000_000_000 else float(v)
            return datetime.fromtimestamp(sec, tz=timezone.utc)

        if isinstance(v, str):
            s = v.strip()
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    return datetime.now(timezone.utc)


def _extract_trades(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("trades", "data", "result", "items"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def normalize_trade(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Приводит trade к схеме exchange_trades:
      source, symbol, price, volume_usdt, volume_rub, trade_time
    trade_time — только HH:MM:SS (как у Rapira).
    """
    price = _as_decimal(obj.get("price"))
    amount = (
        _as_decimal(obj.get("amount"))
        or _as_decimal(obj.get("volume"))
        or _as_decimal(obj.get("qty"))
        or _as_decimal(obj.get("quantity"))
    )
    total = (
        _as_decimal(obj.get("total"))
        or _as_decimal(obj.get("funds"))
        or _as_decimal(obj.get("cost"))
        or _as_decimal(obj.get("quote_volume"))
        or _as_decimal(obj.get("quoteVolume"))
    )

    if price is None or amount is None:
        return None
    if total is None:
        total = price * amount

    if price <= 0 or amount <= 0 or total <= 0:
        return None

    ts_utc = _parse_ts(obj)
    # Важно: в БД у тебя time without tz, поэтому пишем время в МСК (как у Rapira).
    # Если нужно другое — меняй на UTC или свою TZ и синхронизируй с Rapira.
    trade_time = ts_utc.astimezone(timezone.utc).astimezone(
        timezone(datetime.now().astimezone().utcoffset() or timezone.utc.utcoffset(datetime.now()))
    )
    # На практике проще фиксировать Moscow offset: но для прод лучше оставим UTC->local не надо.
    # Поэтому делаем так: возьмём время по UTC+3 (Москва) жёстко:
    moscow = timezone.utc if False else timezone(timezone.utc.utcoffset(ts_utc) or timezone.utc.utcoffset(ts_utc))
    # Однако выше бессмысленно — оставим корректно: Moscow offset +03:00:
    moscow = timezone(timedelta(hours=3))  # type: ignore  # noqa

    trade_time_str = ts_utc.astimezone(moscow).strftime("%H:%M:%S")

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),
        "trade_time": trade_time_str,
        "_tid": _get_tid(obj),
        "_ts": ts_utc.timestamp(),
    }


def make_key(row: Dict[str, Any]) -> TradeKey:
    return TradeKey(
        tid=row.get("_tid"),
        trade_time=row["trade_time"],
        price=row["price"],
        volume_usdt=row["volume_usdt"],
    )


def chunked(xs: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [xs]
    return [xs[i : i + n] for i in range(0, len(xs), n)]


# ───────────────────────── SUPABASE ─────────────────────────
async def supabase_upsert(client: httpx.AsyncClient, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("SUPABASE_URL or SUPABASE_KEY not set; skipping insert.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": ON_CONFLICT}
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }

    payload = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    r = await client.post(url, headers=headers, params=params, json=payload)
    if r.status_code >= 300:
        logger.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
    else:
        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(payload), SUPABASE_TABLE)


async def preload_seen_from_supabase(client: httpx.AsyncClient, preload_limit: int = 1500) -> List[TradeKey]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {
        "select": "trade_time,price,volume_usdt",
        "source": f"eq.{SOURCE}",
        "symbol": f"eq.{SYMBOL}",
        "order": "created_at.desc",
        "limit": str(preload_limit),
    }

    r = await client.get(url, headers=headers, params=params)
    if r.status_code >= 300:
        logger.warning("Preload failed (%s): %s", r.status_code, r.text[:200])
        return []

    out: List[TradeKey] = []
    try:
        data = r.json()
        if isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    continue
                tt = row.get("trade_time")
                pr = row.get("price")
                vu = row.get("volume_usdt")
                if tt and pr and vu:
                    out.append(TradeKey(tid=None, trade_time=str(tt), price=str(pr), volume_usdt=str(vu)))
    except Exception:
        return []
    return out


# ───────────────────────── HTTP FETCH (основной) ─────────────────────────
async def fetch_window_httpx(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(TRADES_URL)
    ct = (r.headers.get("content-type") or "").lower()
    text = r.text or ""

    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}; ct={ct}; body={text[:200]!r}")

    if "application/json" not in ct:
        # на Render часто так выглядит Cloudflare/HTML
        raise RuntimeError(f"Non-JSON response; ct={ct}; body={text[:200]!r}")

    try:
        payload = r.json()
    except Exception:
        raise RuntimeError(f"JSON decode failed; ct={ct}; body={text[:200]!r}")

    raw = _extract_trades(payload)
    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    out.sort(key=lambda x: float(x.get("_ts", 0.0)))
    return out


# ───────────────────────── PLAYWRIGHT FALLBACK ─────────────────────────
def playwright_install() -> None:
    if SKIP_BROWSER_INSTALL:
        logger.info("SKIP_BROWSER_INSTALL=1, skipping playwright install.")
        return
    logger.warning("Installing Playwright browsers (runtime)...")
    r = subprocess.run(
        ["python", "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
        check=False,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        logger.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s", r.returncode, r.stdout, r.stderr)
    else:
        logger.info("Playwright browsers installed.")


async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    context = await browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=UA,
    )
    page = await context.new_page()
    await page.goto(TRADING_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1500)
    return browser, context, page


async def fetch_window_playwright(page: Page) -> List[Dict[str, Any]]:
    # Запрос к API из браузера (часто проходит, когда чистый httpx блокируют)
    resp = await page.request.get(TRADES_URL, headers=HEADERS)
    status = resp.status
    body = await resp.text()
    ct = (resp.headers.get("content-type") or "").lower()

    if status >= 400:
        raise RuntimeError(f"PW HTTP {status}; ct={ct}; body={body[:200]!r}")
    if "application/json" not in ct:
        raise RuntimeError(f"PW non-JSON; ct={ct}; body={body[:200]!r}")

    payload = json.loads(body)
    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    out.sort(key=lambda x: float(x.get("_ts", 0.0)))
    return out


async def safe_close(browser: Optional[Browser], context: Optional[BrowserContext], page: Optional[Page]) -> None:
    try:
        if page:
            await page.close()
    except Exception:
        pass
    try:
        if context:
            await context.close()
    except Exception:
        pass
    try:
        if browser:
            await browser.close()
    except Exception:
        pass


# ───────────────────────── WORKER LOOP ─────────────────────────
async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0
    non_json_streak = 0

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=20,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,
    ) as client:

        # preload seen, чтобы после рестарта не слать дублей пачкой
        pre = await preload_seen_from_supabase(client, preload_limit=1500)
        for k in pre:
            seen.add(k)
            seen_q.append(k)
        logger.info("Preloaded %d recent keys from Supabase | seen=%d", len(pre), len(seen))

        async with async_playwright() as pw:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            while True:
                t0 = time.time()
                try:
                    # 1) основной: httpx
                    window: List[Dict[str, Any]] = []
                    use_pw = non_json_streak >= NON_JSON_BEFORE_FALLBACK

                    if not use_pw:
                        window = await fetch_window_httpx(client)
                    else:
                        if page is None:
                            try:
                                browser, context, page = await open_browser(pw)
                            except Exception as e:
                                # если браузер не установлен — ставим и повторяем
                                playwright_install()
                                browser, context, page = await open_browser(pw)
                        window = await fetch_window_playwright(page)

                    # если дошли сюда — значит JSON норм
                    non_json_streak = 0
                    backoff = 2.0

                    if not window:
                        logger.warning("Empty window. url=%s", TRADES_URL)
                    else:
                        new_rows: List[Dict[str, Any]] = []
                        # вставляем старые -> новые
                        for row in window:
                            k = make_key(row)
                            if k in seen:
                                continue
                            new_rows.append(row)
                            seen.add(k)
                            seen_q.append(k)

                        # если set разъехался — пересоберём
                        if len(seen) > len(seen_q) + 500:
                            seen = set(seen_q)

                        if new_rows:
                            newest_clean = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                            logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest_clean, ensure_ascii=False))
                            for batch in chunked(new_rows, UPSERT_BATCH):
                                await supabase_upsert(client, batch)
                        else:
                            logger.info("No new trades.")

                    now = time.time()
                    if now - last_heartbeat >= HEARTBEAT_SEC:
                        logger.info(
                            "Heartbeat: alive | market=%s | poll=%.2fs | seen=%d | mode=%s",
                            MARKET, POLL_SEC, len(seen), ("playwright" if use_pw else "httpx"),
                        )
                        last_heartbeat = now

                    # sleep with jitter
                    dt = time.time() - t0
                    sleep_s = max(0.3, POLL_SEC - dt + random.uniform(-0.15, 0.15))
                    await asyncio.sleep(sleep_s)

                except Exception as e:
                    msg = str(e)
                    # считаем "не JSON / блокировки" как повод включить fallback
                    if "Non-JSON" in msg or "JSON decode failed" in msg or "403" in msg or "429" in msg:
                        non_json_streak += 1
                        logger.error("Fetch error (streak=%d): %s", non_json_streak, msg)
                    else:
                        logger.error("Worker error: %s", msg)

                    # если в playwright-режиме упали — перезапустим браузер
                    if page is not None:
                        await safe_close(browser, context, page)
                        browser = context = page = None

                    logger.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
