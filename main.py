import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx
from curl_cffi import requests as crequests

# ───────────────────────── ENV (как у тебя на скрине) ─────────────────────────

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "") or ""
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades") or "exchange_trades"

LIMIT = int(os.getenv("LIMIT", "200"))          # window size from API
POLL_SEC = float(os.getenv("POLL_SEC", "2"))    # polling interval

HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")

# ───────────────────────── GRINEX CONFIG ─────────────────────────

BASE_URL = (os.getenv("GRINEX_BASE_URL", "https://grinex.io") or "https://grinex.io").rstrip("/")
MARKET = os.getenv("GRINEX_MARKET", "usdta7a5") or "usdta7a5"

SOURCE = os.getenv("SOURCE", "grinex") or "grinex"
SYMBOL = os.getenv("SYMBOL", "USDT/RUB") or "USDT/RUB"

# Калининград/МСК в БД у тебя time без tz. Для консистентности оставим +2 (KGD).
# Если хочешь МСК — поставь TIME_OFFSET_HOURS=3
TIME_OFFSET_HOURS = int(os.getenv("TIME_OFFSET_HOURS", "2"))
LOCAL_TZ = timezone(timedelta(hours=TIME_OFFSET_HOURS))

# Supabase unique index
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt") or "source,symbol,trade_time,price,volume_usdt"

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

TRADES_URL = f"{BASE_URL}/api/v2/trades?market={MARKET}&limit={LIMIT}&order_by=desc"

HEADERS = {
    "User-Agent": os.getenv(
        "UA",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{BASE_URL}/trading/{MARKET}?lang=ru",
}

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("grinex-worker")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

Q8 = Decimal("0.00000001")


# ───────────────────────── MODEL ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    tid: Optional[str]
    trade_time: str
    price: str
    volume_usdt: str


# ───────────────────────── HELPERS ─────────────────────────

def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def _as_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", ".").replace(" ", "")
        if not s:
            return None
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _parse_ts(obj: Dict[str, Any]) -> datetime:
    # Grinex API обычно отдаёт created_at, но подстрахуемся
    for k in ("created_at", "timestamp", "ts", "time", "at", "date"):
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


def _get_tid(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "tid", "trade_id"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    return None


def _extract_trades(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("trades", "data", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def normalize_trade(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    price = _as_decimal(obj.get("price"))
    amount = _as_decimal(obj.get("amount")) or _as_decimal(obj.get("volume"))  # base (USDT)
    total = _as_decimal(obj.get("total")) or _as_decimal(obj.get("funds")) or _as_decimal(obj.get("quote_volume"))

    if price is None or amount is None:
        return None
    if total is None:
        total = price * amount
    if price <= 0 or amount <= 0 or total <= 0:
        return None

    ts_utc = _parse_ts(obj)
    local = ts_utc.astimezone(LOCAL_TZ)
    trade_time = local.strftime("%H:%M:%S")  # time without tz (как у rapira)

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),
        "trade_time": trade_time,
        "_tid": _get_tid(obj),
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
    return [xs[i:i + n] for i in range(0, len(xs), n)]


# ───────────────────────── GRINEX FETCH (curl-cffi) ─────────────────────────

class NonJsonError(RuntimeError):
    pass


def _proxy_dict() -> Optional[Dict[str, str]]:
    # curl-cffi ожидает dict, но примет и одну строку; делаем явно.
    # Если ты выставил HTTP_PROXY/HTTPS_PROXY — берём их.
    if HTTP_PROXY or HTTPS_PROXY:
        p: Dict[str, str] = {}
        if HTTP_PROXY:
            p["http"] = HTTP_PROXY
        if HTTPS_PROXY:
            p["https"] = HTTPS_PROXY
        # если задан только один — продублируем
        if "http" in p and "https" not in p:
            p["https"] = p["http"]
        if "https" in p and "http" not in p:
            p["http"] = p["https"]
        return p
    return None


def fetch_window_sync() -> List[Dict[str, Any]]:
    """
    Синхронная функция под asyncio.to_thread().
    Делает запрос через curl-cffi с impersonate Chrome (TLS fingerprint).
    """
    proxies = _proxy_dict()

    r = crequests.get(
        TRADES_URL,
        headers=HEADERS,
        proxies=proxies,
        timeout=20,
        impersonate="chrome124",
    )

    ct = (r.headers.get("content-type") or "").lower()
    body = r.text or ""

    # Иногда сервер отдаёт json без content-type; проверим по первому символу.
    looks_like_json = body.lstrip().startswith("{") or body.lstrip().startswith("[")
    is_json_ct = "application/json" in ct or "text/json" in ct

    if not (is_json_ct or looks_like_json):
        snippet = body[:600].replace("\n", "\\n")
        logger.warning("Non-JSON response from Grinex | HTTP %s | ct=%s | body=%s", r.status_code, ct or "?", snippet)
        raise NonJsonError(f"Non-JSON from Grinex | ct={ct or '?'}")

    try:
        payload = r.json()
    except Exception as e:
        snippet = body[:600].replace("\n", "\\n")
        logger.warning("JSON parse failed | HTTP %s | ct=%s | body=%s", r.status_code, ct or "?", snippet)
        raise NonJsonError(f"JSON parse failed: {e}")

    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)
    return out


async def fetch_window() -> List[Dict[str, Any]]:
    return await asyncio.to_thread(fetch_window_sync)


# ───────────────────────── SUPABASE UPSERT ─────────────────────────

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


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0

    # httpx тут только для Supabase; прокси ему не мешают
    async with httpx.AsyncClient(
        timeout=30,
        verify=certifi.where(),
        follow_redirects=True,
        trust_env=True,
    ) as supa:
        logger.info(
            "Starting grinex worker | market=%s | url=%s | poll=%.2fs | limit=%d | proxy=%s",
            MARKET,
            TRADES_URL,
            POLL_SEC,
            LIMIT,
            "ON" if (HTTP_PROXY or HTTPS_PROXY) else "OFF",
        )

        while True:
            t0 = time.time()
            try:
                window = await fetch_window()

                if not window:
                    logger.warning("Empty window from API. url=%s", TRADES_URL)
                else:
                    # новые сверху -> пишем старые -> новые
                    new_rows: List[Dict[str, Any]] = []
                    for row in reversed(window):
                        k = make_key(row)
                        if k in seen:
                            continue
                        new_rows.append(row)
                        seen.add(k)
                        seen_q.append(k)

                    if len(seen) > len(seen_q) + 200:
                        seen = set(seen_q)

                    if new_rows:
                        newest = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                        logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest, ensure_ascii=False))
                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(supa, batch)
                    else:
                        logger.info("No new trades.")

                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    logger.info("Heartbeat: alive | market=%s | seen=%d", MARKET, len(seen))
                    last_heartbeat = now

                backoff = 2.0
                dt = time.time() - t0

                # небольшой джиттер, чтобы не бить ровно по таймингу
                sleep_s = max(0.5, POLL_SEC - dt) + random.uniform(0.0, 0.25)
                await asyncio.sleep(sleep_s)

            except NonJsonError as e:
                # это именно блок/антибот
                logger.error("Worker error: %s", e)
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(120.0, backoff * 2)

            except Exception as e:
                logger.error("Worker error (unexpected): %s", e)
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
