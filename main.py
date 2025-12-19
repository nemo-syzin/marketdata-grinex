import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx

try:
    # py3.9+
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# ───────────────────────── ENV (как на скрине) ─────────────────────────
# Render env (как у тебя):
# LIMIT
# POLL_SEC
# SUPABASE_URL
# SUPABASE_KEY
# SUPABASE_TABLE
#
# Дополнительно для Grinex:
# GRINEX_MARKET (usdta7a5)
# GRINEX_BASE_URL (https://grinex.io)
# GRINEX_PROXY (опц.) или HTTP_PROXY/HTTPS_PROXY
# SOURCE (опц., по умолчанию grinex)
# SYMBOL (опц., по умолчанию USDT/RUB)
# TIMEZONE (опц., по умолчанию Europe/Moscow)
# SEEN_MAX, HEARTBEAT_SEC, UPSERT_BATCH (опц.)

BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")

# под твой скрин
LIMIT = int(os.getenv("LIMIT", "200"))            # window size из API
POLL_SEC = float(os.getenv("POLL_SEC", "1.5"))    # poll interval

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# доп. опции
SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

# unique index в БД должен совпасть с этим on_conflict
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))

# Прокси:
# 1) если задан GRINEX_PROXY — используем его
# 2) иначе trust_env=True подхватит HTTP_PROXY/HTTPS_PROXY
GRINEX_PROXY = os.getenv("GRINEX_PROXY", "").strip() or None

TRADES_URL = f"{BASE_URL}/api/v2/trades?market={MARKET}&limit={LIMIT}&order_by=desc"

HEADERS = {
    "User-Agent": os.getenv(
        "UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{BASE_URL}/trading/{MARKET}?lang=ru",
}


# ───────────────────────── LOGGING ─────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("grinex-worker")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ───────────────────────── DECIMALS ─────────────────────────
Q8 = Decimal("0.00000001")


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def _as_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", ".").replace(" ", "").replace("\xa0", "")
        if not s:
            return None
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _tzinfo() -> timezone:
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE)  # type: ignore
    except Exception:
        return timezone.utc


TZ = _tzinfo()


# ───────────────────────── MODEL / DEDUP ─────────────────────────
@dataclass(frozen=True)
class TradeKey:
    tid: Optional[str]
    trade_time: str
    price: str
    volume_usdt: str


def _get_tid(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "tid", "trade_id"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    return None


def _parse_ts(obj: Dict[str, Any]) -> datetime:
    # Grinex обычно отдаёт created_at, но страхуемся
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
    amount = _as_decimal(obj.get("amount")) or _as_decimal(obj.get("volume"))
    total = _as_decimal(obj.get("total")) or _as_decimal(obj.get("funds")) or _as_decimal(obj.get("quote_volume"))

    if price is None or amount is None:
        return None
    if total is None:
        total = price * amount

    if price <= 0 or amount <= 0 or total <= 0:
        return None

    ts_utc = _parse_ts(obj)
    local = ts_utc.astimezone(TZ)
    trade_time = local.strftime("%H:%M:%S")  # time without tz в БД

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),  # у тебя NOT NULL; total = quote объём
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
        logger.error("Supabase upsert failed (%s): %s", r.status_code, r.text[:500])
    else:
        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(payload), SUPABASE_TABLE)


# ───────────────────────── FETCH GRINEX ─────────────────────────
async def fetch_window(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(TRADES_URL)

    ct = (r.headers.get("content-type") or "").lower()
    if r.status_code >= 400:
        raise RuntimeError(f"Grinex HTTP {r.status_code} | ct={ct} | body={r.text[:300]!r}")

    # Главное: на Render ты ловишь HTML (anti-bot). Это место обрабатывает корректно.
    if "application/json" not in ct:
        snippet = (r.text or "")[:350].replace("\n", "\\n")
        logger.warning("Non-JSON response from Grinex | HTTP %s | ct=%s | body=%s", r.status_code, ct, snippet)
        raise RuntimeError(f"Non-JSON from Grinex | ct={ct}")

    payload = r.json()
    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    return out


# ───────────────────────── WORKER LOOP ─────────────────────────
async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=20,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,     # подхватит HTTP_PROXY/HTTPS_PROXY
        proxy=GRINEX_PROXY  # если задан — используем явно
    ) as client:
        while True:
            t0 = time.time()
            try:
                window = await fetch_window(client)

                if not window:
                    logger.warning("Empty window from API. url=%s", TRADES_URL)
                else:
                    # API обычно "новые сверху". Пишем в БД старые → новые.
                    new_rows: List[Dict[str, Any]] = []
                    for row in reversed(window):
                        k = make_key(row)
                        if k in seen:
                            continue
                        new_rows.append(row)
                        seen.add(k)
                        seen_q.append(k)

                    # защита от рассинхронизации set/deque
                    if len(seen) > len(seen_q) + 200:
                        seen = set(seen_q)

                    if new_rows:
                        newest = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                        logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest, ensure_ascii=False))
                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(client, batch)
                    else:
                        logger.info("No new trades.")

                # heartbeat, чтобы воркер “не молчал”
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    logger.info(
                        "Heartbeat: alive | market=%s | poll=%.2fs | url=%s | seen=%d",
                        MARKET, POLL_SEC, TRADES_URL, len(seen)
                    )
                    last_heartbeat = now

                backoff = 2.0
                # небольшой джиттер, чтобы не долбиться в ровный ритм
                dt = time.time() - t0
                sleep_s = max(0.5, (POLL_SEC - dt) + random.uniform(-0.15, 0.15))
                await asyncio.sleep(sleep_s)

            except Exception as e:
                logger.error("Worker error: %s", e)
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
