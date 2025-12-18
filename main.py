import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple

import certifi
import httpx

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# ───────────────────────── CONFIG ─────────────────────────

GRINEX_BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
GRINEX_MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")
GRINEX_LIMIT = int(os.getenv("GRINEX_LIMIT", "200"))
GRINEX_POLL_SECONDS = float(os.getenv("GRINEX_POLL_SECONDS", "1.5"))

# Если Grinex режет Render — укажи прокси (например, http://user:pass@host:port)
GRINEX_PROXY = os.getenv("GRINEX_PROXY", "").strip()

SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

# ВАЖНО: endpoint как у твоей локальной версии
TRADES_URL = f"{GRINEX_BASE_URL}/api/v2/trades?market={GRINEX_MARKET}&limit={GRINEX_LIMIT}&order_by=desc"

HEADERS = {
    "User-Agent": os.getenv(
        "UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{GRINEX_BASE_URL}/trading/{GRINEX_MARKET}?lang=ru",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("grinex-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

Q8 = Decimal("0.00000001")


# ───────────────────────── TYPES ─────────────────────────

@dataclass(frozen=True)
class SeenKey:
    # локальная дедупликация (не заменяет ON_CONFLICT в БД)
    tid: Optional[str]
    trade_time: str
    price: str
    volume_usdt: str


# ───────────────────────── HELPERS ─────────────────────────

def _tzinfo():
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE)  # type: ignore
    except Exception:
        return timezone.utc


TZ = _tzinfo()


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


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def _extract_trades(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("trades", "data", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _get_tid(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "tid", "trade_id"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    return None


def _parse_ts(obj: Dict[str, Any]) -> datetime:
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
    trade_time = local.strftime("%H:%M:%S")  # как у Rapira: time without tz

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),   # в твоей таблице volume_rub NOT NULL
        "trade_time": trade_time,
        "_tid": _get_tid(obj),         # внутреннее поле
    }


def make_seen_key(row: Dict[str, Any]) -> SeenKey:
    return SeenKey(
        tid=row.get("_tid"),
        trade_time=row["trade_time"],
        price=row["price"],
        volume_usdt=row["volume_usdt"],
    )


def chunked(rows: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [rows]
    return [rows[i:i + n] for i in range(0, len(rows), n)]


class NonJsonFromGrinex(RuntimeError):
    pass


# ───────────────────────── NETWORK ─────────────────────────

async def fetch_window(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(TRADES_URL)
    ct = (r.headers.get("content-type") or "").lower()

    # На Render Grinex часто возвращает HTML-страницу защиты (как у тебя в логах)
    if "application/json" not in ct:
        body = (r.text or "")[:600].replace("\n", "\\n")
        logger.warning("Non-JSON response from Grinex | HTTP %s | ct=%s | body=%s", r.status_code, ct, body)
        raise NonJsonFromGrinex(f"Grinex returned non-JSON ct={ct}")

    payload = r.json()
    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    # обычно new->old, но порядок нам важен на вставку: old->new сделаем позже
    return out


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

    resp = await client.post(url, headers=headers, params=params, json=payload)
    if resp.status_code >= 300:
        logger.error("Supabase upsert failed (%s): %s", resp.status_code, resp.text)
    else:
        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(payload), SUPABASE_TABLE)


# ───────────────────────── WORKER ─────────────────────────

async def worker() -> None:
    seen: Set[SeenKey] = set()
    backoff = 2.0
    last_hb = 0.0

    # Если задан GRINEX_PROXY — используем его.
    # Иначе trust_env=True позволит взять HTTPS_PROXY/HTTP_PROXY из окружения Render.
    proxy_arg = GRINEX_PROXY or None

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=15,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,
        proxy=proxy_arg,
    ) as client:
        while True:
            t0 = time.time()
            try:
                window = await fetch_window(client)

                # old -> new
                new_rows: List[Dict[str, Any]] = []
                for row in reversed(window):
                    k = make_seen_key(row)
                    if k in seen:
                        continue
                    new_rows.append(row)
                    seen.add(k)
                    if len(seen) > SEEN_MAX:
                        # простая “очистка”: оставляем ключи только из текущего окна
                        seen = {make_seen_key(r) for r in window}

                if new_rows:
                    newest = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                    logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest, ensure_ascii=False))
                    for batch in chunked(new_rows, UPSERT_BATCH):
                        await supabase_upsert(client, batch)
                else:
                    # не спамим постоянно
                    pass

                now = time.time()
                if now - last_hb >= HEARTBEAT_SEC:
                    logger.info(
                        "Heartbeat: alive | market=%s | poll=%.2fs | url=%s | window=%d | seen=%d | proxy=%s",
                        GRINEX_MARKET, GRINEX_POLL_SECONDS, TRADES_URL, len(window), len(seen), "yes" if (GRINEX_PROXY or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")) else "no"
                    )
                    last_hb = now

                backoff = 2.0
                dt = time.time() - t0
                sleep_s = max(0.35, GRINEX_POLL_SECONDS - dt)
                sleep_s += random.uniform(-0.10, 0.10)
                await asyncio.sleep(max(0.25, sleep_s))

            except NonJsonFromGrinex:
                # это ровно твоя проблема на Render: HTML вместо JSON
                logger.error(
                    "BLOCKED: Grinex is returning HTML instead of JSON from this host/IP. "
                    "Without Playwright, fix is: run on another host/IP or set GRINEX_PROXY/HTTPS_PROXY."
                )
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

            except Exception as e:
                logger.error("Worker error: %s", e)
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
