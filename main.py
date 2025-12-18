import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# ───────────────────────── ENV / CONFIG ─────────────────────────

BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")
LIMIT = int(os.getenv("GRINEX_LIMIT", "200"))  # окно сделок с запасом
POLL_SEC = float(os.getenv("GRINEX_POLL_SEC", "2.0"))

SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")  # чтобы trade_time был как у Rapira (HH:MM:SS)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

TRADES_URL = f"{BASE_URL}/api/v2/trades?market={MARKET}&limit={LIMIT}&order_by=desc"

HEADERS = {
    "User-Agent": os.getenv(
        "UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
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


# ───────────────────────── NUMERIC HELPERS ─────────────────────────

Q8 = Decimal("0.00000001")


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def as_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


# ───────────────────────── TIME HELPERS ─────────────────────────

def tzinfo() -> timezone:
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE_NAME)  # type: ignore
    except Exception:
        return timezone.utc


TZ = tzinfo()


def parse_ts(obj: Dict[str, Any]) -> datetime:
    """
    Grinex может отдавать created_at ISO или timestamp (sec/ms).
    """
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


# ───────────────────────── MODEL / DEDUPE ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    tid: Optional[str]
    ts_epoch: int
    price: str
    amount: str


def extract_tid(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "tid", "trade_id"):
        v = obj.get(k)
        if v is not None:
            return str(v)
    return None


def make_key(tid: Optional[str], ts_utc: datetime, price_s: str, amount_s: str) -> TradeKey:
    return TradeKey(tid=tid, ts_epoch=int(ts_utc.timestamp()), price=price_s, amount=amount_s)


def extract_trades(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("trades", "data", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def normalize_trade(obj: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], TradeKey]]:
    """
    Возвращает:
      row для Supabase (source,symbol,price,volume_usdt,volume_rub,trade_time)
      + TradeKey для локальной дедупликации
    """
    price = as_decimal(obj.get("price"))
    amount = as_decimal(obj.get("amount")) or as_decimal(obj.get("volume"))
    total = (
        as_decimal(obj.get("total"))
        or as_decimal(obj.get("funds"))
        or as_decimal(obj.get("quote_volume"))
    )

    if price is None or amount is None:
        return None
    if total is None:
        total = price * amount

    if price <= 0 or amount <= 0 or total <= 0:
        return None

    ts_utc = parse_ts(obj)
    trade_time = ts_utc.astimezone(TZ).strftime("%H:%M:%S")  # time-only как у Rapira

    price_s = q8_str(price)
    amount_s = q8_str(amount)
    total_s = q8_str(total)

    tid = extract_tid(obj)
    key = make_key(tid, ts_utc, price_s, amount_s)

    row = {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": price_s,
        "volume_usdt": amount_s,
        "volume_rub": total_s,      # в твоей схеме volume_rub NOT NULL
        "trade_time": trade_time,   # time-only
    }
    return row, key


def chunked(rows: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [rows]
    return [rows[i:i + n] for i in range(0, len(rows), n)]


# ───────────────────────── HTTP / SUPABASE ─────────────────────────

async def supabase_upsert(sb: httpx.AsyncClient, rows: List[Dict[str, Any]]) -> None:
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

    r = await sb.post(url, headers=headers, params=params, json=rows)
    if r.status_code >= 300:
        logger.error("Supabase upsert failed (%s): %s", r.status_code, r.text[:500])
    else:
        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


async def fetch_window(api: httpx.AsyncClient) -> List[Tuple[Dict[str, Any], TradeKey]]:
    r = await api.get(TRADES_URL)

    # Если иногда вместо JSON прилетает HTML/пусто (Cloudflare/ограничения) —
    # это место даст понятный лог, а не просто "Expecting value..."
    ct = (r.headers.get("content-type") or "").lower()
    if r.status_code >= 400:
        logger.warning("Grinex HTTP %s | ct=%s | body=%s", r.status_code, ct, r.text[:200])
        r.raise_for_status()

    try:
        payload = r.json()
    except Exception:
        logger.warning("Non-JSON response from Grinex | HTTP %s | ct=%s | body=%s", r.status_code, ct, r.text[:300])
        raise

    raw = extract_trades(payload)

    out: List[Tuple[Dict[str, Any], TradeKey]] = []
    for obj in raw:
        norm = normalize_trade(obj)
        if norm:
            out.append(norm)

    return out


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0
    last_success = 0.0

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=15,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,  # если на Render задашь прокси через env, httpx её подхватит
    ) as api, httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,
    ) as sb:

        while True:
            t0 = time.time()
            try:
                window = await fetch_window(api)

                # API обычно отдаёт desc (новые сверху). Для записи в БД удобнее old->new:
                new_rows: List[Dict[str, Any]] = []
                for row, key in reversed(window):
                    if key in seen:
                        continue
                    new_rows.append(row)
                    seen.add(key)
                    seen_q.append(key)

                # если deque вытеснил элементы — иногда проще пересобрать set
                if len(seen) > len(seen_q) + 200:
                    seen = set(seen_q)

                if new_rows:
                    last_success = time.time()
                    logger.info("Parsed %d new trades. Newest=%s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                    for batch in chunked(new_rows, UPSERT_BATCH):
                        await supabase_upsert(sb, batch)
                else:
                    logger.info("No new trades.")

                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    ok_ago = int(now - last_success) if last_success else -1
                    logger.info(
                        "Heartbeat: alive | market=%s | poll=%.2fs | window=%d | seen=%d | last_ok=%ss",
                        MARKET, POLL_SEC, len(window), len(seen), ok_ago
                    )
                    last_heartbeat = now

                backoff = 2.0

            except Exception as e:
                logger.error("Worker error: %s", e)
                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

            # выдерживаем частоту опроса
            dt = time.time() - t0
            sleep_s = max(0.3, POLL_SEC - dt)
            await asyncio.sleep(sleep_s)


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
