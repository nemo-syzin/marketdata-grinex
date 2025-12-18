# main.py
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
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# ───────────────────────── ENV (как на твоём скрине) ─────────────────────────
# Обязательные в Render:
#   SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE
#   LIMIT, POLL_SEC
# Необязательные:
#   ABCEX_EMAIL, ABCEX_PASSWORD (не используются этим воркером)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

LIMIT = int(os.getenv("LIMIT", "800"))          # окно сделок за один запрос
POLL_SEC = float(os.getenv("POLL_SEC", "1.0"))  # период опроса

# ───────────────────────── GRINEX CONFIG ─────────────────────────
BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")  # рынок Grinex
SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

# time without tz в БД — приводим к Москве (как у Rapira)
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

# Должно совпадать с твоим unique index:
# create unique index ... (source, symbol, trade_time, price, volume_usdt)
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

# локальная память ключей (чтобы не слать одно и то же в Supabase по кругу)
SEEN_MAX = int(os.getenv("SEEN_MAX", "30000"))

# батч вставки
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))

# heartbeat чтобы воркер “не молчал”
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
    """
    Пытаемся вытащить время сделки из разных возможных полей.
    Возвращаем datetime в UTC.
    """
    for k in ("created_at", "createdAt", "timestamp", "ts", "time", "at", "date"):
        v = obj.get(k)
        if v is None:
            continue

        # число: sec или ms
        if isinstance(v, (int, float)):
            sec = float(v) / 1000.0 if v > 10_000_000_000 else float(v)
            return datetime.fromtimestamp(sec, tz=timezone.utc)

        # строка ISO
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
    Приводим trade к формату таблицы exchange_trades:
      source, symbol, price, volume_usdt, volume_rub, trade_time
    """
    # price
    price = _as_decimal(obj.get("price"))

    # base amount (USDT)
    amount = (
        _as_decimal(obj.get("amount"))
        or _as_decimal(obj.get("volume"))
        or _as_decimal(obj.get("qty"))
        or _as_decimal(obj.get("quantity"))
    )

    # quote total (RUB-эквивалент)
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
    local = ts_utc.astimezone(TZ)
    trade_time = local.strftime("%H:%M:%S")  # time without tz в БД

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),
        "trade_time": trade_time,
        "_tid": _get_tid(obj),
        "_ts": ts_utc.timestamp(),  # для сортировки (внутреннее)
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


async def preload_seen_from_supabase(client: httpx.AsyncClient, preload_limit: int = 2000) -> List[TradeKey]:
    """
    Чтобы после рестарта не спамить повторными upsert'ами.
    Таблица у тебя содержит created_at, поэтому сортируем по нему.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
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


# ───────────────────────── FETCH ─────────────────────────
async def fetch_window(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    resp = await client.get(TRADES_URL)
    resp.raise_for_status()

    payload = resp.json()
    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    # сортируем по _ts, чтобы вставлять строго от старых к новым
    out.sort(key=lambda r: float(r.get("_ts", 0.0)))
    return out


# ───────────────────────── WORKER ─────────────────────────
async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0
    started = time.time()

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=httpx.Timeout(15.0, connect=10.0),
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,
    ) as client:

        # preload
        try:
            pre = await preload_seen_from_supabase(client, preload_limit=2000)
            for k in pre:
                seen.add(k)
                seen_q.append(k)
            logger.info("Preloaded %d recent keys from Supabase | seen=%d", len(pre), len(seen))
        except Exception as e:
            logger.warning("Preload skipped: %s", e)

        while True:
            t0 = time.time()
            try:
                window = await fetch_window(client)

                if not window:
                    logger.warning("Empty/invalid window from API. url=%s", TRADES_URL)
                else:
                    new_rows: List[Dict[str, Any]] = []
                    for row in window:  # уже отсортировано старые -> новые
                        k = make_key(row)
                        if k in seen:
                            continue
                        new_rows.append(row)
                        seen.add(k)
                        seen_q.append(k)

                    # защита от рассинхронизации set/deque (редко)
                    if len(seen) > len(seen_q) + 500:
                        seen = set(seen_q)

                    if new_rows:
                        newest = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                        logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest, ensure_ascii=False))

                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(client, batch)
                    else:
                        logger.info("No new trades.")

                # heartbeat
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    logger.info(
                        "Heartbeat: alive | market=%s | poll=%.2fs | limit=%d | window=%d | seen=%d | uptime=%.0fs",
                        MARKET, POLL_SEC, LIMIT, len(window), len(seen), now - started
                    )
                    last_heartbeat = now

                backoff = 2.0
                dt = time.time() - t0
                sleep_s = max(0.25, POLL_SEC - dt)
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
