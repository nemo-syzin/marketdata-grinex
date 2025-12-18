import asyncio
import json
import logging
import os
import re
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


# ───────────────────────── CONFIG ─────────────────────────

GRINEX_BASE_URL = os.getenv("GRINEX_BASE_URL", "https://grinex.io").rstrip("/")
GRINEX_TRADES_PATH = os.getenv("GRINEX_TRADES_PATH", "/api/v2/trades")
GRINEX_MARKET = os.getenv("GRINEX_MARKET", "usdta7a5")
GRINEX_LIMIT = int(os.getenv("GRINEX_LIMIT", "200"))
GRINEX_ORDER_BY = os.getenv("GRINEX_ORDER_BY", "desc")
GRINEX_POLL_SECONDS = float(os.getenv("GRINEX_POLL_SECONDS", "2.0"))

SOURCE = os.getenv("SOURCE", "grinex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))

HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))
SHOW_EMPTY_EVERY_SEC = int(os.getenv("SHOW_EMPTY_EVERY_SEC", "60"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
TRUST_ENV = os.getenv("TRUST_ENV", "1") == "1"  # чтобы HTTP(S)_PROXY работал, если ты задашь его в Render

UA = os.getenv(
    "UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{GRINEX_BASE_URL}/trading/{GRINEX_MARKET}?lang=ru",
}

Q8 = Decimal("0.00000001")

TRADES_URL = f"{GRINEX_BASE_URL}{GRINEX_TRADES_PATH}"

# meta refresh (noscript) — типично: content="0; url=/abcdef"
META_REFRESH_RE = re.compile(r'http-equiv=["\']refresh["\'][^>]*content=["\'][^;]+;\s*url=([^"\']+)["\']', re.I)
# иногда встречается просто url=/path в html
URL_FALLBACK_RE = re.compile(r'url=([^"\'>\s]+)', re.I)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("grinex-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ───────────────────────── TYPES ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    tid: Optional[str]
    trade_time: str
    price: str
    volume_usdt: str


def _tzinfo() -> timezone:
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(TIMEZONE_NAME)  # type: ignore
    except Exception:
        return timezone.utc


TZ = _tzinfo()


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
    """
    Приводим к схеме exchange_trades:
      source, symbol, price, volume_usdt, volume_rub, trade_time
    """
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
    trade_time = local.strftime("%H:%M:%S")  # как у Rapira

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(amount),
        "volume_rub": q8_str(total),
        "trade_time": trade_time,
        "_tid": _get_tid(obj),  # внутреннее поле для seen
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


# ───────────────────────── GRINEX FETCH (NO PLAYWRIGHT) ─────────────────────────

async def _noscript_handshake_if_needed(client: httpx.AsyncClient, html: str) -> bool:
    """
    Пытаемся пройти “noscript meta refresh” (получить cookie) и вернуть True, если сделали попытку.
    Это не гарантирует успех, но часто превращает HTML → JSON на следующем запросе.
    """
    m = META_REFRESH_RE.search(html)
    if not m:
        m2 = URL_FALLBACK_RE.search(html)
        if not m2:
            return False
        path = m2.group(1)
    else:
        path = m.group(1)

    if not path.startswith("/"):
        # иногда бывает относительный/странный — нормализуем
        if path.startswith("http"):
            url = path
        else:
            url = f"{GRINEX_BASE_URL}/{path.lstrip('/')}"
    else:
        url = f"{GRINEX_BASE_URL}{path}"

    logger.warning("Grinex returned HTML, trying noscript handshake: GET %s", url)
    try:
        r = await client.get(url)
        logger.info("Handshake response: HTTP %s | ct=%s", r.status_code, r.headers.get("content-type", ""))
    except Exception as e:
        logger.warning("Handshake failed: %s", e)
    return True


async def fetch_window(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    params = {"market": GRINEX_MARKET, "limit": str(GRINEX_LIMIT), "order_by": GRINEX_ORDER_BY}

    r = await client.get(TRADES_URL, params=params)
    ct = (r.headers.get("content-type") or "").lower()
    body = r.text or ""

    # Если вдруг снова HTML — логируем и пробуем handshake + повтор
    if "application/json" not in ct:
        snippet = body[:400].replace("\n", "\\n")
        logger.warning("Non-JSON response from Grinex | HTTP %s | ct=%s | body=%s", r.status_code, ct, snippet)

        did = await _noscript_handshake_if_needed(client, body)
        if did:
            # повторяем запрос 1 раз
            r2 = await client.get(TRADES_URL, params=params)
            ct2 = (r2.headers.get("content-type") or "").lower()
            if "application/json" not in ct2:
                snippet2 = (r2.text or "")[:400].replace("\n", "\\n")
                raise RuntimeError(f"Still non-JSON from Grinex after handshake | ct={ct2} | body={snippet2}")
            payload = r2.json()
        else:
            raise RuntimeError(f"Non-JSON from Grinex | ct={ct} | body={snippet}")
    else:
        payload = r.json()

    raw = _extract_trades(payload)

    out: List[Dict[str, Any]] = []
    for obj in raw:
        row = normalize_trade(obj)
        if row:
            out.append(row)

    return out


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

    resp = await client.post(url, headers=headers, params=params, json=payload)
    if resp.status_code >= 300:
        logger.error("Supabase upsert failed (%s): %s", resp.status_code, resp.text)
    else:
        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(payload), SUPABASE_TABLE)


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0
    last_empty_log = 0.0

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=TRUST_ENV,
        http2=False,  # иногда снижает “странные” ответы на некоторых хостингах
    ) as client:
        while True:
            t0 = time.time()
            try:
                window = await fetch_window(client)

                if window:
                    # API обычно “новые сверху” → пишем старые→новые
                    new_rows: List[Dict[str, Any]] = []
                    for row in reversed(window):
                        k = make_key(row)
                        if k in seen:
                            continue
                        new_rows.append(row)
                        seen.add(k)
                        seen_q.append(k)

                    # защита от рассинхрона
                    if len(seen) > len(seen_q) + 200:
                        seen = set(seen_q)

                    if new_rows:
                        newest = {k: v for k, v in new_rows[-1].items() if not k.startswith("_")}
                        logger.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(newest, ensure_ascii=False))
                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(client, batch)
                    else:
                        now = time.time()
                        if now - last_empty_log >= SHOW_EMPTY_EVERY_SEC:
                            logger.info("No new trades (market=%s).", GRINEX_MARKET)
                            last_empty_log = now
                else:
                    logger.warning("Empty window from Grinex API (market=%s).", GRINEX_MARKET)

                # heartbeat
                now = time.time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    logger.info(
                        "Heartbeat: alive | market=%s | poll=%.2fs | window=%d | seen=%d",
                        GRINEX_MARKET, GRINEX_POLL_SECONDS, len(window), len(seen)
                    )
                    last_heartbeat = now

                backoff = 2.0
                dt = time.time() - t0
                sleep_s = max(0.3, GRINEX_POLL_SECONDS - dt)
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
