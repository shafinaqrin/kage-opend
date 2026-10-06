"""Kage market data sidecar.

A thin HTTP facade over Moomoo OpenD. Laravel cannot speak OpenD's socket
protocol, so this service owns that connection and exposes four read-only
endpoints:

    GET /health              OpenD reachability, login state, MY entitlement
    GET /watchlist           the user's Bursa watchlist (symbols, names)
    GET /snapshot?codes=...  prices for explicit Bursa codes
    GET /positions           the account's open Bursa holdings
    GET /realized            closed Bursa positions and their booked profit
    GET /deals               executed Bursa fills (~90 days; for closed trades)

Moomoo OpenD is the only provider. No endpoint fabricates data: where OpenD has
no data or lacks MY quote permission, `/snapshot` reports each instrument as
`available: false` with OpenD's own reason, so the UI can list the watchlist
without inventing prices. `/positions` reads the trade context, which needs no
quote entitlement, so holdings carry real figures even when prices do not.
Read-only throughout: no order path exists.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from .opend_client import QuoteResult, get_client, get_trade_client

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("kage.sidecar")

SOURCE = "moomoo-opend"

#: Bursa Malaysia exchange and trading currency.
MY_EXCHANGE = "MYX"
MY_CURRENCY = "MYR"


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("kage-opend sidecar starting")
    yield
    get_client().close()
    get_trade_client().close()
    logger.info("kage-opend sidecar stopped")


app = FastAPI(title="Kage OpenD sidecar", version="1.0.0", lifespan=lifespan)


def _parse_codes(raw: str) -> list[str]:
    codes = [part.strip() for part in raw.split(",") if part.strip()]
    if not codes:
        raise HTTPException(status_code=400, detail="'codes' must contain at least one Bursa code.")
    return codes


def _quote_payload(result: QuoteResult) -> dict[str, Any]:
    """Map an OpenD quote onto the API contract, leaving gaps as null."""
    if not result.available:
        return {
            "symbol": result.code,
            "name": result.name,
            "market": MY_EXCHANGE,
            "currency": MY_CURRENCY,
            "available": False,
            "last": None,
            "previousClose": None,
            "change": None,
            "changePercent": None,
            "open": None,
            "dayHigh": None,
            "dayLow": None,
            "volume": None,
            "turnover": None,
            "updatedAt": None,
            "reason": result.reason or "No data returned by Moomoo OpenD.",
        }

    previous = result.previous_close
    change = None if previous is None or result.last is None else round(result.last - previous, 3)
    change_percent = (
        None
        if previous is None or previous <= 0 or change is None
        else round((change / previous) * 100, 2)
    )

    return {
        "symbol": result.code,
        "name": result.name,
        "market": MY_EXCHANGE,
        "currency": MY_CURRENCY,
        "available": True,
        "last": result.last,
        "previousClose": previous,
        "change": change,
        "changePercent": change_percent,
        "open": result.open,
        "dayHigh": result.day_high,
        "dayLow": result.day_low,
        "volume": result.volume,
        "turnover": result.turnover,
        "updatedAt": result.updated_at,
        "reason": None,
    }


#: Watchlist membership is static between edits, and reading it from OpenD is
#: comparatively slow, so cache it rather than paying that cost on every poll.
_WATCHLIST_TTL_S = float(os.getenv("OPEND_WATCHLIST_TTL", "300"))
_WATCHLIST_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


@app.get("/health")
def health() -> dict[str, Any]:
    """OpenD reachability, MY quote rights, and the live Bursa market session."""
    status = get_client().health()
    return {
        "source": SOURCE,
        **status,
        "marketSession": get_client().market_session(),
    }


@app.get("/watchlist")
def watchlist(market: str = Query(default="MY")) -> dict[str, Any]:
    """The user's OpenD watchlist, filtered to one market.

    OpenD owns membership. Names come from OpenD basic info, which needs no quote
    entitlement, so the list is complete even when prices are blocked.
    """
    key = market.upper()
    now = time.monotonic()

    cached = _WATCHLIST_CACHE.get(key)
    if cached and now - cached[0] < _WATCHLIST_TTL_S:
        return {"source": SOURCE, "market": key, "symbols": cached[1]}

    try:
        entries = get_client().watchlist(market=key)
    except Exception as exc:
        logger.warning("watchlist unavailable: %s", exc)
        if cached is not None:
            # Serve the last good list rather than blanking the dashboard.
            logger.info("serving cached watchlist after failure")
            return {"source": SOURCE, "market": key, "symbols": cached[1]}
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    _WATCHLIST_CACHE[key] = (now, entries)
    return {"source": SOURCE, "market": key, "symbols": entries}


@app.get("/snapshot")
def snapshot(codes: str = Query(description="Comma-separated Bursa codes, e.g. 0345,0373")) -> dict[str, Any]:
    """Quotes for explicit codes. Unavailable instruments come back flagged.

    A missing MY quote entitlement is reported per instrument rather than as a
    transport failure, so the watchlist still renders.
    """
    requested = _parse_codes(codes)
    results = get_client().quotes(requested)

    return {
        "source": SOURCE,
        "myQuotePermission": get_client().health().get("my_quote_permission", False),
        "quotes": [_quote_payload(result) for result in results],
    }


#: Positions move intraday, so they are cached for far less time than the
#: watchlist. Short enough to feel live, long enough to spare OpenD a query per
#: browser poll.
_POSITIONS_TTL_S = float(os.getenv("OPEND_POSITIONS_TTL", "60"))
_POSITIONS_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}

#: Realized P/L for a closed position is fixed once the sell settles, so it
#: changes far less often than open holdings. Shares the positions TTL: still
#: refreshed often enough to pick up a trade made minutes ago.
_REALIZED_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


@app.get("/realized")
def realized(market: str = Query(default="MY")) -> dict[str, Any]:
    """Closed positions with a booked profit, read from the OpenD account.

    OpenD keeps reporting a sold-out position as a row with `qty == 0` and its
    `realized_pl`, so closed trades are available directly and need no
    reconstruction. This is preferable to rebuilding gains from `/deals`: it is
    OpenD's own netted figure, and it is not subject to the ~90-day cap that
    truncates deal history.

    Not exhaustive: a holding sold from an IPO allotment is absent from the
    position list entirely, so it has no `realized_pl` here. Such sells are
    reported by `/deals` as unmatched instead.
    """
    key = market.upper()
    now = time.monotonic()

    cached = _REALIZED_CACHE.get(key)
    if cached and now - cached[0] < _POSITIONS_TTL_S:
        return {"source": SOURCE, "market": key, "realized": cached[1]}

    try:
        entries = get_trade_client().realized(market=key)
    except Exception as exc:
        logger.warning("realized P/L unavailable: %s", exc)
        if cached is not None:
            logger.info("serving cached realized P/L after failure")
            return {"source": SOURCE, "market": key, "realized": cached[1]}
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    _REALIZED_CACHE[key] = (now, entries)
    return {"source": SOURCE, "market": key, "realized": entries}

@app.get("/positions")
def positions(market: str = Query(default="MY")) -> dict[str, Any]:
    """Open positions for one market, read straight from the OpenD account.

    This reads the trade context, not the quote context: holdings come back even
    when MY quote rights are missing. Anything OpenD omits stays null rather than
    being rendered as a zero.
    """
    key = market.upper()
    now = time.monotonic()

    cached = _POSITIONS_CACHE.get(key)
    if cached and now - cached[0] < _POSITIONS_TTL_S:
        return {"source": SOURCE, "market": key, "positions": cached[1]}

    try:
        entries = get_trade_client().positions(market=key)
    except Exception as exc:
        logger.warning("positions unavailable: %s", exc)
        if cached is not None:
            logger.info("serving cached positions after failure")
            return {"source": SOURCE, "market": key, "positions": cached[1]}
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    _POSITIONS_CACHE[key] = (now, entries)
    return {"source": SOURCE, "market": key, "positions": entries}

#: Deal history is append-only and changes only when the user trades, so it is
#: cached far longer than positions. Cached separately from the window because
#: OpenD truncates wide windows to its own ~90-day cap anyway.
_DEALS_TTL_S = float(os.getenv("OPEND_DEALS_TTL", "300"))
_DEALS_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}

@app.get("/deals")
def deals(
    market: str = Query(default="MY"),
    start: str = Query(default="", description="YYYY-MM-DD; OpenD truncates wide windows"),
    end: str = Query(default="", description="YYYY-MM-DD"),
) -> dict[str, Any]:
    """Executed deals (fills) for one market, oldest first.

    Needed because `/positions` reports open holdings only: selling a position
    removes it from that endpoint entirely, so closed trades -- and the realized
    P/L they produced -- are invisible without reading the deal stream.

    **OpenD caps this history at roughly 90 days.** It truncates the requested
    window itself and reports nothing about having done so, which is why the
    response echoes back the window actually covered: a caller must not present
    the total as all-time. `oldest`/`newest` are null when no deals came back.
    """
    key = market.upper()
    now = time.monotonic()

    cached = _DEALS_CACHE.get(key)
    if cached and now - cached[0] < _DEALS_TTL_S:
        entries = cached[1]
    else:
        try:
            entries = get_trade_client().deals(market=key, start=start, end=end)
        except Exception as exc:
            logger.warning("deal history unavailable: %s", exc)
            if cached is not None:
                logger.info("serving cached deals after failure")
                entries = cached[1]
            else:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
        else:
            _DEALS_CACHE[key] = (now, entries)

    return {
        "source": SOURCE,
        "market": key,
        "deals": entries,
        # The window actually covered, so the UI can label the figure honestly
        # instead of implying it spans the account's whole history.
        "oldest": entries[0]["time"] if entries else None,
        "newest": entries[-1]["time"] if entries else None,
    }
