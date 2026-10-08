"""Moomoo OpenD client.

Speaks the OpenD socket protocol through the official `moomoo-api` SDK. This is
the only place in Kage that talks to OpenD: the Laravel backend reaches it over
plain HTTP, because OpenD does not expose an HTTP interface.

Read-only. No trade context is ever opened, and live trading is never enabled.

There is no fallback data anywhere in this module. When OpenD cannot answer --
unreachable, not logged in, or missing a quote entitlement -- the caller gets an
explicit failure or `available: false`, never a synthesised price.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from moomoo import (
    RET_OK,
    OpenQuoteContext,
    OpenSecTradeContext,
    SecurityFirm,
    TrdEnv,
    TrdMarket,
)

logger = logging.getLogger("kage.opend")

#: Bursa Malaysia instruments are addressed as `MY.<code>` in Moomoo.
MY_PREFIX = "MY."

#: OpenD error fragment used to detect a missing MY quote entitlement. Verified
#: against OpenD 10.11, where `get_market_snapshot` returns e.g.
#: "No permission to get quotes for MY.<code>. Please check MY Security
#: MarketStocks quote permissions."
_PERMISSION_MARKERS = ("no permission", "permission", "not authorized", "unauthorized")


class OpenDUnavailable(RuntimeError):
    """OpenD could not be reached or is not logged in."""


@dataclass(frozen=True)
class QuoteResult:
    """Outcome of a snapshot request for a single instrument."""

    code: str
    available: bool
    name: str | None = None
    last: float | None = None
    previous_close: float | None = None
    open: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    volume: int | None = None
    turnover: int | None = None
    updated_at: int | None = None
    reason: str | None = None


def _to_float(value: Any) -> float | None:
    """Coerce an OpenD cell to float, treating N/A and blanks as absent."""
    if value is None:
        return None
    try:
        # pandas uses NaN for missing numerics; NaN != NaN.
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result:  # NaN
        return None
    return result


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return None if number is None else int(number)


def to_moomoo_code(code: str) -> str:
    """Normalise a Bursa code to Moomoo's `MY.<code>` form."""
    clean = str(code).strip().upper()
    return clean if clean.startswith(MY_PREFIX) else MY_PREFIX + clean


def to_bursa_code(code: str) -> str:
    """Reduce a Moomoo `MY.<code>` symbol back to its bare Bursa code."""
    clean = str(code).strip().upper()
    return clean[len(MY_PREFIX):] if clean.startswith(MY_PREFIX) else clean


def is_permission_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _PERMISSION_MARKERS)


class OpenDClient:
    """Thread-safe, reconnecting wrapper around a single quote context.

    OpenD caps how many concurrent connections it accepts and warns when clients
    leak them, so one context is created lazily, reused for every request, and
    guarded by a lock. A failed call drops the context so the next request
    reconnects instead of inheriting a dead socket.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._ctx: OpenQuoteContext | None = None
        # Re-entrant: public helpers acquire the lock and then call other locked
        # helpers (e.g. watchlist -> basic_info_for), which a plain Lock deadlocks.
        self._lock = threading.RLock()

    # -- connection management ------------------------------------------------

    def _context(self) -> OpenQuoteContext:
        if self._ctx is None:
            logger.info("connecting to OpenD at %s:%s", self._host, self._port)
            self._ctx = OpenQuoteContext(host=self._host, port=self._port)
        return self._ctx

    def _drop_context(self) -> None:
        ctx, self._ctx = self._ctx, None
        if ctx is not None:
            try:
                ctx.close()
            except Exception:  # pragma: no cover - best-effort teardown
                logger.debug("ignoring error while closing OpenD context", exc_info=True)

    def close(self) -> None:
        with self._lock:
            self._drop_context()

    def _call(self, method: str, *args: Any, **kwargs: Any) -> tuple[int, Any]:
        """Invoke an SDK method, reconnecting once if the socket has died."""
        for attempt in (1, 2):
            try:
                ctx = self._context()
                return getattr(ctx, method)(*args, **kwargs)
            except Exception as exc:
                logger.warning("OpenD %s failed (attempt %s): %s", method, attempt, exc)
                self._drop_context()
                if attempt == 2:
                    raise OpenDUnavailable(
                        f"Moomoo OpenD is unreachable at {self._host}:{self._port}: {exc}"
                    ) from exc
                time.sleep(0.4)
        raise AssertionError("unreachable")  # pragma: no cover

    # -- health ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Report OpenD reachability, login state, and MY quote entitlement."""
        with self._lock:
            try:
                ret, state = self._call("get_global_state")
            except OpenDUnavailable as exc:
                return {
                    "reachable": False,
                    "logged_in": False,
                    "status": "unreachable",
                    "market_my": None,
                    "my_quote_permission": False,
                    "detail": str(exc),
                }

            if ret != RET_OK:
                return {
                    "reachable": True,
                    "logged_in": False,
                    "status": "unconfigured",
                    "market_my": None,
                    "my_quote_permission": False,
                    "detail": f"OpenD rejected get_global_state: {state}",
                }

            logged_in = bool(state.get("qot_logined")) if hasattr(state, "get") else False
            market_my = state.get("market_my") if hasattr(state, "get") else None

            permission, detail = self._probe_my_permission()
            if not logged_in:
                status = "unconfigured"
            elif permission:
                status = "connected"
            else:
                status = "no_permission"

            return {
                "reachable": True,
                "logged_in": logged_in,
                "status": status,
                "market_my": market_my,
                "my_quote_permission": permission,
                "detail": detail,
                "server_version": state.get("server_ver") if hasattr(state, "get") else None,
            }

    def market_session(self) -> str | None:
        """The live Bursa Malaysia session as OpenD reports it.

        Returned verbatim from OpenD (e.g. CLOSED, OPEN, PRE_OPEN) rather than
        inferred, so the UI never claims the market is open when it is not.
        """
        try:
            ret, state = self._call("get_global_state")
        except OpenDUnavailable:
            return None
        if ret != RET_OK or not hasattr(state, "get"):
            return None
        return state.get("market_my")

    def _probe_my_permission(self) -> tuple[bool, str]:
        """Probe one Bursa snapshot to learn whether MY quotes are entitled.

        The instrument probed comes from the user's own watchlist, not a literal,
        so this reports on something the user actually tracks.
        """
        try:
            ret, data = self._call("get_user_security", "All")
        except OpenDUnavailable as exc:
            return False, str(exc)

        if ret != RET_OK:
            return False, str(data)

        codes = self._extract_codes(data, "MY")
        if not codes:
            return False, "No Bursa instruments in the OpenD watchlist to probe."

        try:
            ret, data = self._call("get_market_snapshot", [codes[0]])
        except OpenDUnavailable as exc:
            return False, str(exc)

        if ret == RET_OK:
            return True, "MY quote entitlement present."
        return False, str(data)

    # -- watchlist ------------------------------------------------------------

    def watchlist(self, market: str = "MY", group: str = "All") -> list[dict[str, Any]]:
        """Read one user's OpenD watchlist group and keep only `market` instruments.

        Names and lot sizes come from `get_stock_basicinfo`, which does not need a
        quote entitlement. That is what lets the watchlist render even while MY
        price permissions are missing.
        """
        with self._lock:
            ret, data = self._call("get_user_security", group)
            if ret != RET_OK:
                raise OpenDUnavailable(f"OpenD could not read watchlist group '{group}': {data}")

            codes = self._extract_codes(data, market)
            if not codes:
                return []

            names = self._basic_info(codes)
            out: list[dict[str, Any]] = []
            for code in codes:
                info = names.get(code, {})
                out.append(
                    {
                        "symbol": to_bursa_code(code),
                        "moomooCode": code,
                        "name": info.get("name") or to_bursa_code(code),
                        "market": market,
                        "category": group,
                    }
                )
            return out

    def watchlist_groups(self) -> list[str]:
        """Return the user's Moomoo watchlist groups in display order."""
        with self._lock:
            ret, data = self._call("get_user_security_group")
            if ret != RET_OK or data is None:
                raise OpenDUnavailable(f"OpenD could not read watchlist groups: {data}")

            groups: list[str] = []
            if hasattr(data, "iterrows"):
                for _, row in data.iterrows():
                    name = str(row.get("group_name") or row.get("groupName") or row.get("name") or "").strip()
                    if name and name not in groups:
                        groups.append(name)
            elif isinstance(data, (list, tuple)):
                for entry in data:
                    name = str(entry.get("group_name") or entry.get("groupName") or entry.get("name") or "").strip() if isinstance(entry, dict) else str(entry).strip()
                    if name and name not in groups:
                        groups.append(name)

            return groups or ["All"]

    @staticmethod
    def _extract_codes(data: Any, market: str) -> list[str]:
        if data is None or not hasattr(data, "empty") or data.empty:
            return []
        prefix = f"{market.upper()}."
        codes: list[str] = []
        seen: set[str] = set()
        for raw in data["code"].tolist():
            code = str(raw).strip().upper()
            if not code.startswith(prefix) or code in seen:
                continue
            seen.add(code)
            codes.append(code)
        return codes

    def basic_info_for(self, codes: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Public wrapper over basic info, which needs no quote entitlement.

        Names remain available while MY prices are blocked, so the UI can label
        instruments whose prices come from another provider.
        """
        with self._lock:
            return self._basic_info([str(code).strip().upper() for code in codes])

    def _basic_info(self, codes: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Fetch name/lot size per code, skipping any OpenD refuses."""
        out: dict[str, dict[str, Any]] = {}
        batch_size = 200  # SDK limit for get_stock_basicinfo
        for start in range(0, len(codes), batch_size):
            batch = list(codes[start:start + batch_size])
            try:
                ret, data = self._call("get_stock_basicinfo", "MY", code_list=batch)
            except OpenDUnavailable:
                logger.warning("basic info unavailable; serving codes without names")
                continue
            if ret != RET_OK or data is None or not hasattr(data, "empty") or data.empty:
                logger.warning("basic info returned nothing for %s codes: %s", len(batch), data)
                continue
            for _, row in data.iterrows():
                code = str(row.get("code", "")).strip().upper()
                if not code:
                    continue
                out[code] = {"name": row.get("name") or None}
        return out

    # -- quotes ---------------------------------------------------------------

    def quotes(self, codes: Iterable[str]) -> list[QuoteResult]:
        """Snapshot the given codes, marking each one available or not.

        A missing MY entitlement is a per-instrument `available: false`, not a
        transport failure: the watchlist must still render.
        """
        wanted = [to_moomoo_code(code) for code in codes if str(code).strip()]
        if not wanted:
            return []

        with self._lock:
            try:
                ret, data = self._call("get_market_snapshot", wanted)
            except OpenDUnavailable as exc:
                # Even with OpenD unreachable we still know the instruments; fall
                # back to basic info so the watchlist keeps its names.
                info = self._basic_info(wanted)
                return [
                    QuoteResult(
                        code=to_bursa_code(c),
                        available=False,
                        name=info.get(c, {}).get("name"),
                        reason=str(exc),
                    )
                    for c in wanted
                ]

            if ret != RET_OK:
                # A missing MY entitlement fails the whole batch. Basic info does
                # not need that entitlement, so use it for names and lot sizes.
                reason = str(data)
                logger.info("snapshot unavailable for %s codes: %s", len(wanted), reason)
                info = self._basic_info(wanted)
                return [
                    QuoteResult(
                        code=to_bursa_code(c),
                        available=False,
                        name=info.get(c, {}).get("name"),
                        reason=reason,
                    )
                    for c in wanted
                ]

            rows = self._index_rows(data)
            return [self._present(code, rows.get(code)) for code in wanted]

    @staticmethod
    def _index_rows(data: Any) -> dict[str, Any]:
        if data is None or not hasattr(data, "empty") or data.empty:
            return {}
        rows: dict[str, Any] = {}
        for _, row in data.iterrows():
            key = str(row.get("code", "")).strip().upper()
            if key:
                rows[key] = row
        return rows

    @staticmethod
    def _present(code: str, row: Any) -> QuoteResult:
        symbol = to_bursa_code(code)
        if row is None:
            return QuoteResult(
                code=symbol,
                available=False,
                reason=f"No snapshot returned by Moomoo OpenD for {code}.",
            )

        # Basic info (the instrument name) survives a missing quote entitlement,
        # so it is carried through even when the price fields are blocked.
        last = _to_float(row.get("last_price"))
        if last is None:
            return QuoteResult(
                code=symbol,
                available=False,
                name=row.get("name") or None,
                reason="Moomoo OpenD returned no last price for this instrument.",
            )

        updated = row.get("update_time")
        return QuoteResult(
            code=symbol,
            available=True,
            name=row.get("name") or None,
            last=last,
            previous_close=_to_float(row.get("prev_close_price")),
            open=_to_float(row.get("open_price")),
            day_high=_to_float(row.get("high_price")),
            day_low=_to_float(row.get("low_price")),
            volume=_to_int(row.get("volume")),
            turnover=_to_int(row.get("turnover")),
            updated_at=_parse_update_time(updated),
        )


def _parse_update_time(value: Any) -> int | None:
    """Convert OpenD's `update_time` string into epoch milliseconds."""
    if not value or not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return int(time.mktime(time.strptime(value, fmt)) * 1000)
        except ValueError:
            continue
    return None


class OpenTradeClient:
    """Read-only positions from OpenD's *trade* context.

    Positions are a different API from quotes: `position_list_query` reads the
    account, not the market, so it works without the MY quote entitlement that
    blocks snapshots. That is what lets the Positions panel show real figures
    while the quote columns read "—".

    Read-only by construction: only query methods are called, no order path
    exists, and live trading is never unlocked.
    """

    #: Security firm per market. OpenD rejects the wrong one for the account.
    _FIRMS = {"MY": SecurityFirm.FUTUMY}

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._ctx: OpenSecTradeContext | None = None
        self._lock = threading.RLock()

    def _context(self, market: str) -> OpenSecTradeContext:
        if self._ctx is None:
            logger.info("connecting to OpenD trade context at %s:%s", self._host, self._port)
            self._ctx = OpenSecTradeContext(
                filter_trdmarket=TrdMarket.MY,
                host=self._host,
                port=self._port,
                security_firm=self._FIRMS.get(market, SecurityFirm.FUTUMY),
            )
        return self._ctx

    def _drop_context(self) -> None:
        ctx, self._ctx = self._ctx, None
        if ctx is not None:
            try:
                ctx.close()
            except Exception:  # pragma: no cover - best-effort teardown
                logger.debug("ignoring error while closing OpenD trade context", exc_info=True)

    def close(self) -> None:
        with self._lock:
            self._drop_context()

    def _call(self, method: str, *args: Any, **kwargs: Any) -> tuple[int, Any]:
        for attempt in (1, 2):
            try:
                return getattr(self._context("MY"), method)(*args, **kwargs)
            except Exception as exc:
                logger.warning("OpenD trade %s failed (attempt %s): %s", method, attempt, exc)
                self._drop_context()
                if attempt == 2:
                    raise OpenDUnavailable(
                        f"Moomoo OpenD trade context is unreachable at {self._host}:{self._port}: {exc}"
                    ) from exc
                time.sleep(0.4)
        raise AssertionError("unreachable")  # pragma: no cover

    def working_orders(self, market: str = "MY") -> list[dict[str, Any]]:
        """Read active orders so TP/SL can be matched to held positions."""
        with self._lock:
            ret, data = self._call("order_list_query", trd_env=TrdEnv.REAL)
            if ret != RET_OK:
                raise OpenDUnavailable(f"OpenD could not read working orders: {data}")
            prefix = f"{market.upper()}."
            out = []
            for row in self._index_rows(data):
                code = str(row.get("code", "")).strip().upper()
                if code.startswith(prefix):
                    out.append({"symbol": to_bursa_code(code), "price": _to_float(row.get("price")), "remark": row.get("remark") or "", "orderType": str(row.get("order_type", "")), "auxPrice": _to_float(row.get("aux_price")), "orderId": str(row.get("order_id", "")), "status": str(row.get("order_status", ""))})
            return out

    def positions(self, market: str = "MY") -> list[dict[str, Any]]:
        """Open positions for one market, newest query straight from OpenD.

        Only the REAL environment carries the user's holdings; OpenD rejects
        `TrdEnv.SIMULATE` for this account, so that is the one used.

        Only rows with a non-zero quantity are returned: a sold-out position is
        not a holding. It is *not* discarded though -- OpenD keeps reporting such
        rows with `qty == 0` and a booked `realized_pl`, which `realized()`
        returns. Dropping them here is what made closed trades invisible.
        """
        with self._lock:
            ret, data = self._call("position_list_query", trd_env=TrdEnv.REAL)
            if ret != RET_OK:
                raise OpenDUnavailable(f"OpenD could not read positions: {data}")

            prefix = f"{market.upper()}."
            out: list[dict[str, Any]] = []
            for row in self._index_rows(data):
                code = str(row.get("code", "")).strip().upper()
                if not code.startswith(prefix):
                    continue
                quantity = _to_float(row.get("qty"))
                if not quantity:
                    # A closed position is not a holding.
                    continue
                out.append(
                    {
                        "symbol": to_bursa_code(code),
                        "moomooCode": code,
                        "name": row.get("stock_name") or to_bursa_code(code),
                        "market": market.upper(),
                        "currency": row.get("currency") or "MYR",
                        "quantity": quantity,
                        "sellableQuantity": _to_float(row.get("can_sell_qty")),
                        "averageCost": _to_float(row.get("average_cost")),
                        "last": _to_float(row.get("nominal_price")),
                        "marketValue": _to_float(row.get("market_val")),
                        "profitLoss": _to_float(row.get("pl_val")),
                        "profitLossPercent": _to_float(row.get("pl_ratio")),
                        "todayProfitLoss": _to_float(row.get("today_pl_val")),
                        # Working orders are exposed by OpenD only when the
                        # position row carries the corresponding trigger prices.
                        "takeProfit": _to_float(row.get("take_profit_price")),
                        "stopLoss": _to_float(row.get("stop_loss_price")),
                    }
                )
            return out

    def realized(self, market: str = "MY") -> list[dict[str, Any]]:
        """Closed positions with a booked profit, for one market.

        Uses OpenD's own `realized_pl` rather than reconstructing gains from the
        deal stream: it is authoritative, already netted by OpenD, and -- unlike
        deal history, which OpenD truncates to ~90 days -- not window-limited.

        OpenD reports a sold-out position as a `position_list_query` row with
        `qty == 0` rather than removing it, which is what makes this available at
        all. `positions()` deliberately skips those rows.

        Selected by `realized_pl != 0`, **not** by `qty == 0`. A position sold
        only partly still holds shares (`qty > 0`) yet has already booked a
        realized gain, and that gain is just as real. Filtering on quantity
        alone would silently drop it. Note a genuinely break-even exit books
        `realized_pl == 0` and so appears in neither list -- unavoidable, since
        OpenD reports no separate "closed at zero" flag.

        **Not every closed trade appears here.** A position sold from an IPO
        allotment is absent from the position list entirely (verified: SRKKAI),
        so OpenD books no `realized_pl` for it. Those remain undetectable here
        and are surfaced from the deal stream as unmatched instead.
        """
        with self._lock:
            ret, data = self._call("position_list_query", trd_env=TrdEnv.REAL)
            if ret != RET_OK:
                raise OpenDUnavailable(f"OpenD could not read positions: {data}")

            prefix = f"{market.upper()}."
            out: list[dict[str, Any]] = []
            for row in self._index_rows(data):
                code = str(row.get("code", "")).strip().upper()
                if not code.startswith(prefix):
                    continue
                # Select on a booked gain, not on being fully sold: a partially
                # closed position still holds shares but has realized profit too.
                # This also excludes None (no figure) and 0 (break-even).
                realized = _to_float(row.get("realized_pl"))
                if not realized:
                    continue
                out.append(
                    {
                        "symbol": to_bursa_code(code),
                        "name": row.get("stock_name") or to_bursa_code(code),
                        "market": market.upper(),
                        "currency": row.get("currency") or "MYR",
                        "realized": realized,
                    }
                )
            return out

    @staticmethod
    def _index_rows(data: Any) -> list[Any]:
        """Positions arrive as a DataFrame; an empty result is a valid one."""
        if data is None or not hasattr(data, "empty") or data.empty:
            return []
        return [row for _, row in data.iterrows()]

    def deals(self, market: str = "MY", start: str = "", end: str = "") -> list[dict[str, Any]]:
        """Executed deals (fills) for one market, oldest first.

        This is the only source of *closed* trades: `position_list_query` returns
        open holdings alone, so a position the user sold simply disappears from
        it. Realized P/L therefore has to be reconstructed from the deal stream.

        Two properties of the underlying API shape what callers can claim:

        1. Only these markets are returned. Deals for other markets in the same
           account (e.g. US) are filtered out here, because Kage reports Bursa
           only and mixing currencies would make any total meaningless.
        2. **History is capped at roughly 90 days by OpenD itself**, regardless
           of the `start`/`end` asked for -- the SDK normalises the window and
           silently truncates anything wider. So a realized-P/L figure built on
           this is a *windowed* figure, never all-time. Callers must label it as
           such rather than implying it covers the account's whole past.

        An IPO allotment never appears here as a BUY: a primary-market
        subscription produces no deal, so a sold IPO holding shows up as a SELL
        with no matching cost. Callers must treat that as unmatched rather than
        assuming a zero cost.
        """
        with self._lock:
            kwargs: dict[str, Any] = {"trd_env": TrdEnv.REAL}
            if start:
                kwargs["start"] = start
            if end:
                kwargs["end"] = end

            ret, data = self._call("history_deal_list_query", **kwargs)
            if ret != RET_OK:
                raise OpenDUnavailable(f"OpenD could not read deal history: {data}")

            prefix = f"{market.upper()}."
            out: list[dict[str, Any]] = []
            for row in self._index_rows(data):
                code = str(row.get("code", "")).strip().upper()
                if not code.startswith(prefix):
                    continue
                out.append(
                    {
                        "symbol": to_bursa_code(code),
                        "name": row.get("stock_name") or to_bursa_code(code),
                        "market": market.upper(),
                        "dealId": str(row.get("deal_id", "")),
                        "side": str(row.get("trd_side", "")).strip().upper(),
                        "quantity": _to_float(row.get("qty")),
                        "price": _to_float(row.get("price")),
                        "time": str(row.get("create_time", "")),
                    }
                )

            # OpenD returns newest first; realized P/L is reconstructed by
            # walking trades forward, so order them oldest first here.
            out.sort(key=lambda d: d["time"])
            return out


_client: OpenDClient | None = None


def get_client() -> OpenDClient:
    """Process-wide OpenD client, configured from the environment."""
    global _client
    if _client is None:
        _client = OpenDClient(
            host=os.getenv("OPEND_HOST", "host.docker.internal"),
            port=int(os.getenv("OPEND_PORT", "11111")),
        )
    return _client


_trade_client: OpenTradeClient | None = None


def get_trade_client() -> OpenTradeClient:
    """Process-wide OpenD trade client, configured from the environment."""
    global _trade_client
    if _trade_client is None:
        _trade_client = OpenTradeClient(
            host=os.getenv("OPEND_HOST", "host.docker.internal"),
            port=int(os.getenv("OPEND_PORT", "11111")),
        )
    return _trade_client
