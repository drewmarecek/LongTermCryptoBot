#!/usr/bin/env python3
"""
Alpaca **paper** crypto live runner for **V8**: 1h momentum breakout, market entry, ATR trailing stop (no fixed TP).

Entry when ``close > EMA_200``, ``close > upper BB``, and volume > 1.5× 20-SMA. Initial stop: entry − 2×ATR.
Each closed hour, stop is raised to ``max(prior, close − 2×ATR)`` via Alpaca stop order replace.

Requires ``.env``::
    ALPACA_API_KEY=...
    ALPACA_SECRET_KEY=...
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
import schedule
from dotenv import load_dotenv

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.models import Order
from alpaca.trading.requests import (
    GetOrderByIdRequest,
    GetOrdersRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLossRequest,
)

from crypto_bot.risk_manager import RiskManager
from crypto_bot.strategy import COL_ATR_14, StrategyEngine

logger = logging.getLogger(__name__)

SYMBOL = "BTC/USD"
TIMEFRAME = TimeFrame(1, TimeFrameUnit.Hour)
BAR_HOURS = 1
BAR_FETCH_LIMIT = 500

_trading: Optional[TradingClient] = None
_data: Optional[CryptoHistoricalDataClient] = None
_strategy = StrategyEngine()
_stop_order_id: Optional[str] = None
_trailing_stop_price: Optional[float] = None


def _enum_str(x: Any) -> str:
    """Safely normalize enum/value objects to lowercase strings."""
    return str(getattr(x, "value", x)).lower()


def _require_keys() -> tuple[str, str]:
    """Load and validate required Alpaca credentials from environment."""
    load_dotenv()
    key = os.environ.get("ALPACA_API_KEY", "").strip()
    secret = os.environ.get("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError(
            "Set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env (or environment)."
        )
    return key, secret


def init_clients() -> None:
    """Initialize Alpaca trading and market-data clients (paper mode)."""
    global _trading, _data
    key, secret = _require_keys()
    _trading = TradingClient(key, secret, paper=True)
    _data = CryptoHistoricalDataClient(key, secret)
    logger.info("Alpaca TradingClient (paper) and CryptoHistoricalDataClient initialized.")


def _alpaca_bars_to_df(bar_list: list) -> pd.DataFrame:
    """Convert Alpaca bars payload into a UTC-indexed OHLCV DataFrame."""
    rows = []
    for b in bar_list:
        rows.append(
            {
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(b.volume),
            }
        )
    idx = pd.DatetimeIndex([b.timestamp for b in bar_list], name="timestamp")
    out = pd.DataFrame(rows, index=idx)
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    return out.sort_index()


def _signal_bar_iloc(df: pd.DataFrame) -> int:
    """Return index of latest fully closed 1h bar."""
    if df.empty:
        raise ValueError("empty bar history")
    if len(df) < 2:
        return len(df) - 1
    now = datetime.now(timezone.utc)
    latest_start = df.index[-1]
    if latest_start.tzinfo is None:
        latest_start = latest_start.replace(tzinfo=timezone.utc)
    bar_end = latest_start + pd.Timedelta(hours=BAR_HOURS)
    if now < bar_end.tz_convert(timezone.utc):
        return len(df) - 2
    return len(df) - 1


def _already_long_btc(trading: TradingClient) -> bool:
    """Check whether an open BTC/USD position already exists."""
    norm = SYMBOL.replace("/", "")
    for p in trading.get_all_positions():
        if p.symbol.replace("/", "") == norm:
            return True
    return False


def _pick_stop_sell_leg(parent: Order) -> Optional[Order]:
    """Find protective stop sell leg from a nested bracket order response."""
    for leg in parent.legs or []:
        side = _enum_str(leg.side)
        typ = _enum_str(leg.type)
        if side == "sell" and "stop" in typ:
            return leg
    return None


def _float_stop_price(order: Order) -> Optional[float]:
    """Read stop price as float when present."""
    sp = order.stop_price
    if sp is None:
        return None
    return float(sp)


def _sync_stop_leg_from_open_orders(trading: TradingClient) -> None:
    """If we are long but lost local stop id (restart), reattach to an open protective stop."""
    global _stop_order_id, _trailing_stop_price
    if _stop_order_id is not None:
        return
    try:
        orders = trading.get_orders(
            GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[SYMBOL],
                nested=True,
                limit=50,
            )
        )
    except Exception as exc:
        logger.warning("get_orders for trailing sync: %s", exc)
        return

    for o in orders:
        side = _enum_str(o.side)
        typ = _enum_str(o.type)
        if side == "sell" and "stop" in typ:
            _stop_order_id = str(o.id)
            tp = _float_stop_price(o)
            if tp is not None:
                _trailing_stop_price = tp
            logger.info(
                "Recovered open stop order id=%s stop_price=%s",
                _stop_order_id,
                _trailing_stop_price,
            )
            return


def _clear_trail_state() -> None:
    """Reset in-memory trailing stop/order trackers."""
    global _stop_order_id, _trailing_stop_price
    _stop_order_id = None
    _trailing_stop_price = None


def _maybe_raise_trailing_stop(trading: TradingClient, row: pd.Series) -> None:
    """Ratchet stop upward using ``max(old_stop, close - 2*ATR)`` for active longs."""
    global _stop_order_id, _trailing_stop_price

    if not _already_long_btc(trading):
        _clear_trail_state()
        return

    _sync_stop_leg_from_open_orders(trading)

    close = float(row["close"])
    atr = float(row[COL_ATR_14])
    candidate = RiskManager.trailing_stop_level(close, atr)

    if _trailing_stop_price is None:
        logger.warning("Long position but no trailing anchor; open stop missing?")
        return

    new_stop = max(_trailing_stop_price, candidate)
    if new_stop <= _trailing_stop_price + 1e-8:
        return

    _trailing_stop_price = new_stop
    if _stop_order_id is None:
        logger.warning("Long but no stop order id; cannot ratchet stop yet.")
        return

    r_new = round(new_stop, 2)
    try:
        trading.replace_order_by_id(
            _stop_order_id,
            ReplaceOrderRequest(stop_price=r_new),
        )
        logger.info("Raised trailing stop to %.2f (close=%.2f ATR=%.6f)", r_new, close, atr)
    except Exception:
        logger.exception("replace_order trailing stop failed")


def check_market_and_trade() -> None:
    """Single scheduled iteration: evaluate signal, enter, or trail stop."""
    global _stop_order_id, _trailing_stop_price

    if _trading is None or _data is None:
        logger.error("Clients not initialized; call init_clients() first.")
        return

    trading, data = _trading, _data

    try:
        req = CryptoBarsRequest(
            symbol_or_symbols=SYMBOL,
            timeframe=TIMEFRAME,
            limit=BAR_FETCH_LIMIT,
        )
        bar_map = data.get_crypto_bars(req)
        raw = bar_map.data.get(SYMBOL)
        if not raw or len(raw) < StrategyEngine.EMA_LENGTH:
            logger.warning(
                "Insufficient %s 1h history (have %s, need ≥ %s).",
                SYMBOL,
                len(raw or []),
                StrategyEngine.EMA_LENGTH,
            )
            return

        df = _alpaca_bars_to_df(list(raw))
        sig_i = _signal_bar_iloc(df)
        enriched = _strategy.enrich_indicators(df)
        row = enriched.iloc[sig_i]
        ts = enriched.index[sig_i]

        logger.info(
            "Evaluating signal bar %s (iloc=%s of %s)",
            ts.isoformat(),
            sig_i,
            len(enriched),
        )

        if _already_long_btc(trading):
            _maybe_raise_trailing_stop(trading, row)
            return

        _clear_trail_state()

        if not _strategy.long_entry_signal(row):
            reason = _strategy.explain_skip_long(row)
            logger.info("No entry: %s", reason or "conditions not met")
            return

        account = trading.get_account()
        equity = float(account.portfolio_value)
        entry_px = float(row["close"])
        atr = float(row[COL_ATR_14])

        risk = RiskManager(initial_balance=equity)
        bracket = risk.build_long_bracket(entry_px, atr)
        if bracket is None:
            logger.warning("RiskManager rejected bracket (ATR / leverage cap).")
            return

        qty = round(bracket.quantity, 8)
        if qty <= 0:
            logger.warning("Computed qty <= 0; skip.")
            return

        sl = round(bracket.stop_loss, 2)

        order_req = MarketOrderRequest(
            symbol=SYMBOL,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.GTC,
            order_class=OrderClass.BRACKET,
            stop_loss=StopLossRequest(stop_price=sl),
        )

        logger.info(
            "Submitting BRACKET market BUY qty=%s ~%.2f SL=%.2f (1%% risk $%.2f equity $%.2f, %.0fx cap, no TP)",
            qty,
            entry_px,
            sl,
            bracket.risk_usd,
            equity,
            risk.LEVERAGE_MULTIPLIER,
        )
        order = trading.submit_order(order_req)
        logger.info("Order submitted: id=%s status=%s", order.id, order.status)

        nested = trading.get_order_by_id(order.id, GetOrderByIdRequest(nested=True))
        leg = _pick_stop_sell_leg(nested)
        if leg is not None:
            _stop_order_id = str(leg.id)
            tp = _float_stop_price(leg)
            _trailing_stop_price = tp if tp is not None else bracket.stop_loss
            logger.info(
                "Attached to protective stop leg id=%s stop=%.2f",
                _stop_order_id,
                _trailing_stop_price,
            )
        else:
            logger.warning(
                "Could not read bracket stop leg from nested order; will recover via open orders next run."
            )
            _trailing_stop_price = bracket.stop_loss

    except Exception:
        logger.exception("check_market_and_trade failed")


def main() -> None:
    """Start hourly scheduler and run the live paper loop."""
    log_level = getattr(
        logging,
        os.environ.get("LOG_LEVEL", "INFO").upper(),
        logging.INFO,
    )
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    init_clients()

    schedule.every().hour.at(":00", tz="UTC").do(check_market_and_trade)
    logger.info("Scheduled hourly check at :00 UTC (V8 momentum + ATR trail).")

    logger.info("Live loop running (schedule.run_pending every 60s). Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
