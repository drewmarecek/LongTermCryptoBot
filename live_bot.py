#!/usr/bin/env python3
"""
Alpaca **paper** crypto live runner for V5 strategy (BB snap-back + volume spike).

Schedules checks every 4 hours on the UTC boundary. Backtesting entry points are unchanged.

Requires ``.env``::
    ALPACA_API_KEY=...
    ALPACA_SECRET_KEY=...
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import schedule
from dotenv import load_dotenv

from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from crypto_bot.risk_manager import RiskManager
from crypto_bot.strategy import COL_ATR_14, StrategyEngine

logger = logging.getLogger(__name__)

SYMBOL = "BTC/USD"
TIMEFRAME = TimeFrame(4, TimeFrameUnit.Hour)
BAR_HOURS = 4
BAR_FETCH_LIMIT = 300

_trading: Optional[TradingClient] = None
_data: Optional[CryptoHistoricalDataClient] = None
_strategy = StrategyEngine()


def _require_keys() -> tuple[str, str]:
    load_dotenv()
    key = os.environ.get("ALPACA_API_KEY", "").strip()
    secret = os.environ.get("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError(
            "Set ALPACA_API_KEY and ALPACA_SECRET_KEY in .env (or environment)."
        )
    return key, secret


def init_clients() -> None:
    global _trading, _data
    key, secret = _require_keys()
    _trading = TradingClient(key, secret, paper=True)
    _data = CryptoHistoricalDataClient(key, secret)
    logger.info("Alpaca TradingClient (paper) and CryptoHistoricalDataClient initialized.")


def _alpaca_bars_to_df(bar_list: list) -> pd.DataFrame:
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
    """
    Index of the latest **fully closed** 4h bar (``iloc``).

    If the newest row is still inside its 4h window, evaluate the previous bar.
    """
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
    norm = SYMBOL.replace("/", "")
    for p in trading.get_all_positions():
        if p.symbol.replace("/", "") == norm:
            return True
    return False


def check_market_and_trade() -> None:
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
                "Insufficient BTC/USD 4h history (have %s, need ≥ %s).",
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

        if not _strategy.long_entry_signal(row):
            reason = _strategy.explain_skip_long(row)
            logger.info("No entry: %s", reason or "conditions not met")
            return

        if _already_long_btc(trading):
            logger.info("Skip BUY: already have a %s position.", SYMBOL)
            return

        account = trading.get_account()
        equity = float(account.portfolio_value)
        entry_px = float(row["close"])
        atr = float(row[COL_ATR_14])

        risk = RiskManager(initial_balance=equity)
        bracket = risk.build_long_bracket(entry_px, atr)
        if bracket is None:
            logger.warning("RiskManager rejected bracket (ATR/balance).")
            return

        qty = round(bracket.quantity, 8)
        if qty <= 0:
            logger.warning("Computed qty <= 0; skip.")
            return

        tp = round(bracket.take_profit, 2)
        sl = round(bracket.stop_loss, 2)

        order_req = MarketOrderRequest(
            symbol=SYMBOL,
            qty=qty,
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp),
            stop_loss=StopLossRequest(stop_price=sl),
        )

        logger.info(
            "Submitting BRACKET market BUY qty=%s entry≈%s TP=%s SL=%s (1%% risk $%.2f equity $%.2f)",
            qty,
            entry_px,
            tp,
            sl,
            bracket.risk_usd,
            equity,
        )
        order = trading.submit_order(order_req)
        logger.info("Order submitted: id=%s status=%s", order.id, order.status)

    except Exception:
        logger.exception("check_market_and_trade failed")


def main() -> None:
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

    utc_hours = (0, 4, 8, 12, 16, 20)
    for h in utc_hours:
        schedule.every().day.at(f"{h:02d}:00", tz="UTC").do(check_market_and_trade)
        logger.info("Scheduled check at %02d:00 UTC", h)

    logger.info("Live loop running (schedule.run_pending every 60s). Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
