"""Run historical simulation: fetch data, enrich signals, wire risk + SQLite logging."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import ccxt
import pandas as pd

from crypto_bot import config
from crypto_bot.config import DEFAULT_EXCHANGE_ID, DEFAULT_SYMBOLS, DEFAULT_TIMEFRAMES
from crypto_bot.data_engine import DataEngine, load_ohlcv_from_csv
from crypto_bot.db_logger import SQLiteLogger
from crypto_bot.risk_manager import RiskManager
from crypto_bot.strategy import COL_ATR_14, StrategyEngine

logger = logging.getLogger(__name__)


@dataclass
class _OpenPosition:
    trade_id: int
    stop_loss: float


class TradingBot:
    """Row-by-row paper trading over pre-fetched OHLCV with strategy, risk, and DB log."""

    def __init__(
        self,
        strategy: StrategyEngine,
        risk_manager: RiskManager,
        db_logger: SQLiteLogger,
    ) -> None:
        self.strategy = strategy
        self.risk = risk_manager
        self.db = db_logger
        self._open: _OpenPosition | None = None

    def run_backtest(self, df: pd.DataFrame, symbol: str) -> None:
        """Simulate one symbol end-to-end over historical bars."""
        if df.empty:
            logger.warning("Empty DataFrame for %s; nothing to simulate.", symbol)
            return

        enriched = self.strategy.enrich_indicators(df)
        warmup = max(
            self.strategy.EMA_LENGTH,
            self.strategy.ATR_LENGTH,
            self.strategy.BB_LENGTH,
        )

        logger.info(
            "Starting backtest %s rows=%s warmup=%s (first simulated bar index=%s)",
            symbol,
            len(enriched),
            warmup,
            warmup,
        )

        for i in range(warmup, len(enriched)):
            bar = enriched.iloc[i]
            ts = enriched.index[i]
            ts_iso = ts.isoformat()
            low = float(bar["low"])
            high = float(bar["high"])

            if self._open is not None:
                close = float(bar["close"])
                atr = float(bar[COL_ATR_14])
                trail_level = self.risk.trailing_stop_level(close, atr)
                if trail_level > self._open.stop_loss:
                    logger.debug(
                        "[%s] Trailing stop raised %.6f → %.6f",
                        ts_iso,
                        self._open.stop_loss,
                        trail_level,
                    )
                    self._open.stop_loss = trail_level
                self._maybe_exit_long(symbol, bar, ts_iso, low, high)
                continue

            if self.strategy.long_entry_signal(bar):
                self._try_open_long(symbol, bar, ts_iso)
            else:
                reason = self.strategy.explain_skip_long(bar)
                logger.debug("[%s] Skip long: %s", ts_iso, reason)

        self._force_close_open_at_last_candle(enriched, symbol)

    def _force_close_open_at_last_candle(
        self, enriched: pd.DataFrame, symbol: str
    ) -> None:
        """Close any open trade at final close so PnL is realized in reports."""
        if self._open is None:
            return

        last = enriched.iloc[-1]
        ts_iso = enriched.index[-1].isoformat()
        exit_px = float(last["close"])
        self._finalize_long(
            symbol=symbol,
            ts_iso=ts_iso,
            exit_px=exit_px,
            tag="END_OF_SERIES",
            low=float(last["low"]),
            high=float(last["high"]),
        )

    def _try_open_long(self, symbol: str, bar: pd.Series, entry_time_iso: str) -> None:
        """Open a market-at-close long with ATR-sized risk if valid."""
        entry = float(bar["close"])
        atr = float(bar[COL_ATR_14])
        bracket = self.risk.build_long_bracket(entry, atr)
        if bracket is None:
            logger.info(
                "[%s] BUY signal ignored for %s (risk bracket invalid at entry=%.6f).",
                entry_time_iso,
                symbol,
                entry,
            )
            return

        trade_id = self.db.open_trade(
            symbol=symbol,
            entry_time=entry_time_iso,
            entry_price=bracket.entry_price,
            quantity=bracket.quantity,
            stop_loss=bracket.stop_loss,
            take_profit=bracket.take_profit,
        )
        self._open = _OpenPosition(
            trade_id=trade_id,
            stop_loss=bracket.stop_loss,
        )
        logger.info(
            "[%s] BUY %s @ %.6f | initial SL %.6f qty %.8f (risk $%.2f, trail 2×ATR)",
            entry_time_iso,
            symbol,
            bracket.entry_price,
            bracket.stop_loss,
            bracket.quantity,
            bracket.risk_usd,
        )

    def _maybe_exit_long(
        self,
        symbol: str,
        bar: pd.Series,
        ts_iso: str,
        low: float,
        high: float,
    ) -> None:
        """Exit only on trailing-stop breach for V8."""
        assert self._open is not None
        op = self._open
        sl = op.stop_loss

        if low <= sl:
            exit_px = sl
            tag = "TRAILING_STOP"
        else:
            return

        self._finalize_long(symbol, ts_iso, exit_px, tag, low, high)

    def _finalize_long(
        self,
        symbol: str,
        ts_iso: str,
        exit_px: float,
        tag: str,
        low: float,
        high: float,
    ) -> None:
        assert self._open is not None
        op = self._open
        pnl, _fees = self.db.close_trade(
            op.trade_id, exit_price=exit_px, exit_time=ts_iso
        )
        self.risk.apply_realized_pnl(pnl)

        logger.info(
            "[%s] %s %s trade_id=%s exit=%.6f net_pnl=%.2f (bar low=%.6f high=%.6f)",
            ts_iso,
            tag,
            symbol,
            op.trade_id,
            exit_px,
            pnl,
            low,
            high,
        )
        self._open = None


def _symbol_label_from_csv_path(csv_path: str) -> str:
    """``data/eth_1h_clean.csv`` → ``ETH/USDT``."""
    stem = Path(csv_path).stem
    m = re.match(r"^(.+)_1h_clean$", stem, re.IGNORECASE)
    base = m.group(1) if m else stem
    return f"{base.upper()}/USDT"


def _parse_symbols_env() -> list[str]:
    """Parse CRYPTO_BOT_SYMBOL into a symbol list (supports comma-separated values)."""
    raw = os.environ.get("CRYPTO_BOT_SYMBOL", "").strip()
    if not raw or raw.lower() == "all":
        return list(DEFAULT_SYMBOLS)
    return [s.strip() for s in raw.split(",") if s.strip()]


def main() -> int:
    """Program entrypoint for exchange/csv backtesting with a shared paper ledger."""
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    timeframe = os.environ.get("CRYPTO_BOT_TIMEFRAME", DEFAULT_TIMEFRAMES[0])

    target_raw = os.environ.get(
        "CRYPTO_BOT_TARGET_BARS",
        os.environ.get("CRYPTO_BOT_LIMIT", str(config.DEFAULT_TARGET_OHLCV_BARS)),
    )
    try:
        target_bars = int(target_raw)
    except ValueError:
        logger.error("Invalid CRYPTO_BOT_TARGET_BARS / CRYPTO_BOT_LIMIT=%r", target_raw)
        return 1

    db_path = os.environ.get("CRYPTO_BOT_TRADES_DB", config.DEFAULT_TRADES_DB)
    data_source = os.environ.get("CRYPTO_BOT_DATA_SOURCE", "exchange").strip().lower()

    strategy = StrategyEngine()
    risk = RiskManager()
    db_log = SQLiteLogger(db_path)
    logger.info("Paper trades table recreated for this run; prior DB rows cleared.")
    bot = TradingBot(strategy, risk, db_log)

    if data_source == "csv":
        data_dir = Path(os.environ.get("CRYPTO_BOT_DATA_DIR", "data"))
        glob_pat = os.environ.get("CRYPTO_BOT_CSV_GLOB", config.DEFAULT_CSV_GLOB)
        paths = sorted(data_dir.glob(glob_pat))

        single = os.environ.get("CRYPTO_BOT_CSV_PATH", "").strip()
        if single:
            paths = [Path(single)]

        if not paths:
            logger.error(
                "CSV mode: no files matching %s under %s (set CRYPTO_BOT_CSV_PATH for one file).",
                glob_pat,
                data_dir,
            )
            return 4

        logger.info(
            "CSV mode: %s file(s) | glob=%s | V8 momentum + ATR trail (offline)",
            len(paths),
            glob_pat,
        )

        for csv_path in paths:
            sym_csv = _symbol_label_from_csv_path(str(csv_path))
            try:
                raw = load_ohlcv_from_csv(str(csv_path))
            except (OSError, ValueError) as exc:
                logger.error("CSV load failed %s: %s", csv_path, exc)
                return 4

            logger.info(
                "Backtest path=%s rows=%s label=%s",
                csv_path.name,
                len(raw),
                sym_csv,
            )
            bot.run_backtest(raw, sym_csv)
            logger.info("Finished %s | balance $%.2f", sym_csv, risk.balance)

    else:
        symbols = _parse_symbols_env()
        if not symbols:
            logger.error("No symbols after parsing CRYPTO_BOT_SYMBOL")
            return 1

        page_raw = os.environ.get("CRYPTO_BOT_PAGE_LIMIT", "")
        page_limit = int(page_raw) if page_raw.strip().isdigit() else None

        try:
            engine = DataEngine()
        except ValueError as exc:
            logger.error("Failed to create DataEngine: %s", exc)
            return 1

        for symbol in symbols:
            try:
                raw = engine.fetch_ohlcv_history(
                    symbol,
                    timeframe=timeframe,
                    target_bars=target_bars,
                    page_limit=page_limit,
                )
            except ccxt.NetworkError as exc:
                logger.error("Network error (%s): %s", symbol, exc)
                return 2
            except ccxt.ExchangeError as exc:
                logger.error(
                    "Exchange error (%s on %s): %s — try CRYPTO_BOT_EXCHANGE=bybit "
                    "or another CCXT id if this venue fails.",
                    symbol,
                    DEFAULT_EXCHANGE_ID,
                    exc,
                )
                return 3

            bot.run_backtest(raw, symbol)
            logger.info("Finished backtest for %s | balance $%.2f", symbol, risk.balance)

    logger.info("Final paper balance: $%.2f", risk.balance)
    logger.info("Trades DB: %s", db_log.db_path.resolve())
    db_log.print_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
