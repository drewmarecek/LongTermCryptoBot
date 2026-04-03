"""SQLite persistence for simulated paper trades."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Literal

from crypto_bot import config

logger = logging.getLogger(__name__)

TradeStatus = Literal["OPEN", "CLOSED"]


class SQLiteLogger:
    def __init__(self, db_path: str | Path = config.DEFAULT_TRADES_DB) -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        """Drop and recreate ``paper_trades`` so each backtest run starts empty."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("DROP TABLE IF EXISTS paper_trades")
            conn.execute(
                """
                CREATE TABLE paper_trades (
                    trade_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol     TEXT    NOT NULL,
                    entry_time TEXT    NOT NULL,
                    entry_price REAL   NOT NULL,
                    quantity    REAL   NOT NULL,
                    stop_loss   REAL   NOT NULL,
                    take_profit REAL   NOT NULL,
                    status      TEXT   NOT NULL,
                    exit_price  REAL,
                    exit_time   TEXT,
                    pnl         REAL,
                    fees_paid   REAL
                )
                """
            )
            conn.commit()
        logger.info(
            "SQLite trade log reset (fresh paper_trades): %s",
            self.db_path.resolve(),
        )

    def open_trade(
        self,
        *,
        symbol: str,
        entry_time: str,
        entry_price: float,
        quantity: float,
        stop_loss: float,
        take_profit: float,
    ) -> int:
        """Insert a new OPEN row; returns ``trade_id``."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO paper_trades (
                    symbol, entry_time, entry_price, quantity,
                    stop_loss, take_profit, status
                ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN')
                """,
                (symbol, entry_time, entry_price, quantity, stop_loss, take_profit),
            )
            conn.commit()
            tid = int(cur.lastrowid)
        logger.info(
            "OPEN trade_id=%s %s qty=%.8f entry=%.6f SL=%.6f TP=%.6f",
            tid,
            symbol,
            quantity,
            entry_price,
            stop_loss,
            take_profit,
        )
        return tid

    def close_trade(
        self,
        trade_id: int,
        *,
        exit_price: float,
        exit_time: str,
    ) -> tuple[float, float]:
        """
        Load the open row, compute PnL and fees (0.1% per leg), mark CLOSED.

        Returns ``(pnl_net, fees_paid)``. PnL is net of fees.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT entry_price, quantity, status FROM paper_trades
                WHERE trade_id = ?
                """,
                (trade_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"No trade with trade_id={trade_id}")
            entry_price, quantity, status = row
            if status != "OPEN":
                raise ValueError(f"Trade {trade_id} is not OPEN (status={status!r})")

            entry_fee = entry_price * quantity * config.FEE_RATE_PER_LEG
            exit_fee = exit_price * quantity * config.FEE_RATE_PER_LEG
            fees_paid = entry_fee + exit_fee
            gross = (exit_price - entry_price) * quantity
            pnl = gross - fees_paid

            conn.execute(
                """
                UPDATE paper_trades SET
                    status = 'CLOSED',
                    exit_price = ?,
                    exit_time = ?,
                    pnl = ?,
                    fees_paid = ?
                WHERE trade_id = ?
                """,
                (exit_price, exit_time, pnl, fees_paid, trade_id),
            )
            conn.commit()

        logger.info(
            "CLOSED trade_id=%s exit=%.6f net_pnl=%.2f fees=%.2f",
            trade_id,
            exit_price,
            pnl,
            fees_paid,
        )
        return pnl, fees_paid

    def print_summary(self) -> None:
        """
        Print aggregate stats for **closed** paper trades (win rate, PnL, fees).
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(pnl), 0.0),
                    COALESCE(SUM(fees_paid), 0.0)
                FROM paper_trades
                WHERE status = 'CLOSED'
                """
            ).fetchone()

        total_closed = int(row[0] or 0)
        wins = int(row[1] or 0)
        total_pnl = float(row[2] or 0.0)
        total_fees = float(row[3] or 0.0)
        win_rate_pct = (100.0 * wins / total_closed) if total_closed else 0.0

        line = "=" * 52
        print(line)
        print(" PAPER TRADING SUMMARY (CLOSED TRADES)")
        print(line)
        print(f"  Total Trades Taken : {total_closed}")
        print(f"  Win Rate           : {win_rate_pct:.2f}% ({wins} wins / {total_closed - wins} losses)")
        print(f"  Total PnL          : ${total_pnl:,.2f}")
        print(f"  Total Fees Paid    : ${total_fees:,.2f}")
        print(line)

        open_rows = self._count_open_trades()
        if open_rows:
            print(f"  Note: {open_rows} OPEN trade(s) excluded from summary.")
            print(line)

        logger.info(
            "Summary: closed=%s win_rate=%.2f%% pnl=$%.2f fees=$%.2f",
            total_closed,
            win_rate_pct,
            total_pnl,
            total_fees,
        )

    def _count_open_trades(self) -> int:
        with self._connect() as conn:
            r = conn.execute(
                "SELECT COUNT(*) FROM paper_trades WHERE status = 'OPEN'",
            ).fetchone()
        return int(r[0] or 0)
