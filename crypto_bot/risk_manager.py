"""Position sizing and stop / take-profit brackets from ATR."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from crypto_bot import config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BracketOrder:
    """Risk-defined long setup derived from entry and ATR."""

    entry_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    risk_usd: float
    atr_used: float


class RiskManager:
    """
    1% of current paper balance at risk if the stop is touched.

    Stop: entry - 2 * ATR. Take profit: entry + 6 * ATR (3:1 reward:risk in ATR units).
    Quantity (base units) = risk_usd / (entry - stop).
    """

    SL_ATR_MULTIPLIER = 2.0
    TP_ATR_MULTIPLIER = 6.0

    def __init__(self, initial_balance: float = config.INITIAL_PAPER_BALANCE) -> None:
        self.balance = float(initial_balance)

    @property
    def risk_amount_usd(self) -> float:
        return self.balance * config.RISK_PER_TRADE_FRACTION

    def apply_realized_pnl(self, pnl_net: float) -> None:
        """Update paper balance after a closed trade (PnL includes fees)."""
        self.balance += pnl_net
        logger.info("Balance updated to $%.2f after realized PnL $%.2f", self.balance, pnl_net)

    def build_long_bracket(self, entry_price: float, atr: float) -> BracketOrder | None:
        """
        Compute stop, target, and size for a long. Returns None if the setup is invalid.
        """
        if atr is None or not atr > 0:
            logger.warning("ATR missing or non-positive; cannot size position.")
            return None

        risk_per_unit = self.SL_ATR_MULTIPLIER * atr
        stop = entry_price - risk_per_unit
        take_profit = entry_price + self.TP_ATR_MULTIPLIER * atr

        if stop <= 0 or stop >= entry_price:
            logger.warning(
                "Invalid stop (entry=%.6f stop=%.5f atr=%.6f); skip.",
                entry_price,
                stop,
                atr,
            )
            return None

        risk_usd = self.risk_amount_usd
        quantity = risk_usd / (entry_price - stop)
        notional = quantity * entry_price

        if notional > self.balance:
            logger.warning(
                "Insufficient paper balance: need $%.2f notional, have $%.2f; skip entry.",
                notional,
                self.balance,
            )
            return None

        logger.debug(
            "Bracket: entry=%.6f stop=%.6f tp=%.6f qty=%.8f risk_usd=%.2f atr=%.6f",
            entry_price,
            stop,
            take_profit,
            quantity,
            risk_usd,
            atr,
        )

        return BracketOrder(
            entry_price=entry_price,
            stop_loss=stop,
            take_profit=take_profit,
            quantity=quantity,
            risk_usd=risk_usd,
            atr_used=atr,
        )
