"""Position sizing and initial stop from ATR (V8: no fixed take-profit; trail handled in the bot)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from crypto_bot import config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BracketOrder:
    """Sized long: initial stop at entry − 2×ATR; optional fixed TP omitted in V8."""

    entry_price: float
    stop_loss: float
    take_profit: float | None
    quantity: float
    risk_usd: float
    atr_used: float


class RiskManager:
    """
    1% of current **cash equity** at risk at the **initial** stop (notional may use leverage).

    Initial stop: ``entry - 2 * ATR``. No fixed take-profit — exits are via trailing stop in simulation/live.

    Quantity (base units) = ``risk_usd / (entry - stop)``.
    Notional cap: ``balance * LEVERAGE_MULTIPLIER``.
    """

    SL_ATR_MULTIPLIER = 2.0
    LEVERAGE_MULTIPLIER = 2.0

    def __init__(self, initial_balance: float = config.INITIAL_PAPER_BALANCE) -> None:
        self.balance = float(initial_balance)

    @property
    def risk_amount_usd(self) -> float:
        return self.balance * config.RISK_PER_TRADE_FRACTION

    def apply_realized_pnl(self, pnl_net: float) -> None:
        """Update paper balance after a closed trade (PnL includes fees)."""
        self.balance += pnl_net
        logger.info("Balance updated to $%.2f after realized PnL $%.2f", self.balance, pnl_net)

    @staticmethod
    def trailing_stop_level(close: float, atr: float) -> float:
        """Long trail anchor: current close minus ``SL_ATR_MULTIPLIER * atr`` (ratchet up only in the bot)."""
        return float(close) - RiskManager.SL_ATR_MULTIPLIER * float(atr)

    def build_long_bracket(self, entry_price: float, atr: float) -> BracketOrder | None:
        """
        Compute initial stop and size for a long. ``take_profit`` is always ``None`` (V8).
        """
        if self.balance <= 0:
            logger.warning("Balance is non-positive (%.2f); cannot size position.", self.balance)
            return None
        if atr is None or not atr > 0:
            logger.warning("ATR missing or non-positive; cannot size position.")
            return None

        risk_per_unit = self.SL_ATR_MULTIPLIER * atr
        stop = entry_price - risk_per_unit

        if stop <= 0 or stop >= entry_price:
            logger.warning(
                "Invalid stop (entry=%.6f stop=%.5f atr=%.6f); skip.",
                entry_price,
                stop,
                atr,
            )
            return None

        risk_usd = self.risk_amount_usd
        if risk_usd <= 0:
            logger.warning("Risk amount is non-positive (%.6f); cannot size position.", risk_usd)
            return None
        risk_distance = entry_price - stop
        if risk_distance <= 0:
            logger.warning("Risk distance is non-positive (%.8f); cannot size position.", risk_distance)
            return None
        quantity = risk_usd / risk_distance
        notional = quantity * entry_price
        max_notional = self.balance * self.LEVERAGE_MULTIPLIER

        if notional > max_notional:
            logger.warning(
                "Notional exceeds leverage cap: need $%.2f, max $%.2f (balance $%.2f × %.1fx); skip entry.",
                notional,
                max_notional,
                self.balance,
                self.LEVERAGE_MULTIPLIER,
            )
            return None

        logger.debug(
            "Long: entry=%.6f stop=%.6f qty=%.8f risk_usd=%.2f atr=%.6f (no fixed TP)",
            entry_price,
            stop,
            quantity,
            risk_usd,
            atr,
        )

        return BracketOrder(
            entry_price=entry_price,
            stop_loss=stop,
            take_profit=None,
            quantity=quantity,
            risk_usd=risk_usd,
            atr_used=atr,
        )
