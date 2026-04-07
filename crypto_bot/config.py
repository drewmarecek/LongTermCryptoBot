"""Shared defaults for symbols, timeframes, and exchange settings."""

import os
from typing import Final

# Primary pairs for the simulation (CCXT unified symbols).
DEFAULT_SYMBOLS: Final[tuple[str, ...]] = ("BTC/USDT", "ETH/USDT")

# Supported candle intervals (CCXT timeframes). V8 uses 1h momentum breakouts.
DEFAULT_TIMEFRAMES: Final[tuple[str, ...]] = ("1h",)

# Default public REST: KuCoin / Bybit typically allow deeper OHLCV than Kraken’s ~720h cap.
DEFAULT_EXCHANGE_ID: Final[str] = os.environ.get("CRYPTO_BOT_EXCHANGE", "kucoin")
DEFAULT_OHLCV_LIMIT: Final[int] = 500
# Paginated backtests: default row count for CCXT history.
DEFAULT_TARGET_OHLCV_BARS: Final[int] = 5_000
DEFAULT_OHLCV_PAGE_LIMIT: Final[int] = 1_000

# Paper trading and fees
INITIAL_PAPER_BALANCE: Final[float] = 10_000.0
RISK_PER_TRADE_FRACTION: Final[float] = 0.01  # 1% of balance at entry
FEE_RATE_PER_LEG: Final[float] = 0.001  # 0.1% on notional per side

DEFAULT_TRADES_DB: Final[str] = "trades.db"

# Offline OHLCV: CRYPTO_BOT_DATA_SOURCE=csv — glob ``CRYPTO_BOT_CSV_GLOB`` under ``CRYPTO_BOT_DATA_DIR``.
DEFAULT_CSV_GLOB: Final[str] = "*_1h_clean.csv"
DEFAULT_CSV_PATH: Final[str] = "data/btc_1h_clean.csv"
