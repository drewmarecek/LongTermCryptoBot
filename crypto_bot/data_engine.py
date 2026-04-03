"""OHLCV data fetching via CCXT, normalized to pandas DataFrames."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import ccxt
import numpy as np
import pandas as pd

from crypto_bot import config

logger = logging.getLogger(__name__)

_COLUMN_ALIASES: dict[str, str] = {
    "timestamp": "timestamp",
    "time": "timestamp",
    "datetime": "timestamp",
    "date": "timestamp",
    "ts": "timestamp",
    "open": "open",
    "o": "open",
    "high": "high",
    "h": "high",
    "low": "low",
    "l": "low",
    "close": "close",
    "c": "close",
    "volume": "volume",
    "vol": "volume",
    "base_volume": "volume",
    "v": "volume",
}


def load_ohlcv_from_csv(file_path: str | Path) -> pd.DataFrame:
    """
    Load OHLCV from a local CSV into the same shape as ``DataEngine`` / CCXT output:

    UTC ``DatetimeIndex`` and float columns ``open``, ``high``, ``low``, ``close``, ``volume``.

    The canonical layout (from ``prepare_data.py`` → ``data/btc_1h_clean.csv``) is::

        timestamp,open,high,low,close,volume

    where ``timestamp`` is **Unix milliseconds** (int), and the index is rebuilt in UTC.

    Also accepts common header aliases (case-insensitive) and epoch columns as seconds,
    milliseconds, microseconds, ISO strings, or numeric strings.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"OHLCV CSV not found: {path.resolve()}")

    df = pd.read_csv(path)
    lower_map = {
        str(c).strip().lower().replace(" ", "_"): c for c in df.columns
    }
    rename_in = {}
    for low, orig in lower_map.items():
        if low in _COLUMN_ALIASES:
            rename_in[orig] = _COLUMN_ALIASES[low]
        elif low in ("open", "high", "low", "close", "volume"):
            rename_in[orig] = low

    df = df.rename(columns=rename_in)

    needed = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = sorted(needed - set(df.columns))
    if missing:
        raise ValueError(
            f"CSV missing columns {missing} after normalizing headers. "
            f"Found: {list(df.columns)}"
        )

    ts_series = df["timestamp"]
    idx = _parse_timestamp_index(ts_series)

    out = df[["open", "high", "low", "close", "volume"]].copy()
    for col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out.index = idx
    out = out[~out.index.isna()]
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]

    if out.isna().any().any():
        bad = int(out.isna().any(axis=1).sum())
        logger.warning("Dropping %s row(s) with NaN in OHLCV columns.", bad)
        out = out.dropna()

    logger.info(
        "Loaded CSV %s rows=%s | range=%s → %s",
        path.name,
        len(out),
        out.index.min(),
        out.index.max(),
    )
    return out


def _binance_epoch_to_milliseconds(s: pd.Series) -> pd.Series:
    """
    Align raw Binance-style epoch values to **milliseconds** before ``to_datetime``.

    Spot klines use ms, but some Vision dumps use µs or ns (larger integers).
    """
    t = pd.to_numeric(s, errors="coerce")
    raw = t.to_numpy()
    out = raw.astype("float64", copy=True)
    mask = ~np.isnan(out)
    if mask.any():
        vals = out[mask].astype(np.int64, copy=False)
        scaled = np.where(
            vals >= 10**18,
            vals // 1_000_000,
            np.where(vals >= 10**15, vals // 1_000, vals),
        ).astype(np.float64)
        out[mask] = scaled
    return pd.Series(out, index=t.index, dtype="float64")


def _parse_timestamp_index(ts_series: pd.Series) -> pd.DatetimeIndex:
    s = ts_series
    if not pd.api.types.is_numeric_dtype(s):
        numeric = pd.to_numeric(s, errors="coerce")
        if bool(numeric.notna().all()) and len(numeric) > 0:
            s = numeric

    if pd.api.types.is_numeric_dtype(s):
        s = _binance_epoch_to_milliseconds(s)
        sample = float(s.dropna().iloc[0]) if s.notna().any() else 0.0
        if sample > 1e15:
            parsed = pd.to_datetime(s, unit="us", utc=True)
        elif sample > 1e12:
            parsed = pd.to_datetime(s, unit="ms", utc=True)
        elif sample > 1e9:
            parsed = pd.to_datetime(s, unit="s", utc=True)
        else:
            parsed = pd.to_datetime(s, utc=True)
    else:
        parsed = pd.to_datetime(s, utc=True, format="mixed")

    idx = pd.DatetimeIndex(parsed)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")
    return idx


class DataEngine:
    """
    Pulls historical candles from a CCXT exchange and returns labeled DataFrames.

    The DataFrame columns are: timestamp (UTC index), open, high, low, close, volume.
    """

    def __init__(
        self,
        exchange_id: str = config.DEFAULT_EXCHANGE_ID,
        *,
        enable_rate_limit: bool = True,
        timeout_ms: int | None = 30_000,
    ) -> None:
        self._exchange_id = exchange_id
        exchange_class = getattr(ccxt, exchange_id, None)
        if exchange_class is None:
            raise ValueError(f"Unknown CCXT exchange id: {exchange_id!r}")

        opts: dict[str, Any] = {"enableRateLimit": enable_rate_limit}
        if timeout_ms is not None:
            opts["timeout"] = timeout_ms

        self.exchange = exchange_class(opts)
        logger.info(
            "DataEngine initialized: exchange=%s rate_limit=%s",
            exchange_id,
            enable_rate_limit,
        )

    def _timeframe_supported(self, timeframe: str) -> bool:
        tf = self.exchange.timeframes or {}
        return timeframe in tf

    def _ohlcv_page_limit(self, requested: int | None) -> int:
        opt = self.exchange.options or {}
        raw = opt.get("fetchOHLCVLimit") or opt.get("maxOHLCVLimit") or config.DEFAULT_OHLCV_PAGE_LIMIT
        cap = max(1, min(int(raw), 1000))
        if requested is not None:
            return max(1, min(int(requested), cap))
        return cap

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int | None = None,
        since_ms: int | None = None,
    ) -> pd.DataFrame:
        """
        Single CCXT request. For long histories use ``fetch_ohlcv_history``.

        Parameters
        ----------
        symbol : str
            Unified CCXT symbol, e.g. ``BTC/USDT``.
        timeframe : str
            Candle size, e.g. ``1h`` or ``4h`` (must exist on the exchange).
        limit : int, optional
            Number of candles (exchange max applies). Defaults to ``DEFAULT_OHLCV_LIMIT``.
        since_ms : int, optional
            Milliseconds UTC timestamp; fetches candles at or after this open time when set.
        """
        if limit is None:
            limit = config.DEFAULT_OHLCV_LIMIT

        if not self._timeframe_supported(timeframe):
            available = sorted((self.exchange.timeframes or {}).keys())
            logger.error(
                "Timeframe %r not supported on %s. Available: %s",
                timeframe,
                self._exchange_id,
                available,
            )
            raise ValueError(
                f"Unsupported timeframe {timeframe!r} for {self._exchange_id}"
            )

        logger.debug(
            "Fetching OHLCV: symbol=%s timeframe=%s limit=%s since_ms=%s",
            symbol,
            timeframe,
            limit,
            since_ms,
        )

        raw = self.exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
        return _raw_to_df(raw, symbol, timeframe, log_range=True)

    def fetch_ohlcv_history(
        self,
        symbol: str,
        timeframe: str = "1h",
        *,
        target_bars: int = config.DEFAULT_TARGET_OHLCV_BARS,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """
        Stitch multiple ``fetch_ohlcv`` calls (``since`` cursor) into one DataFrame.

        Walks forward from ``now - target_bars * timeframe`` toward the present,
        de-duplicates overlapping rows, sorts chronologically, then keeps the last
        ``target_bars`` rows when extra overlap exists.
        """
        if target_bars < 1:
            return _empty_ohlcv_frame()

        if not self._timeframe_supported(timeframe):
            available = sorted((self.exchange.timeframes or {}).keys())
            logger.error(
                "Timeframe %r not supported on %s. Available: %s",
                timeframe,
                self._exchange_id,
                available,
            )
            raise ValueError(
                f"Unsupported timeframe {timeframe!r} for {self._exchange_id}"
            )

        tf_sec = float(self.exchange.parse_timeframe(timeframe))
        tf_ms = int(tf_sec * 1000)
        page = self._ohlcv_page_limit(page_limit)

        max_iterations = max(10, target_bars // max(page // 2, 1) + 10)

        logger.info(
            "Paginating OHLCV (backward chaining): %s %s target_bars=%s page_limit=%s",
            symbol,
            timeframe,
            target_bars,
            page,
        )

        # Newest chunk first (CCXT: ``since=None`` → latest ``limit`` candles, ascending).
        tail = self.exchange.fetch_ohlcv(symbol, timeframe, since=None, limit=page)
        if not tail:
            logger.warning("Empty initial OHLCV for %s %s", symbol, timeframe)
            return _empty_ohlcv_frame()

        chunks: list[list[list[Any]]] = [tail]
        oldest_ms = int(tail[0][0])
        iteration = 0
        min_ts = max(0, self.exchange.milliseconds() - target_bars * tf_ms)

        while sum(len(c) for c in chunks) < target_bars and iteration < max_iterations:
            iteration += 1
            # Next page starts far enough back to fetch another full window before ``oldest_ms``.
            since_ms = max(0, oldest_ms - page * tf_ms)
            batch = self.exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=page)
            if not batch:
                logger.debug("Empty batch at since_ms=%s; stopping.", since_ms)
                break

            first_ts = int(batch[0][0])
            last_ts = int(batch[-1][0])

            if last_ts < oldest_ms:
                older = batch
            else:
                older = [c for c in batch if c[0] < oldest_ms]

            if not older:
                logger.debug(
                    "No older candles before %s (batch %s→%s); stopping.",
                    oldest_ms,
                    first_ts,
                    last_ts,
                )
                break

            if int(older[-1][0]) >= oldest_ms:
                logger.debug("Older slice did not end before chain; stopping.")
                break

            chunks.insert(0, older)
            new_oldest = int(older[0][0])
            if new_oldest >= oldest_ms:
                break
            oldest_ms = new_oldest

            if oldest_ms <= min_ts:
                break

        flat: list[list[Any]] = [row for part in chunks for row in part]
        df = _raw_to_df(flat, symbol, timeframe, log_range=False)

        if df.empty:
            logger.warning("No OHLCV after pagination for %s %s", symbol, timeframe)
            return df

        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()
        if len(df) > target_bars:
            df = df.iloc[-target_bars:]

        if len(df) < target_bars:
            logger.warning(
                "OHLCV depth short of target: got %s rows (wanted %s) for %s %s — "
                "exchange history may be shallower than requested.",
                len(df),
                target_bars,
                symbol,
                timeframe,
            )

        logger.info(
            "Stitched OHLCV: %s %s rows=%s | range=%s → %s (pages=%s)",
            symbol,
            timeframe,
            len(df),
            df.index.min(),
            df.index.max(),
            len(chunks),
        )
        return df

    def fetch_default_universe(
        self,
        timeframe: str = "1h",
        target_bars: int | None = None,
        page_limit: int | None = None,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch OHLCV for all ``DEFAULT_SYMBOLS`` into a symbol-keyed dict.

        Uses paginated history when ``target_bars`` is set (default: ``DEFAULT_TARGET_OHLCV_BARS``).
        """
        if target_bars is None:
            target_bars = config.DEFAULT_TARGET_OHLCV_BARS
        out: dict[str, pd.DataFrame] = {}
        for sym in config.DEFAULT_SYMBOLS:
            logger.info("Loading universe member: %s @ %s", sym, timeframe)
            out[sym] = self.fetch_ohlcv_history(
                sym,
                timeframe=timeframe,
                target_bars=target_bars,
                page_limit=page_limit,
            )
        return out


def _raw_to_df(
    raw: list[list[Any]],
    symbol: str,
    timeframe: str,
    *,
    log_range: bool,
) -> pd.DataFrame:
    if not raw:
        return _empty_ohlcv_frame()

    df = pd.DataFrame(
        raw,
        columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.drop(columns=["timestamp_ms"])
    df = df.set_index("timestamp").sort_index()

    if log_range:
        logger.info(
            "Fetched %s rows: %s %s | range=%s → %s",
            len(df),
            symbol,
            timeframe,
            df.index.min(),
            df.index.max(),
        )
    return df


def _empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["open", "high", "low", "close", "volume"],
        dtype="float64",
    )
