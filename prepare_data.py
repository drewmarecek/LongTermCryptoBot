#!/usr/bin/env python3
"""
Merge Binance Vision monthly kline CSVs (no headers) into one clean OHLCV file.

Input : data/binance_raw/*.csv  (12 columns per Binance Vision spec)
Output: data/btc_1h_clean.csv    (header + Unix-ms ``timestamp``, OHLCV columns)

Output rows are headered CSV with Unix open-time in **milliseconds** (same convention as
CCXT), matching ``crypto_bot.data_engine.load_ohlcv_from_csv``.

Vision shards may mix millisecond and microsecond open times; this script normalizes to ms.

Usage (from repo root):
    python prepare_data.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "binance_raw"
OUT_PATH = ROOT / "data" / "btc_1h_clean.csv"

# Binance Vision spot klines layout (no header row in source ZIPs).
VISION_COLUMNS: list[str] = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

KEEP = ["timestamp", "open", "high", "low", "close", "volume"]


def _open_time_to_milliseconds_unix(ts: pd.Series) -> pd.Series:
    """
    Binance Vision ``open`` time is usually **milliseconds**, but some monthly
    shards use **microseconds** (or very rarely **nanoseconds**).

    Canonical output is always **Unix ms** to match CCXT and our CSV loader.
    """
    t = ts.astype("int64")
    return pd.Series(
        np.where(
            t >= 10**18,
            t // 1_000_000,
            np.where(t >= 10**15, t // 1_000, t),
        ),
        dtype="int64",
        index=ts.index,
    )


def main() -> int:
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        print(f"No .csv files under {RAW_DIR}", file=sys.stderr)
        return 1

    dtypes = {
        "timestamp": "int64",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "close_time": "int64",
        "quote_asset_volume": "float64",
        "number_of_trades": "int64",
        "taker_buy_base_asset_volume": "float64",
        "taker_buy_quote_asset_volume": "float64",
        "ignore": "int64",
    }

    frames: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_csv(
            path,
            header=None,
            names=VISION_COLUMNS,
            dtype=dtypes,
        )
        df = df[KEEP].copy()
        frames.append(df)
        print(f"  read {path.name}: {len(df)} rows")

    combined = pd.concat(frames, ignore_index=True)
    combined["timestamp"] = _open_time_to_milliseconds_unix(combined["timestamp"])
    combined = combined.sort_values("timestamp", kind="mergesort").drop_duplicates(
        subset=["timestamp"], keep="last"
    )

    for col in ["open", "high", "low", "close", "volume"]:
        combined[col] = pd.to_numeric(combined[col], errors="coerce")

    before = len(combined)
    combined = combined.dropna()
    dropped = before - len(combined)
    if dropped:
        print(f"  dropped {dropped} row(s) with NaN in OHLCV")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    out = combined[KEEP].reset_index(drop=True)
    out.to_csv(OUT_PATH, index=False)

    t_first = pd.to_datetime(out["timestamp"].iloc[0], unit="ms", utc=True)
    t_last = pd.to_datetime(out["timestamp"].iloc[-1], unit="ms", utc=True)

    print(f"Wrote {OUT_PATH} ({len(out)} rows)")
    print(f"  range: {t_first} → {t_last}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
