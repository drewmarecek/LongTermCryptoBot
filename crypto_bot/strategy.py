"""V8: 1h momentum breakout — close above upper Bollinger + volume spike in macro uptrend."""

from __future__ import annotations

import pandas as pd
import pandas_ta as ta

# Column names (pandas_ta Bollinger naming)
COL_EMA_200 = "EMA_200"
COL_ATR_14 = "ATRr_14"
COL_BBU_20_2 = "BBU_20_2.0_2.0"
COL_VOL_SMA_20 = "SMA_VOL_20"


class StrategyEngine:
    """
    **V8 — momentum breakout (market entry on signal bar close):**

    1. **Macro trend:** ``close > EMA_200``
    2. **Volatility expansion:** ``close > BBU_20_2.0`` (close above upper Bollinger band)
    3. **Volume:** ``volume > 1.5 ×`` 20-period SMA of volume
    """

    EMA_LENGTH = 200
    ATR_LENGTH = 14
    BB_LENGTH = 20
    BB_STD = 2.0
    VOL_SMA_LENGTH = 20
    VOLUME_SPIKE_MULT = 1.5

    def enrich_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append EMA, ATR, Bollinger bands, and volume SMA."""
        if df.empty:
            return df.copy()

        out = df.copy()
        out.ta.ema(length=self.EMA_LENGTH, append=True)
        out.ta.atr(length=self.ATR_LENGTH, append=True)
        out.ta.bbands(length=self.BB_LENGTH, std=self.BB_STD, append=True)

        vol_sma = ta.sma(out["volume"], length=self.VOL_SMA_LENGTH)
        out[COL_VOL_SMA_20] = vol_sma

        need = (COL_EMA_200, COL_ATR_14, COL_BBU_20_2)
        missing = [c for c in need if c not in out.columns]
        if missing:
            raise RuntimeError(
                f"pandas_ta did not produce expected columns: {missing}. "
                f"Band cols: {[c for c in out.columns if c.startswith('BB')]}"
            )
        if COL_VOL_SMA_20 not in out.columns:
            raise RuntimeError(f"Volume SMA column {COL_VOL_SMA_20!r} missing after ta.sma.")

        return out

    def long_entry_signal(self, row: pd.Series) -> bool:
        """True when trend, upper-band breakout, and volume spike align."""
        bbu = row.get(COL_BBU_20_2)
        v_sma = row.get(COL_VOL_SMA_20)
        vol = row.get("volume")
        if any(
            pd.isna(x)
            for x in (
                row.get(COL_EMA_200),
                bbu,
                row.get("close"),
                vol,
                v_sma,
            )
        ):
            return False
        cl = float(row["close"])
        ema = float(row[COL_EMA_200])
        u = float(bbu)
        v = float(vol)
        vs = float(v_sma)
        thresh = vs * self.VOLUME_SPIKE_MULT
        return bool(cl > ema and cl > u and v > thresh)

    def explain_skip_long(self, row: pd.Series) -> str | None:
        """If ``long_entry_signal`` is False, return a short reason; else None."""
        base_cols = (COL_EMA_200, COL_BBU_20_2, "close", "volume", COL_VOL_SMA_20)
        if any(pd.isna(row.get(c)) for c in base_cols):
            return "incomplete indicators (warm-up or NaN)"

        cl = float(row["close"])
        ema = float(row[COL_EMA_200])
        u = float(row[COL_BBU_20_2])
        v, vs = float(row["volume"]), float(row[COL_VOL_SMA_20])
        thresh = vs * self.VOLUME_SPIKE_MULT

        if cl <= ema:
            return (
                f"close {cl:.6f} not above EMA200 {ema:.6f} "
                "(momentum breakout requires macro uptrend)"
            )

        if cl <= u:
            return (
                f"close {cl:.6f} not above upper BB {u:.6f} "
                "(no volatility-expansion breakout yet)"
            )

        if v <= thresh:
            return (
                f"Volume spike missing (< {self.VOLUME_SPIKE_MULT}x SMA): "
                f"volume {v:.4f} vs threshold {thresh:.4f} (SMA_VOL_20={vs:.4f})"
            )

        return None
