"""Bollinger lower-band snap-back + volume spike filter (long-only)."""

from __future__ import annotations

import pandas as pd
import pandas_ta as ta

# Column names
COL_EMA_200 = "EMA_200"
COL_ATR_14 = "ATRr_14"
COL_BBL_20_2 = "BBL_20_2.0_2.0"
COL_VOL_SMA_20 = "SMA_VOL_20"


class StrategyEngine:
    """
    Macro uptrend + same-bar Bollinger snap-back + volume spike:

    1. **Trend:** ``close > EMA_200``
    2. **Pierce:** ``low`` < lower Bollinger band (20, 2σ)
    3. **Snap-back:** ``close`` > lower band
    4. **Volume:** ``volume > 1.5 ×`` 20-period SMA of volume
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

        need = (COL_EMA_200, COL_ATR_14, COL_BBL_20_2)
        missing = [c for c in need if c not in out.columns]
        if missing:
            raise RuntimeError(
                f"pandas_ta did not produce expected columns: {missing}. "
                f"Got bands: {[c for c in out.columns if c.startswith('BBL')]}"
            )
        if COL_VOL_SMA_20 not in out.columns:
            raise RuntimeError(f"Volume SMA column {COL_VOL_SMA_20!r} missing after ta.sma.")

        return out

    def long_entry_signal(self, row: pd.Series) -> bool:
        """BB snap-back in uptrend with volume > 1.5× 20-period SMA."""
        bbl = row.get(COL_BBL_20_2)
        v_sma = row.get(COL_VOL_SMA_20)
        vol = row.get("volume")
        if any(
            pd.isna(x)
            for x in (
                row.get(COL_EMA_200),
                bbl,
                row.get("low"),
                row.get("close"),
                vol,
                v_sma,
            )
        ):
            return False
        lo = float(row["low"])
        cl = float(row["close"])
        ema = float(row[COL_EMA_200])
        bb = float(bbl)
        v = float(vol)
        vs = float(v_sma)
        thresh = vs * self.VOLUME_SPIKE_MULT
        return bool(
            cl > ema
            and lo < bb
            and cl > bb
            and v > thresh
        )

    def explain_skip_long(self, row: pd.Series) -> str | None:
        """
        If ``long_entry_signal`` is False, return a short human-readable reason; else None.
        """
        base_cols = (COL_EMA_200, COL_BBL_20_2, "low", "close", "volume", COL_VOL_SMA_20)
        if any(pd.isna(row.get(c)) for c in base_cols):
            return "incomplete indicators (warm-up or NaN)"

        lo, cl = float(row["low"]), float(row["close"])
        ema, bb = float(row[COL_EMA_200]), float(row[COL_BBL_20_2])
        v, vs = float(row["volume"]), float(row[COL_VOL_SMA_20])
        thresh = vs * self.VOLUME_SPIKE_MULT

        if cl <= ema:
            return (
                f"close {cl:.6f} not above EMA200 {ema:.6f} "
                "(BB Lower Band snap-back requires macro uptrend)"
            )

        if lo >= bb:
            return (
                f"low {lo:.6f} not below lower BB {bb:.6f} "
                "(waiting for pierce of lower band — snap-back setup)"
            )

        if cl <= bb:
            return (
                f"close {cl:.6f} not above lower BB {bb:.6f} "
                "(pierced band but no snap-back close yet)"
            )

        if v <= thresh:
            return (
                f"Volume spike missing (< {self.VOLUME_SPIKE_MULT}x SMA): "
                f"volume {v:.4f} vs threshold {thresh:.4f} (SMA_VOL_20={vs:.4f})"
            )

        return None
