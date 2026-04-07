Failed crypto trading experiment focused on end-to-end trading infrastructure.

## Project Context

This repository documents an experiment in systematic crypto trading on 1h candles.  
The strategy research result was not strong enough to beat passive benchmarks, and that outcome is intentionally preserved here.

- **Transparent result:** the V7 Aggressive Strategy backtest returned **-38.8%** over ~26 months (**Jan 2024 - Feb 2026**)
- **Benchmark context:** this underperformed a passive S&P 500 allocation over the same period
- **Why publish anyway:** the codebase demonstrates practical engineering for market data, simulation, execution, and risk controls

## What This Repo Shows

- **V8 momentum breakout implementation (1h):** `close > EMA_200`, `close > upper Bollinger band (20, 2)`, and `volume > 1.5x SMA_VOL_20`
- **Execution model:** market entry on the signal bar close, initial stop at `entry - 2x ATR(14)`, then trailing stop ratchet (`max(old_stop, close - 2x ATR)`)
- **Multi-asset CSV backtester** (`python -m crypto_bot`) that iterates a basket of cleaned `*_1h_clean.csv` files
- **Live execution engine** (`live_bot.py`) using Alpaca paper APIs for real-time signal checks, entry, and stop updates
- **Risk manager** with dynamic ATR-based position sizing, 1% account risk targeting, and leverage notional caps
- **SQLite trade logger** for reproducible, queryable paper-trade records
- **Environment-driven config** for symbols, source mode (exchange/csv), log level, and API credentials

## Tech Stack

- Python
- Pandas
- Pandas_TA
- Alpaca-py
- SQLite
- Docker-ready workflow (standard Python image + `requirements.txt` entrypoint setup)

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` for live paper mode:

```env
ALPACA_API_KEY=your_paper_key
ALPACA_SECRET_KEY=your_paper_secret
```

Run backtests:

```bash
export CRYPTO_BOT_DATA_SOURCE=csv
python -m crypto_bot
```

Run live paper loop:

```bash
python live_bot.py
```
