# LongTermCryptoBot

Repository: [github.com/drewmarecek/LongTermCryptoBot](https://github.com/drewmarecek/LongTermCryptoBot)

Paper-trading toolkit for crypto swing-style logic on **4h** candles: **macro trend (EMA 200)** + **Bollinger lower-band snap-back** + **volume spike**, with optional **offline backtests** (SQLite) and an **Alpaca paper live** runner for **BTC/USD**.

## Features

- **Backtest** (`python -m crypto_bot`): CCXT or CSV OHLCV, paginated history, SQLite `paper_trades` (reset each run), configurable via env vars.
- **Live paper** (`python live_bot.py`): Alpaca **paper** `TradingClient` + `CryptoHistoricalDataClient`, same `StrategyEngine` / `RiskManager` (1% risk, 2× ATR stop, 6× ATR target = 1:3 R:R), bracket market orders on **BTC/USD**, scheduled every **4h on the UTC hour** (00, 04, 08, 12, 16, 20).
- **Data prep** (`prepare_data.py`): Merge Binance Vision monthly klines into one clean CSV (Unix-ms OHLCV).

## Requirements

- Python 3.11+ recommended  
- See `requirements.txt` (ccxt, pandas, pandas-ta, alpaca-py, python-dotenv, schedule, pytz, …)

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

| Mode | Main env vars |
|------|----------------|
| Backtest (exchange) | `CRYPTO_BOT_EXCHANGE` (default `kucoin`), `CRYPTO_BOT_TARGET_BARS`, … |
| Backtest (CSV) | `CRYPTO_BOT_DATA_SOURCE=csv`, `CRYPTO_BOT_CSV_PATH` (default `data/btc_4h_clean.csv`) |
| Live Alpaca paper | `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` in `.env` (never commit `.env`) |

Example `.env` for live:

```env
ALPACA_API_KEY=your_paper_key
ALPACA_SECRET_KEY=your_paper_secret
```

Optional: `LOG_LEVEL=DEBUG`.

## Run backtest

From repo root (so `crypto_bot` resolves):

```bash
# Exchange (default symbols/timeframe from crypto_bot.config)
python -m crypto_bot

# Offline CSV (place file under `data/` or set path)
export CRYPTO_BOT_DATA_SOURCE=csv
export CRYPTO_BOT_CSV_PATH=data/btc_4h_clean.csv
python -m crypto_bot
```

## Run live paper bot

```bash
python live_bot.py
```

Uses `schedule` to call the strategy on UTC boundaries. Ensure your Alpaca account is **paper** and that API keys match paper endpoints.

## Prepare Binance Vision CSVs

Drop headerless monthly CSVs in `data/binance_raw/`, then:

```bash
python prepare_data.py
```

Writes `data/btc_1h_clean.csv` (script default filename); adjust paths or `config.DEFAULT_CSV_PATH` for **4h** files as needed.

## Project layout

| Path | Purpose |
|------|--------|
| `crypto_bot/` | Package: `DataEngine`, `StrategyEngine`, `RiskManager`, `SQLiteLogger`, `__main__.py` backtest loop |
| `live_bot.py` | Alpaca paper scheduling + orders |
| `prepare_data.py` | Binance Vision → single clean OHLCV CSV |
| `requirements.txt` | Dependencies |

## Disclaimer

Educational / experimental only. Past backtests do not guarantee future results. Crypto and leverage carry substantial risk; verify fee and order rules on your broker.
