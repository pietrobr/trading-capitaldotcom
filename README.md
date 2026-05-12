# trading-capitaldotcom

Trading bot for Capital.com (demo + live) with pluggable strategies and multi-session scheduling.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # then edit .env with your credentials
python -m src.main       # starts the web server on http://127.0.0.1:8000
```

The web server starts even if `config.yaml` is missing/invalid or credentials
are not set — the dashboard will show what is missing. Open
<http://127.0.0.1:8000/> to see status, planned sessions, and strategies.

## Account switch

`config.yaml` → `account.mode: demo | live`. Default is `demo`. Live requires
`LIVE_ORDERS=true` in `.env` AND the `--confirm-live` CLI flag.

## Strategies

- **Quick Flip Scalper** (`src/strategies/quick_flip_scalper.py`): opening-range
  breakout on the first 15m candle, ATR(14) daily filter (range >= 25% ATR),
  pattern entry on 5m (Hammer / Inv. Hammer / Bullish or Bearish Engulfing),
  TP at the opposite side of the box, SL at the pattern candle extreme,
  90-minute entry window.

## Sessions

Each strategy is deployed per `(symbol, session)` pair. Multiple sessions per
day per symbol are supported (e.g. London + NY on EURUSD). See `config.yaml`.
