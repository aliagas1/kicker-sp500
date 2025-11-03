# detector.py — Twelve Data (detección en pre-market profesional)
import os, json, time
from datetime import datetime, timedelta, date
from dateutil import tz
import pandas as pd
import requests

# ===================== CONFIG =====================
UNIVERSE_CSV = "universe.csv"
SAVE_DIR = "results"

LOCAL_TZ = "America/Lima"            # zona de referencia usuario final
NY_TZ    = "America/New_York"        # mercado US
OPEN_TIME = "09:30:00"               # apertura oficial NYSE/NASDAQ
PREMARKET_LAST = "09:29:00"          # última vela de pre-market NY

REQUEST_SLEEP = 0.1                  # pausa suave entre requests
INTRADAY_INTERVAL = "1min"           # granularidad para hoy
INTRADAY_OUTPUTSIZE = 1000           # velas recientes (~16 h)

# ===================================================

def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_universe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.dropna(subset=["ticker"])
    df["ticker_td"] = df["ticker"].astype(str).str.strip().str.replace("-", ".", regex=False)
    return df

def ny_today() -> date:
    return datetime.now(tz.gettz(NY_TZ)).date()

def td_time_series(symbol: str, interval: str, outputsize: int, api_key: str):
    url = (
        "https://api.twelvedata.com/time_series"
        f"?symbol={symbol}&interval={interval}&outputsize={outputsize}&timezone={NY_TZ.replace('/', '%2F')}"
        f"&apikey={api_key}"
    )
    r = requests.get(url, timeout=20)
    data = r.json()
    time.sleep(REQUEST_SLEEP)
    return data

def get_prev_daily(symbol: str, api_key: str):
    data = td_time_series(symbol, "1day", 3, api_key)
    if data.get("status") != "ok" or "values" not in data:
        return None
    vals = data["values"]
    today_ny = ny_today()
    prev_row = None
    for v in vals:
        d = v.get("datetime", "")[:10]
        try:
            d_date = datetime.strptime(d, "%Y-%m-%d").date()
        except:
            continue
        if d_date < today_ny:
            prev_row = v
            break
    if not prev_row:
        return None
    try:
        return {
            "Open":  float(prev_row["open"]),
            "High":  float(prev_row["high"]),
            "Low":   float(prev_row["low"]),
            "Close": float(prev_row["close"]),
            "Date":  prev_row["datetime"][:10],
        }
    except:
        return None

def get_premarket_last(symbol: str, api_key: str):
    """
    Obtiene la última vela del pre-market (<= 09:29 NY)
    """
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key)
    if data.get("status") != "ok" or "values" not in data:
        return None
    vals = data["values"]
    if not vals:
        return None

    today_str = ny_today().isoformat()
    premarket_last = None

    # values vienen en orden descendente (más reciente primero)
    for v in vals:
        dt = v.get("datetime", "")
        if len(dt) < 19:
            continue
        d = dt[:10]
        t = dt[11:19]
        if d == today_str and t <= PREMARKET_LAST:
            premarket_last = v
            break

    if not premarket_last:
        return None

    try:
        return {
            "Open": float(premarket_last["open"]),
            "Close": float(premarket_last["close"]),
            "Time": premarket_last["datetime"]
        }
    except:
        return None

def detect_kicker(prev: dict, premarket: dict) -> str | None:
    """
    Kicker profesional:
    - Bullish: gap alcista >= 0.5% y vela verde en pre-market
    - Bearish: gap bajista >= 0.5% y vela roja en pre-market
    """
    if not prev or not premarket:
        return None

    prev_close = prev["Close"]
    pre_open = premarket["Open"]
    pre_close = premarket["Close"]

    # cálculo de gaps
    gap_up = (pre_open - prev_close) / prev_close
    gap_down = (prev_close - pre_open) / prev_close

    if gap_up >= 0.005 and pre_close > pre_open:
        return "bullish"
    elif gap_down >= 0.005 and pre_close < pre_open:
        return "bearish"
    return None

def main():
    ensure_dirs()

    API_KEY = os.getenv("TWELVEDATA_API_KEY")
    if not API_KEY:
        print("⚠️ Falta TWELVEDATA_API_KEY en variables de entorno.")
        return

    now_local = datetime.now(tz.gettz(LOCAL_TZ))
    today_str_local = now_local.date().isoformat()

    universe = load_universe(UNIVERSE_CSV)
    tickers = universe["ticker_td"].tolist()

    bullish_list, bearish_list = [], []
    checked = 0
    too_early = 0
    errors = 0

    for t in tickers:
        try:
            prev = get_prev_daily(t, API_KEY)
            if not prev:
                continue

            premarket = get_premarket_last(t, API_KEY)
            if not premarket:
                too_early += 1
                continue

            sig = detect_kicker(prev, premarket)
            if sig == "bullish":
                bullish_list.append(t)
            elif sig == "bearish":
                bearish_list.append(t)
            checked += 1
        except Exception as e:
            errors += 1
            print(f"[{t}] error: {e}")

    out = {
        "date": today_str_local,
        "bullish": bullish_list,
        "bearish": bearish_list,
        "meta": {
            "provider": "twelvedata",
            "ny_date": ny_today().isoformat(),
            "universe_size": len(tickers),
            "checked_with_0929": checked,
            "skipped_too_early": too_early,
            "errors": errors,
            "note": (
                "Detección con cierre del día anterior y última vela del pre-market (≤09:29 NY). "
                "Requiere datos de pre-market disponibles."
            )
        }
    }

    out_path = os.path.join(SAVE_DIR, f"{today_str_local}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Guardado: {out_path}")
    print(f"Vela Kicker — {today_str_local}")
    print(f"Alcistas: {len(bullish_list)}  |  Bajistas: {len(bearish_list)}")
    print(f"Tickers evaluados (con pre-market disponible): {checked} / {len(tickers)}  |  Too early: {too_early}")

if __name__ == "__main__":
    main()
