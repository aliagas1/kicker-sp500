# detector.py — Twelve Data (detección en apertura)
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

REQUEST_SLEEP = 0.1                  # pausa suave entre requests
INTRADAY_INTERVAL = "1min"           # granularidad para hoy
INTRADAY_OUTPUTSIZE = 500             # velas recientes de 1 min (~40 min)

# ===================================================

def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_universe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.dropna(subset=["ticker"])
    # Twelve Data usa BRK.B, BF.B, etc.
    df["ticker_td"] = df["ticker"].astype(str).str.strip().str.replace("-", ".", regex=False)
    return df

def ny_today() -> date:
    return datetime.now(tz.gettz(NY_TZ)).date()

def td_time_series(symbol: str, interval: str, outputsize: int, api_key: str):
    """
    Llama a /time_series de Twelve Data y devuelve dict JSON.
    """
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
    """
    Obtiene OHLC del día hábil anterior usando interval=1day, outputsize=3 y toma la fila de 'ayer'.
    """
    data = td_time_series(symbol, "1day", 3, api_key)
    if data.get("status") != "ok" or "values" not in data:
        return None
    vals = data["values"]
    # values vienen en orden descendente (más reciente primero). Buscamos la fecha de 'ayer' NY
    today_ny = ny_today()
    yesterday_ny = today_ny - timedelta(days=1)
    # en fines de semana/feriados, ayer puede no existir; buscamos la más reciente < today
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

def get_today_open_and_latest(symbol: str, api_key: str):
    """
    Obtiene:
      - La vela exacta de las 09:30:00 (apertura oficial).
      - La última vela disponible del día (para comparar Close vs Open).
    Si aun no existe la vela 09:30, devuelve None (demasiado temprano).
    """
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key)
    if data.get("status") != "ok" or "values" not in data:
        return None
    vals = data["values"]
    if not vals:
        return None

    # values vienen en orden descendente por datetime (más reciente primero)
    today_str = ny_today().isoformat()
    open_bar = None
    latest_bar = None

    for idx, v in enumerate(vals):
        dt = v.get("datetime", "")
        if len(dt) < 19:  # "YYYY-MM-DD HH:MM:SS"
            continue
        d = dt[:10]
        t = dt[11:19]
        # última vela disponible del día de hoy
        if d == today_str and latest_bar is None:
            latest_bar = v
        # buscamos exactamente la vela 09:30:00 del día de hoy
        if d == today_str and t == OPEN_TIME:
            open_bar = v

    if open_bar is None:
        # aún no hay vela 09:30 consolidada
        return None

    try:
        open_today = float(open_bar["open"])
        latest_close_today = float(latest_bar["close"]) if latest_bar else None
        return {
            "Open": open_today,
            "Close": latest_close_today,
            "OpenBarTime": open_bar["datetime"],
            "LatestBarTime": latest_bar["datetime"] if latest_bar else None
        }
    except:
        return None

def detect_kicker(prev: dict, today: dict) -> str | None:
    """
    Kicker puro:
    - Bullish: Cprev < Oprev, y Open_today > High_prev, y Close_today > Open_today
    - Bearish: Cprev > Oprev, y Open_today < Low_prev,  y Close_today < Open_today
    """
    Oprev, Hprev, Lprev, Cprev = prev["Open"], prev["High"], prev["Low"], prev["Close"]
    Ot, Ct = today["Open"], today["Close"]

    bullish = (Cprev < Oprev) and (Ot > Hprev) and (Ct is not None and Ct > Ot)
    bearish = (Cprev > Oprev) and (Ot < Lprev) and (Ct is not None and Ct < Ot)

    if bullish:
        return "bullish"
    if bearish:
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
            today_intraday = get_today_open_and_latest(t, API_KEY)
            if not today_intraday:
                too_early += 1
                continue

            sig = detect_kicker(prev, today_intraday)
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
            "checked_with_0930": checked,
            "skipped_too_early": too_early,
            "errors": errors,
            "note": (
                "Detección en apertura con Twelve Data. "
                "Se requiere que la vela 09:30 esté disponible; si no, el ticker se marca como 'too early'."
            )
        }
    }

    out_path = os.path.join(SAVE_DIR, f"{today_str_local}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Guardado: {out_path}")
    print(f"Vela Kicker — {today_str_local}")
    print(f"Alcistas: {len(bullish_list)}  |  Bajistas: {len(bearish_list)}")
    print(f"Tickers evaluados (con 09:30 disponible): {checked} / {len(tickers)}  |  Too early: {too_early}")

if __name__ == "__main__":
    main()
