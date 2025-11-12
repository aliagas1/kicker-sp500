# detector.py — Twelve Data (detección en pre-market profesional)
# Guardará resultados por FECHA DE NUEVA YORK para evitar líos de husos.
import os, json, time
from datetime import datetime, timedelta, date
from dateutil import tz
import pandas as pd
import requests

# ===================== CONFIG =====================
UNIVERSE_CSV = "universe.csv"
SAVE_DIR = "results"

LOCAL_TZ = "America/Lima"            # Solo para logs; el archivo se nombra por fecha NY
NY_TZ    = "America/New_York"        # Mercado US
OPEN_TIME = "09:30:00"               # Apertura oficial NYSE/NASDAQ
PREMARKET_LAST = "09:29:00"          # Última vela de pre-market NY

REQUEST_SLEEP = 0.1                  # Pausa suave entre requests
INTRADAY_INTERVAL = "1min"           # Granularidad para hoy
INTRADAY_OUTPUTSIZE = 1000           # Velas recientes (~16 h)

MAX_RETRIES = 3                      # Reintentos por fallos transitorios de red/API
RETRY_BACKOFF_SEC = 1.5              # Backoff exponencial
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

def _safe_get(url: str, timeout: int = 20):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r
            # Si no es 200, espera y reintenta
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        # backoff
        time.sleep(RETRY_BACKOFF_SEC ** attempt)
    raise last_err if last_err else RuntimeError("Unknown HTTP error")

def td_time_series(symbol: str, interval: str, outputsize: int, api_key: str):
    url = (
        "https://api.twelvedata.com/time_series"
        f"?symbol={symbol}&interval={interval}&outputsize={outputsize}&timezone={NY_TZ.replace('/', '%2F')}"
        f"&apikey={api_key}"
    )
    r = _safe_get(url, timeout=20)
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
        # Queremos el ÚLTIMO día completo anterior al HOY de NY
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
    Obtiene la última vela del pre-market del DÍA HOY NY.
    Prioriza EXACTAMENTE 09:29:00 NY. Si no existe, toma la mayor t <= 09:29:00
    y marca TooEarly=True para NO clasificar ese ticker hoy.
    """
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key)
    if data.get("status") != "ok" or "values" not in data:
        return None
    vals = data["values"]
    if not vals:
        return None

    today_str = ny_today().isoformat()
    target = None
    best_t = "00:00:00"
    exact = False

    # values suelen venir en orden descendente (más reciente primero)
    for v in vals:
        dt = v.get("datetime", "")
        if len(dt) < 19:
            continue
        d = dt[:10]
        t = dt[11:19]
        if d == today_str and t <= PREMARKET_LAST:
            if t == PREMARKET_LAST:
                target = v
                exact = True
                break
            if t > best_t:
                best_t = t
                target = v

    if not target:
        return None

    try:
        out = {
            "Open": float(target["open"]),
            "Close": float(target["close"]),
            "Time": target["datetime"],
            "TooEarly": (not exact)
        }
        return out
    except:
        return None

def detect_kicker(prev: dict, premarket: dict) -> str | None:
    """
    Kicker profesional (definición mínima, manteniendo tu lógica original):
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

    # FECHA DE REFERENCIA: SIEMPRE NY
    ny_date_str = ny_today().isoformat()

    # Logs locales opcionales
    now_local = datetime.now(tz.gettz(LOCAL_TZ))
    today_str_local = now_local.date().isoformat()

    universe = load_universe(UNIVERSE_CSV)
    tickers = universe["ticker_td"].tolist()

    bullish_list, bearish_list = [], []
    checked = 0
    too_early = 0
    errors = 0
    diagnostics = []

    for t in tickers:
        try:
            prev = get_prev_daily(t, API_KEY)
            if not prev:
                # sin daily previo útil, no se puede clasificar
                diagnostics.append({"ticker": t, "signal": None, "reason": "no_prev_daily"})
                continue

            premarket = get_premarket_last(t, API_KEY)
            if not premarket:
                too_early += 1
                diagnostics.append({"ticker": t, "signal": None, "reason": "no_premarket_found"})
                continue

            if premarket.get("TooEarly"):
                too_early += 1
                diagnostics.append({
                    "ticker": t, "signal": None, "reason": "premarket_not_exact_0929",
                    "t_pre": premarket.get("Time")
                })
                continue

            sig = detect_kicker(prev, premarket)
            if sig == "bullish":
                bullish_list.append(t)
            elif sig == "bearish":
                bearish_list.append(t)
            checked += 1

            # diag por ticker
            diagnostics.append({
                "ticker": t,
                "signal": sig,
                "prev_close": prev["Close"],
                "pre_open": premarket["Open"],
                "pre_close": premarket["Close"],
                "t_pre": premarket.get("Time")
            })
        except Exception as e:
            errors += 1
            diagnostics.append({"ticker": t, "signal": None, "reason": f"error:{e}"})
            print(f"[{t}] error: {e}")

    out = {
        "date": ny_date_str,                    # clave: NOMBRE DE ARCHIVO Y FECHA = NY
        "bullish": bullish_list,
        "bearish": bearish_list,
        "meta": {
            "provider": "twelvedata",
            "ny_date": ny_date_str,
            "universe_size": len(tickers),
            "checked": checked,
            "skipped_too_early": too_early,
            "errors": errors,
            "note": (
                "Detección con cierre del día anterior y última vela del pre-market (09:29:00 NY). "
                "Se omiten tickers si la vela 09:29 exacta no está disponible."
            ),
            "local_log_date": today_str_local,
            "local_tz": LOCAL_TZ
        }
    }

    out["counts"] = {
        "bullish": len(bullish_list),
        "bearish": len(bearish_list),
        "universe": len(tickers),
        "checked": checked,
        "skipped_too_early": too_early,
        "errors": errors
    }

    # Guardado principal por FECHA NY
    out_path = os.path.join(SAVE_DIR, f"{ny_date_str}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # Diagnóstico opcional por día (útil para auditoría)
    try:
        diag_path = os.path.join(SAVE_DIR, f"diagnostics-{ny_date_str}.jsonl")
        with open(diag_path, "w", encoding="utf-8") as fd:
            for row in diagnostics:
                fd.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"⚠️ No se pudo escribir diagnostics: {e}")

    print(f"Guardado: {out_path}")
    print(f"Vela Kicker — NY Date {ny_date_str}")
    print(f"Alcistas: {len(bullish_list)}  |  Bajistas: {len(bearish_list)}")
    print(f"Tickers evaluados (con 09:29 exacto): {checked} / {len(tickers)}  |  Too early/No 09:29: {too_early}  |  Errors: {errors}")

if __name__ == "__main__":
    main()
