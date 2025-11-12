# detector.py — Twelve Data (detección en pre-market profesional)
# Guardará resultados por FECHA DE NUEVA YORK y solicita PRE/POST (prepost=true).
import os, json, time
from datetime import datetime, date
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

MAX_RETRIES = 3                      # Reintentos por fallos transitorios
RETRY_BACKOFF_SEC = 1.5              # Backoff exponencial
# ===================================================

def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_universe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.dropna(subset=["ticker"])
    # Normaliza tickers para Twelve Data (ej. BRK.B)
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
            last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_err = e
        time.sleep(RETRY_BACKOFF_SEC ** attempt)
    raise last_err if last_err else RuntimeError("Unknown HTTP error")

def td_time_series(symbol: str, interval: str, outputsize: int, api_key: str, *, prepost: bool = False):
    # IMPORTANT: prepost=true para incluir pre/post-market en intradía
    tz_encoded = NY_TZ.replace("/", "%2F")
    base = "https://api.twelvedata.com/time_series"
    url = (
        f"{base}?symbol={symbol}&interval={interval}&outputsize={outputsize}"
        f"&timezone={tz_encoded}&apikey={api_key}"
    )
    if prepost:
        url += "&prepost=true"
    r = _safe_get(url, timeout=20)
    data = r.json()
    time.sleep(REQUEST_SLEEP)
    return data

def get_prev_daily(symbol: str, api_key: str):
    # Diario NO necesita prepost
    data = td_time_series(symbol, "1day", 3, api_key, prepost=False)
    if data.get("status") != "ok" or "values" not in data:
        # Devuelve None y deja que diagnostics registren el error
        return None, data.get("message") or data.get("code") or "unknown_daily_error"
    vals = data["values"]
    if not vals:
        return None, "empty_daily_values"

    today_ny = ny_today()
    prev_row = None
    for v in vals:
        d = v.get("datetime", "")[:10]
        try:
            d_date = datetime.strptime(d, "%Y-%m-%d").date()
        except:
            continue
        # Tomamos el ÚLTIMO día completo anterior a HOY (NY)
        if d_date < today_ny:
            prev_row = v
            break
    if not prev_row:
        return None, "no_prev_before_today"

    try:
        return ({
            "Open":  float(prev_row["open"]),
            "High":  float(prev_row["high"]),
            "Low":   float(prev_row["low"]),
            "Close": float(prev_row["close"]),
            "Date":  prev_row["datetime"][:10],
        }, None)
    except Exception as e:
        return None, f"daily_parse_error:{e}"

def get_premarket_last(symbol: str, api_key: str):
    """
    Obtiene la última vela del pre-market del DÍA HOY NY.
    Prioriza EXACTAMENTE 09:29:00 NY. Si no existe, toma la mayor t <= 09:29:00
    y marca TooEarly=True para NO clasificar ese ticker hoy.
    Requiere prepost=true para incluir velas de pre-market.
    """
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key, prepost=True)
    if data.get("status") != "ok" or "values" not in data:
        return None, data.get("message") or data.get("code") or "unknown_intraday_error"
    vals = data["values"]
    if not vals:
        return None, "empty_intraday_values"

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
        return None, "no_premarket_found"

    try:
        out = {
            "Open": float(target["open"]),
            "Close": float(target["close"]),
            "Time": target["datetime"],
            "TooEarly": (not exact)
        }
        # Si no es 09:29 exacto, no clasificamos
        if out["TooEarly"]:
            return None, "premarket_not_exact_0929"
        return out, None
    except Exception as e:
        return None, f"intraday_parse_error:{e}"

def detect_kicker(prev: dict, premarket: dict) -> str | None:
    """
    Kicker mínimo:
    - Bullish: gap alcista >= 0.5% y vela premarket verde
    - Bearish: gap bajista >= 0.5% y vela premarket roja
    """
    if not prev or not premarket:
        return None

    prev_close = prev["Close"]
    pre_open = premarket["Open"]
    pre_close = premarket["Close"]

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

    ny_date_str = ny_today().isoformat()

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
            prev, daily_err = get_prev_daily(t, API_KEY)
            if not prev:
                diagnostics.append({"ticker": t, "signal": None, "reason": "no_prev_daily", "api_error": daily_err})
                continue

            premarket, intraday_err = get_premarket_last(t, API_KEY)
            if not premarket:
                if intraday_err == "premarket_not_exact_0929":
                    too_early += 1
                diagnostics.append({"ticker": t, "signal": None, "reason": intraday_err})
                continue

            sig = detect_kicker(prev, premarket)
            if sig == "bullish":
                bullish_list.append(t)
            elif sig == "bearish":
                bearish_list.append(t)
            checked += 1

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
        "date": ny_date_str,
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
                "Se omiten tickers si la vela 09:29 exacta no está disponible. Intradía con prepost=true."
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

    out_path = os.path.join(SAVE_DIR, f"{ny_date_str}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # Diagnostics por día
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
