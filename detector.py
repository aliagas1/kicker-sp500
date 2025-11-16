# detector.py — Twelve Data (Kicker con pre-market y fallback quasi-kicker)
# - Intenta PRE/POST (prepost=true) si USE_PREPOST=true
# - Si el plan no lo permite o falta la 09:29 exacta y FALLBACK_QUASI_KICKER=true,
#   usa la vela 09:30 (primera de sesión regular) como "quasi-kicker".
# - Controla cuotas con MAX_PER_MINUTE / MAX_PER_DAY y recorta universo con UNIVERSE_MAX.

import os, json, time
from collections import deque
from datetime import datetime, date
from dateutil import tz
import pandas as pd
import requests

# ===================== CONFIG BASE =====================
UNIVERSE_CSV = "universe.csv"
SAVE_DIR = "results"

LOCAL_TZ = "America/Lima"      # Para logs
NY_TZ    = "America/New_York"  # Mercado US
OPEN_TIME = "09:30:00"         # Apertura oficial NYSE/NASDAQ
PREMARKET_LAST = "09:29:00"    # Última vela de pre-market NY

REQUEST_SLEEP = 0.05           # Pausa suave entre requests
INTRADAY_INTERVAL = "1min"
INTRADAY_OUTPUTSIZE = 1000

MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.5
# ======================================================

# ===================== ENV VARS =======================
def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() == "true"

USE_PREPOST = _env_bool("USE_PREPOST", "true")
FALLBACK_QUASI_KICKER = _env_bool("FALLBACK_QUASI_KICKER", "false")

UNIVERSE_MAX = os.getenv("UNIVERSE_MAX")  # puede ser None o str con número
UNIVERSE_MAX = int(UNIVERSE_MAX) if UNIVERSE_MAX and UNIVERSE_MAX.isdigit() else None

MAX_PER_MINUTE = int(os.getenv("MAX_PER_MINUTE", "7"))  # Basic ~8/min
MAX_PER_DAY    = int(os.getenv("MAX_PER_DAY", "780"))   # Basic ~800/day
# ======================================================

# ===================== RATE LIMITER ===================
_req_times_minute = deque()  # timestamps últimos 60s
_req_count_day = 0           # contador del día de ejecución (job)

def _rate_limit_block():
    """Bloquea hasta que haya cupo por minuto y por día."""
    global _req_count_day
    # Límite diario
    if _req_count_day >= MAX_PER_DAY:
        raise RuntimeError(f"daily_limit_reached:{_req_count_day}/{MAX_PER_DAY}")

    # Límite por minuto
    now = time.time()
    # limpia timestamps > 60s
    while _req_times_minute and now - _req_times_minute[0] > 60.0:
        _req_times_minute.popleft()

    while len(_req_times_minute) >= MAX_PER_MINUTE:
        # espera hasta que se libere el más antiguo
        sleep_for = 60.0 - (now - _req_times_minute[0])
        if sleep_for > 0:
            time.sleep(min(sleep_for, 1.0))
        now = time.time()
        while _req_times_minute and now - _req_times_minute[0] > 60.0:
            _req_times_minute.popleft()

    # reservar slot
    _req_times_minute.append(time.time())
    _req_count_day += 1
# ======================================================

def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_universe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.dropna(subset=["ticker"])
    df["ticker_td"] = df["ticker"].astype(str).strip().str.replace("-", ".", regex=False)
    if UNIVERSE_MAX:
        df = df.head(UNIVERSE_MAX)
    return df

def ny_today() -> date:
    return datetime.now(tz.gettz(NY_TZ)).date()

def _safe_get(url: str, timeout: int = 20):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _rate_limit_block()
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r
            last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_err = e
        time.sleep(RETRY_BACKOFF_SEC ** attempt)
    raise last_err if last_err else RuntimeError("Unknown HTTP error")

def td_time_series(symbol: str, interval: str, outputsize: int, api_key: str, *, prepost: bool = False):
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

def _is_prepost_plan_error(msg: str | None) -> bool:
    if not msg:
        return False
    # Mensaje típico de Twelve Data para planes sin pre/post
    return "Pre/post data is available on Pro+ plans" in msg or "pre/post" in msg.lower()

def _extract_api_message(payload: dict) -> str:
    return payload.get("message") or payload.get("code") or "unknown_error"

def get_prev_daily(symbol: str, api_key: str):
    data = td_time_series(symbol, "1day", 3, api_key, prepost=False)
    if data.get("status") != "ok" or "values" not in data:
        return None, _extract_api_message(data)
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

def _pick_candle(vals: list, target_time: str) -> dict | None:
    """Busca vela EXACTA 'YYYY-MM-DD HH:MM:SS' en el día NY actual."""
    today_str = ny_today().isoformat()
    for v in vals:
        dt = v.get("datetime", "")
        if len(dt) < 19:
            continue
        if dt[:10] == today_str and dt[11:19] == target_time:
            return v
    return None

def get_premarket_0929(symbol: str, api_key: str):
    """Intenta obtener 09:29:00 con prepost=true (pre-market real)."""
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key, prepost=True)
    if data.get("status") != "ok" or "values" not in data:
        msg = _extract_api_message(data)
        if _is_prepost_plan_error(msg):
            return None, "no_premarket_plan"
        return None, msg

    vals = data["values"]
    if not vals:
        return None, "empty_intraday_values"

    v = _pick_candle(vals, PREMARKET_LAST)
    if not v:
        return None, "no_premarket_0929"
    try:
        return ({
            "Open": float(v["open"]),
            "Close": float(v["close"]),
            "Time": v["datetime"],
            "Type": "premarket_0929"
        }, None)
    except Exception as e:
        return None, f"intraday_parse_error:{e}"

def get_open_0930(symbol: str, api_key: str):
    """Obtiene la vela 09:30:00 sin prepost (primera vela regular)."""
    data = td_time_series(symbol, INTRADAY_INTERVAL, INTRADAY_OUTPUTSIZE, api_key, prepost=False)
    if data.get("status") != "ok" or "values" not in data:
        return None, _extract_api_message(data)

    vals = data["values"]
    if not vals:
        return None, "empty_intraday_values"

    v = _pick_candle(vals, OPEN_TIME)
    if not v:
        return None, "no_open_0930"
    try:
        return ({
            "Open": float(v["open"]),
            "Close": float(v["close"]),
            "Time": v["datetime"],
            "Type": "regular_0930"
        }, None)
    except Exception as e:
        return None, f"intraday_parse_error:{e}"

def detect_kicker(prev: dict, intra_candle: dict) -> str | None:
    """
    Señal mínima:
    - Bullish: gap alcista >= 0.5% y vela verde
    - Bearish: gap bajista >= 0.5% y vela roja
    La vela de referencia puede ser premarket 09:29 (profesional) o 09:30 (quasi-kicker).
    """
    if not prev or not intra_candle:
        return None

    prev_close = prev["Close"]
    o = intra_candle["Open"]
    c = intra_candle["Close"]

    gap_up = (o - prev_close) / prev_close
    gap_down = (prev_close - o) / prev_close

    if gap_up >= 0.005 and c > o:
        return "bullish"
    elif gap_down >= 0.005 and c < o:
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
    too_early_or_missing = 0
    errors = 0
    used_fallback_count = 0
    diagnostics = []

    for t in tickers:
        try:
            # 1) Cierre previo (diario)
            prev, daily_err = get_prev_daily(t, API_KEY)
            if not prev:
                diagnostics.append({"ticker": t, "signal": None, "reason": "no_prev_daily", "api_error": daily_err})
                continue

            intra = None
            intra_err = None
            used_fallback = False

            # 2) Intento pre-market 09:29 si USE_PREPOST
            if USE_PREPOST:
                intra, intra_err = get_premarket_0929(t, API_KEY)

                # 2.a) Si el plan no permite pre/post o no está 09:29 exacto → fallback si está permitido
                if (intra is None and FALLBACK_QUASI_KICKER and (intra_err in ("no_premarket_plan", "no_premarket_0929"))):
                    intra, intra_err = get_open_0930(t, API_KEY)
                    if intra:
                        used_fallback = True
                        used_fallback_count += 1

            # 3) Si no usamos prepost o falló y no hay fallback, intentar quasi-kicker directo si está permitido
            if intra is None and not USE_PREPOST and FALLBACK_QUASI_KICKER:
                intra, intra_err = get_open_0930(t, API_KEY)
                if intra:
                    used_fallback = True
                    used_fallback_count += 1

            if not intra:
                # Si venimos de prepost y no hay 09:29 exacto, lo contamos como too_early/missing
                if intra_err in ("no_premarket_0929", "no_open_0930"):
                    too_early_or_missing += 1
                diagnostics.append({"ticker": t, "signal": None, "reason": intra_err})
                continue

            sig = detect_kicker(prev, intra)
            if sig == "bullish":
                bullish_list.append(t)
            elif sig == "bearish":
                bearish_list.append(t)
            checked += 1

            diagnostics.append({
                "ticker": t,
                "signal": sig,
                "prev_close": prev["Close"],
                "intra_open": intra["Open"],
                "intra_close": intra["Close"],
                "intra_time": intra.get("Time"),
                "intra_type": intra.get("Type"),  # premarket_0929 o regular_0930
                "used_fallback": used_fallback
            })

        except Exception as e:
            errors += 1
            diagnostics.append({"ticker": t, "signal": None, "reason": f"error:{e}"})
            print(f"[{t}] error: {e}")

    out = {
        "date": ny_date_str,
        "bullish": sorted(bullish_list),
        "bearish": sorted(bearish_list),
        "meta": {
            "provider": "twelvedata",
            "ny_date": ny_date_str,
            "universe_size": len(tickers),
            "checked": checked,
            "skipped_missing_intra": too_early_or_missing,
            "errors": errors,
            "note": (
                "Detección con cierre del día anterior y vela intradía: "
                "09:29 NY (pre-market) si disponible; si el plan no lo permite o falta, "
                "se usa fallback 09:30 NY (quasi-kicker) si está habilitado."
            ),
            "used_fallback_count": used_fallback_count,
            "config": {
                "USE_PREPOST": USE_PREPOST,
                "FALLBACK_QUASI_KICKER": FALLBACK_QUASI_KICKER,
                "UNIVERSE_MAX": UNIVERSE_MAX,
                "MAX_PER_MINUTE": MAX_PER_MINUTE,
                "MAX_PER_DAY": MAX_PER_DAY
            },
            "local_log_date": today_str_local,
            "local_tz": LOCAL_TZ
        }
    }

    out["counts"] = {
        "bullish": len(bullish_list),
        "bearish": len(bearish_list),
        "universe": len(tickers),
        "checked": checked,
        "skipped_missing_intra": too_early_or_missing,
        "errors": errors
    }

    out_path = os.path.join(SAVE_DIR, f"{ny_date_str}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # Diagnostics por día (JSON Lines)
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
    print(f"Tickers evaluados: {checked} / {len(tickers)}  |  Missing intra: {too_early_or_missing}  |  Errors: {errors}")
    print(f"Fallback (09:30) usados: {used_fallback_count}")

if __name__ == "__main__":
    main()
