# detector.py — versión Polygon (endpoint agrupado), orientada a apertura
import os, json, time
from datetime import datetime, timedelta, date
from dateutil import tz
import pandas as pd
import requests

# ===================== CONFIG =====================
UNIVERSE_CSV = "universe.csv"
SAVE_DIR = "results"

# Zona horaria de referencia para fecha "hoy" (Lima, por el uso de tu padre)
LOCAL_TZ = "America/Lima"

# Ejecutamos a las 08:40 Lima (9:40 NY) para dar tiempo a que Polygon publique el 'open'
TARGET_HOUR = 8
TARGET_MINUTE = 40

# Filtro de volumen desactivado (no inventamos medias sin pedir más datos)
CHECK_VOLUME = False

# Pausa corta para no golpear la API (plan free)
REQUEST_SLEEP = 0.2
# ===================================================


def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)


def load_universe(path: str) -> pd.DataFrame:
    """
    Carga tickers del S&P500 desde universe.csv.
    Normaliza a formato Polygon (reemplaza '-' por '.') p.ej. BRK-B -> BRK.B
    """
    df = pd.read_csv(path)
    df = df.dropna(subset=["ticker"])
    df["ticker_norm"] = df["ticker"].astype(str).str.strip().str.replace("-", ".", regex=False)
    return df


def ny_today() -> date:
    """Fecha de hoy según NY (alineado con sesión US)."""
    now_ny = datetime.now(tz.gettz("America/New_York"))
    return now_ny.date()


def fetch_grouped_for(day: date, api_key: str) -> pd.DataFrame:
    """
    Llama a /v2/aggs/grouped/locale/us/market/stocks/{date}
    Devuelve DataFrame con columnas: T,o,h,l,c,v y Date.
    """
    date_str = day.isoformat()
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=true&apiKey={api_key}"
    r = requests.get(url, timeout=20)
    data = r.json()
    if "results" not in data:
        # Puede no haber datos si es fin de semana/feriado o muy temprano
        return pd.DataFrame()
    df = pd.DataFrame(data["results"])
    if df.empty:
        return df
    # Renombrar a OHLCV estándar
    rename = {"T": "Ticker", "o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume", "t": "Timestamp"}
    df = df.rename(columns=rename)
    # Polygon entrega Ticker en formato como BRK.B (ya es el que queremos)
    # Fecha del agregado
    df["Date"] = pd.to_datetime(df["Timestamp"], unit="ms").dt.date
    time.sleep(REQUEST_SLEEP)
    return df[["Ticker", "Open", "High", "Low", "Close", "Volume", "Date"]]


def find_prev_trading_day(reference: date, api_key: str, max_lookback: int = 7) -> date | None:
    """
    Busca el último día con datos (hábil de mercado) hacia atrás.
    No depende de calendarios locales: intenta hasta 7 días hábiles atrás.
    """
    d = reference - timedelta(days=1)
    for _ in range(max_lookback):
        df = fetch_grouped_for(d, api_key)
        if not df.empty:
            return d
        d -= timedelta(days=1)
    return None


def detect_kicker(prev_row: pd.Series, today_row: pd.Series) -> str | None:
    """
    Regla operativa de Kicker (sin volumen):
    - Alcista: ayer cuerpo bajista (Cprev < Oprev), gap hoy: Open_today > High_prev, y C_today > O_today
    - Bajista: ayer cuerpo alcista (Cprev > Oprev), gap hoy: Open_today < Low_prev, y C_today < O_today
    """
    Oprev, Hprev, Lprev, Cprev = prev_row["Open"], prev_row["High"], prev_row["Low"], prev_row["Close"]
    Ot, Ht, Lt, Ct = today_row["Open"], today_row["High"], today_row["Low"], today_row["Close"]

    bullish = (Cprev < Oprev) and (Ot > Hprev) and (Ct > Ot)
    bearish = (Cprev > Oprev) and (Ot < Lprev) and (Ct < Ot)

    if bullish:
        return "bullish"
    if bearish:
        return "bearish"
    return None


def main():
    ensure_dirs()

    # API key de Polygon desde variable de entorno (pasada por Actions)
    API_KEY = os.getenv("POLYGON_API_KEY")
    if not API_KEY:
        print("⚠️ Falta POLYGON_API_KEY en variables de entorno.")
        return

    # Fecha de hoy (NY) y verificación de hora local (opcional)
    today_local = datetime.now(tz.gettz(LOCAL_TZ))
    today_str = today_local.date().isoformat()

    # Cargamos universo y normalizamos al formato Polygon (BRK-B -> BRK.B)
    universe = load_universe(UNIVERSE_CSV)
    tickers_set = set(universe["ticker_norm"].tolist())

    # 1) Descargamos el agregado "HOY" (debe existir tras apertura + ~10 min)
    today_ny = ny_today()
    df_today = fetch_grouped_for(today_ny, API_KEY)

    # Si aún no hay datos de hoy (muy temprano o feriado), salimos con JSON vacío
    if df_today.empty:
        out = {
            "date": today_str,
            "bullish": [],
            "bearish": [],
            "meta": {
                "provider": "polygon",
                "today": today_ny.isoformat(),
                "prev": None,
                "note": "Sin datos de hoy: puede ser demasiado temprano o feriado en US."
            }
        }
        out_path = os.path.join(SAVE_DIR, f"{today_str}.json")
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print(f"Guardado: {out_path}")
        print("Sin datos de hoy todavía. Revisa la hora o el calendario de mercado.")
        return

    # 2) Buscamos el día hábil anterior con datos
    prev_day = find_prev_trading_day(today_ny, API_KEY)
    if prev_day is None:
        out = {
            "date": today_str,
            "bullish": [],
            "bearish": [],
            "meta": {
                "provider": "polygon",
                "today": today_ny.isoformat(),
                "prev": None,
                "note": "No se encontró día hábil previo con datos (últimos 7 días)."
            }
        }
        out_path = os.path.join(SAVE_DIR, f"{today_str}.json")
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print(f"Guardado: {out_path}")
        return

    df_prev = fetch_grouped_for(prev_day, API_KEY)
    if df_prev.empty:
        # Caso raro: find_prev_trading_day dijo que había datos, pero aquí no
        out = {
            "date": today_str,
            "bullish": [],
            "bearish": [],
            "meta": {
                "provider": "polygon",
                "today": today_ny.isoformat(),
                "prev": prev_day.isoformat(),
                "note": "No se pudieron cargar datos del día previo."
            }
        }
        out_path = os.path.join(SAVE_DIR, f"{today_str}.json")
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print(f"Guardado: {out_path}")
        return

    # 3) Filtramos solo tickers del universo
    # Polygon devuelve miles de símbolos US. Tomamos intersección con nuestro universo.
    df_today = df_today[df_today["Ticker"].isin(tickers_set)].copy()
    df_prev = df_prev[df_prev["Ticker"].isin(tickers_set)].copy()

    # Creamos diccionarios para lookup rápido por ticker
    today_map = {row["Ticker"]: row for _, row in df_today.iterrows()}
    prev_map = {row["Ticker"]: row for _, row in df_prev.iterrows()}

    bullish_list, bearish_list = [], []

    # 4) Evaluamos regla de Kicker para cada ticker presente en ambos días
    common = sorted(set(today_map.keys()) & set(prev_map.keys()))
    for sym in common:
        sig = detect_kicker(prev_map[sym], today_map[sym])
        if sig == "bullish":
            bullish_list.append(sym)
        elif sig == "bearish":
            bearish_list.append(sym)

    # 5) Guardamos salida
    out = {
        "date": today_str,
        "bullish": bullish_list,
        "bearish": bearish_list,
        "meta": {
            "provider": "polygon",
            "today": today_ny.isoformat(),
            "prev": prev_day.isoformat(),
            "universe_size": len(tickers_set),
            "checked": len(common),
            "note": "Detección en apertura (datos agrupados por día desde Polygon)."
        }
    }
    out_path = os.path.join(SAVE_DIR, f"{today_str}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Guardado: {out_path}")
    print(f"Vela Kicker — {today_str}")
    print(f"Alcistas: {len(bullish_list)}")
    print(f"Bajistas: {len(bearish_list)}")
    if bullish_list:
        print("Alcistas:", ", ".join(bullish_list[:20]))
    if bearish_list:
        print("Bajistas:", ", ".join(bearish_list[:20]))


if __name__ == "__main__":
    main()
