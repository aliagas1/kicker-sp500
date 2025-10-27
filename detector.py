# detector.py
import os, json, requests
from datetime import datetime
from dateutil import tz
import pandas as pd
import yfinance as yf

# ===== CONFIG =====
UNIVERSE_CSV = "universe.csv"
SAVE_DIR = "results"

# Zona horaria Perú (UTC-5)
TARGET_TZ = "America/Lima"
TARGET_HOUR = 8
TARGET_MINUTE = 31
FORCE_RUN = True   # En Actions ejecutamos sin validar la hora exacta

# Volumen (opcional)
CHECK_VOLUME = True
VOL_WINDOW = 20
VOLUME_MULTIPLIER = 1.2

# --- Telegram (opcional) ---
TELEGRAM_ENABLED = False
TELEGRAM_BOT_TOKEN = "TU_TOKEN"
TELEGRAM_CHAT_ID = "TU_CHAT_ID"
# ============================

def send_telegram(msg):
    if not TELEGRAM_ENABLED: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except Exception as e:
        print("Telegram error:", e)

def ensure_dirs():
    os.makedirs(SAVE_DIR, exist_ok=True)

def load_universe(path):
    df = pd.read_csv(path)
    return [t.strip() for t in df["ticker"].dropna().tolist()]

def fetch_history(ticker):
    """
    Descarga las últimas 30 velas diarias del ticker desde Finnhub.
    Usa la API key guardada en los secretos de GitHub Actions.
    """
    import requests
    import pandas as pd
    import os
    import time

    # Obtiene la API key del secreto del repositorio
    API_KEY = os.getenv("FINNHUB_API_KEY")

    if not API_KEY:
        print("⚠️ No se encontró FINNHUB_API_KEY en variables de entorno.")
        return pd.DataFrame()

    url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=D&count=30&token={API_KEY}"
    
    try:
        r = requests.get(url)
        data = r.json()

        # Si Finnhub devuelve 's': 'no_data', significa que ese ticker no tiene información
        if data.get("s") != "ok":
            print(f"[{ticker}] sin datos válidos o no cotiza en Finnhub")
            return pd.DataFrame()

        df = pd.DataFrame({
            "Open": data["o"],
            "High": data["h"],
            "Low": data["l"],
            "Close": data["c"],
            "Volume": data["v"]
        }, index=pd.to_datetime(data["t"], unit="s"))

        # Pequeña pausa para no exceder el límite gratuito (60 consultas/minuto)
        time.sleep(0.2)

        return df

    except Exception as e:
        print(f"[{ticker}] error al obtener datos: {e}")
        return pd.DataFrame()

def detect_kicker(df):
    if len(df) < VOL_WINDOW + 2: return None
    df = df.dropna()
    prev, today = df.iloc[-2], df.iloc[-1]
    vol_ok = True
    if CHECK_VOLUME:
        vol_ma = df["Volume"].iloc[-(VOL_WINDOW+1):-1].mean()
        vol_ok = today["Volume"] >= VOLUME_MULTIPLIER * vol_ma if vol_ma > 0 else True
    bullish = (prev["Close"] < prev["Open"]) and (today["Open"] > prev["High"]) and (today["Close"] > today["Open"]) and vol_ok
    bearish = (prev["Close"] > prev["Open"]) and (today["Open"] < prev["Low"]) and (today["Close"] < today["Open"]) and vol_ok
    if bullish: return "bullish"
    if bearish: return "bearish"
    return None

def main():
    ensure_dirs()
    now = datetime.now(tz.gettz(TARGET_TZ))
    tickers = load_universe(UNIVERSE_CSV)
    results = {"date": now.date().isoformat(), "bullish": [], "bearish": []}

    for t in tickers:
        try:
            df = fetch_history(t)
            if df.empty: continue
            signal = detect_kicker(df[["Open","High","Low","Close","Volume"]])
            if signal == "bullish": results["bullish"].append(t)
            elif signal == "bearish": results["bearish"].append(t)
        except Exception as e:
            print(f"{t} error: {e}")

    out = os.path.join(SAVE_DIR, f"{results['date']}.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    summary = f"Vela Kicker — {results['date']}\nAlcistas: {len(results['bullish'])}\nBajistas: {len(results['bearish'])}"
    if results["bullish"]: summary += "\nAlcistas: " + ", ".join(results["bullish"][:10])
    if results["bearish"]: summary += "\nBajistas: " + ", ".join(results["bearish"][:10])
    print(summary)
    send_telegram(summary)

if __name__ == "__main__":
    main()
