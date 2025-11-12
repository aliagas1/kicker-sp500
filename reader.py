# reader.py — Lectura robusta por FECHA NY
# Uso:
#   python reader.py
# Devuelve por stdout un JSON con status: ok|pending|error

import os, json
from datetime import datetime
from dateutil import tz

RESULTS_DIR = "results"
NY_TZ = tz.gettz("America/New_York")

def now_ny():
    return datetime.now(NY_TZ)

def read_results(results_dir=RESULTS_DIR):
    now = now_ny()
    ny_date = now.date().isoformat()
    path = os.path.join(results_dir, f"{ny_date}.json")

    hhmmss = now.strftime("%H:%M:%S")
    cutoff = "09:34:00"  # margen post apertura para que exista la vela 09:29 y el job haya corrido

    if not os.path.exists(path):
        if hhmmss < cutoff:
            return {
                "date": ny_date,
                "status": "pending",
                "message": "Resultados no disponibles aún. Se publican tras 09:34 NY."
            }
        else:
            return {
                "date": ny_date,
                "status": "error",
                "message": "Archivo de resultados no encontrado para hoy (NY). Verifica el workflow del repo."
            }

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {
            "date": ny_date,
            "status": "error",
            "message": f"JSON malformado: {e}"
        }

    data["status"] = "ok"
    # saneo mínimo de counts por si faltaran
    counts = data.get("counts", {})
    data["counts"] = {
        "bullish": counts.get("bullish", len(data.get("bullish", []))),
        "bearish": counts.get("bearish", len(data.get("bearish", []))),
        "universe": counts.get("universe", data.get("meta", {}).get("universe_size", 0)),
        "checked": counts.get("checked", data.get("meta", {}).get("checked", 0)),
        "skipped_too_early": counts.get("skipped_too_early", data.get("meta", {}).get("skipped_too_early", 0)),
        "errors": counts.get("errors", data.get("meta", {}).get("errors", 0))
    }
    return data

if __name__ == "__main__":
    result = read_results()
    print(json.dumps(result, ensure_ascii=False, indent=2))
