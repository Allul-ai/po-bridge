# -*- coding: utf-8 -*-
"""
PO Bridge v2 — OHLC complet + multi-timeframe. DEMO UNIQUEMENT.

Variables d'environnement Render :
    PO_SSID  = trame complete 42["auth",{"session":...}]
    PO_TOKEN = mot de passe de ton choix

Start command Render : python po_bridge.py
"""

import os
import time
import threading

# ============ CONFIGURATION ============
SSID = os.environ.get("PO_SSID", "")
TOKEN = os.environ.get("PO_TOKEN", "")
DEMO = True                                      # toujours True
ASSETS = ["EURUSD_otc", "GBPUSD_otc", "AUDCAD_otc", "EURJPY_otc"]
TIMEFRAMES = [60, 300, 900]                      # M1, M5, M15 (secondes)
NB_CANDLES = 80
REFRESH = 5
PORT = int(os.environ.get("PORT", 5000))
# =======================================

from flask import Flask, jsonify, send_file, request, abort

app = Flask(__name__)

# cache par (asset, tf)
CACHE = {(a, tf): {"o": [], "h": [], "l": [], "c": [], "updated": 0, "error": None}
         for a in ASSETS for tf in TIMEFRAMES}
API = None
API_LOCK = threading.Lock()
CONNECTED = {"ok": False, "msg": "demarrage..."}


def check_token():
    if TOKEN and request.args.get("token") != TOKEN:
        abort(403)


def connect_api():
    global API
    try:
        from pocketoptionapi.stable_api import PocketOption
    except ImportError as e:
        CONNECTED["msg"] = f"librairie manquante: {e}"
        return
    if not SSID:
        CONNECTED["msg"] = "variable PO_SSID absente sur Render"
        return
    try:
        API = PocketOption(SSID, DEMO)
        API.connect()
        for _ in range(30):
            try:
                if API.check_connect():
                    CONNECTED["ok"] = True
                    CONNECTED["msg"] = "connecte (demo)"
                    print(">> Connecte a Pocket Option (DEMO)")
                    return
            except Exception:
                pass
            time.sleep(1)
        CONNECTED["msg"] = "connexion impossible : SSID expire ?"
    except Exception as e:
        CONNECTED["msg"] = f"erreur connexion: {e}"


def extract_ohlc(df):
    """Renvoie 4 listes o,h,l,c depuis un DataFrame pandas ou une liste de dicts."""
    try:
        tail = df.tail(NB_CANDLES)
        return ([float(x) for x in tail["open"].tolist()],
                [float(x) for x in tail["high"].tolist()],
                [float(x) for x in tail["low"].tolist()],
                [float(x) for x in tail["close"].tolist()])
    except Exception:
        rows = list(df)[-NB_CANDLES:]
        return ([float(r["open"]) for r in rows],
                [float(r["high"]) for r in rows],
                [float(r["low"]) for r in rows],
                [float(r["close"]) for r in rows])


def fetch_loop():
    connect_api()
    while True:
        if not CONNECTED["ok"]:
            time.sleep(15)
            connect_api()
            continue
        for asset in ASSETS:
            for tf in TIMEFRAMES:
                try:
                    with API_LOCK:
                        df = API.get_candles(asset, tf)
                    if df is not None:
                        o, h, l, c = extract_ohlc(df)
                        if c:
                            CACHE[(asset, tf)] = {"o": o, "h": h, "l": l, "c": c,
                                                  "updated": time.time(), "error": None}
                        else:
                            CACHE[(asset, tf)]["error"] = "aucune donnee"
                except Exception as e:
                    CACHE[(asset, tf)]["error"] = str(e)
                    CONNECTED["ok"] = False
                    CONNECTED["msg"] = f"connexion perdue: {e}"
                    print(f"!! {asset} tf{tf}: {e}")
                    break
                time.sleep(0.5)
            if not CONNECTED["ok"]:
                break
        time.sleep(REFRESH)


@app.route("/")
def index():
    check_token()
    return send_file("signal-otc-auto.html")


@app.route("/health")
def health():
    return jsonify({"connected": CONNECTED["ok"], "status": CONNECTED["msg"]})


@app.route("/candles")
def candles():
    check_token()
    asset = request.args.get("asset", ASSETS[0])
    try:
        tf = int(request.args.get("tf", 60))
    except ValueError:
        tf = 60
    data = CACHE.get((asset, tf))
    if data is None:
        return jsonify({"error": f"combinaison inconnue. actifs: {ASSETS}, tf: {TIMEFRAMES}"}), 404
    return jsonify({
        "asset": asset, "tf": tf,
        "o": data["o"], "h": data["h"], "l": data["l"], "c": data["c"],
        "updated": data["updated"],
        "age_sec": round(time.time() - data["updated"], 1) if data["updated"] else None,
        "error": data["error"] or (None if CONNECTED["ok"] else CONNECTED["msg"]),
        "assets": ASSETS, "timeframes": TIMEFRAMES,
    })


t = threading.Thread(target=fetch_loop, daemon=True)
t.start()

if __name__ == "__main__":
    print(f">> PO Bridge v2 (DEMO) sur le port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
