# -*- coding: utf-8 -*-
"""
PO Bridge v3 — Support réel via pocketoption-api
"""

import os
import time
import threading
import json
import re
from flask import Flask, jsonify, send_file, request, abort

app = Flask(__name__)

# ============ CONFIGURATION ============
SSID = os.environ.get("PO_SSID", "")
TOKEN = os.environ.get("PO_TOKEN", "")
PORT = int(os.environ.get("PORT", 10000))

ASSETS = ["EURUSD_otc", "GBPUSD_otc", "AUDCAD_otc", "EURJPY_otc"]
TIMEFRAMES = [60, 300, 900]
NB_CANDLES = 80
# =======================================

# État de la connexion
CONNECTED = {"ok": False, "msg": "initialisation...", "is_demo": False, "uid": None}
CACHE = {}
API = None
lock = threading.Lock()

def detect_uid():
    """Extrait l'UID de la trame"""
    match = re.search(r'"uid":(\d+)', SSID)
    if match:
        CONNECTED["uid"] = int(match.group(1))
    return CONNECTED["uid"]

def detect_demo():
    """Détecte si c'est un compte démo"""
    match = re.search(r'"isDemo":(\d+)', SSID)
    if match:
        CONNECTED["is_demo"] = int(match.group(1)) == 1
    return CONNECTED["is_demo"]

def connect_api():
    """Connexion via pocketoption-api"""
    global API
    try:
        # Essayer d'importer la nouvelle bibliothèque
        from pocketoption_api import PocketOption
    except ImportError:
        # Fallback vers l'ancienne
        try:
            from pocketoptionapi.stable_api import PocketOption
        except ImportError:
            CONNECTED["msg"] = "Bibliothèque manquante: installe pocketoption-api"
            return
    
    if not SSID:
        CONNECTED["msg"] = "PO_SSID manquant"
        return
    
    detect_uid()
    is_demo = detect_demo()
    
    print(f">> Connexion en mode {'DÉMO' if is_demo else 'RÉEL'} (UID: {CONNECTED['uid']})")
    
    try:
        API = PocketOption(SSID, is_demo)
        API.connect()
        
        # Vérifier la connexion
        for _ in range(30):
            try:
                if API.check_connect():
                    CONNECTED["ok"] = True
                    CONNECTED["msg"] = f"Connecté ({'DÉMO' if is_demo else 'RÉEL'})"
                    print(f">> ✅ Connecté à Pocket Option ({'DÉMO' if is_demo else 'RÉEL'})")
                    return
            except Exception:
                pass
            time.sleep(1)
        
        CONNECTED["msg"] = "Échec connexion: SSID invalide ou expiré"
        print(f">> ❌ {CONNECTED['msg']}")
        
    except Exception as e:
        CONNECTED["msg"] = f"Erreur: {str(e)[:100]}"
        print(f">> ❌ Erreur: {e}")

def fetch_loop():
    """Boucle de récupération des bougies"""
    connect_api()
    
    while True:
        if not CONNECTED["ok"]:
            print(">> Reconnexion en cours...")
            time.sleep(30)
            connect_api()
            continue
        
        for asset in ASSETS:
            for tf in TIMEFRAMES:
                try:
                    with lock:
                        candles = API.get_candles(asset, tf)
                    
                    if candles is not None and len(candles) > 0:
                        # Format attendu: liste de dicts avec 'open', 'high', 'low', 'close'
                        data = list(candles)[-NB_CANDLES:]
                        o = [float(x["open"]) for x in data]
                        h = [float(x["high"]) for x in data]
                        l = [float(x["low"]) for x in data]
                        c = [float(x["close"]) for x in data]
                        
                        if c:
                            CACHE[(asset, tf)] = {
                                "o": o, "h": h, "l": l, "c": c,
                                "updated": time.time(),
                                "error": None
                            }
                except Exception as e:
                    CACHE[(asset, tf)] = {"error": str(e), "updated": 0}
                    CONNECTED["ok"] = False
                    CONNECTED["msg"] = f"Erreur: {str(e)[:50]}"
                    print(f"!! {asset} tf{tf}: {e}")
                    break
                time.sleep(0.3)
            if not CONNECTED["ok"]:
                break
        time.sleep(5)

def check_token():
    if TOKEN and request.args.get("token") != TOKEN:
        abort(403)

@app.route("/")
def index():
    check_token()
    return send_file("signal-otc-auto.html")

@app.route("/health")
def health():
    return jsonify({
        "connected": CONNECTED["ok"],
        "status": CONNECTED["msg"],
        "uid": CONNECTED["uid"],
        "is_demo": CONNECTED["is_demo"],
        "cached": len(CACHE)
    })

@app.route("/candles")
def candles():
    check_token()
    asset = request.args.get("asset", ASSETS[0])
    tf = int(request.args.get("tf", 60))
    
    data = CACHE.get((asset, tf))
    if data is None:
        return jsonify({"error": f"Aucune donnée pour {asset} tf{tf}"}), 404
    
    return jsonify({
        "asset": asset,
        "tf": tf,
        "o": data.get("o", []),
        "h": data.get("h", []),
        "l": data.get("l", []),
        "c": data.get("c", []),
        "updated": data.get("updated", 0),
        "age_sec": round(time.time() - data.get("updated", 0), 1) if data.get("updated") else None,
        "error": data.get("error") or (None if CONNECTED["ok"] else CONNECTED["msg"]),
        "assets": ASSETS,
        "timeframes": TIMEFRAMES,
        "uid": CONNECTED["uid"],
        "is_demo": CONNECTED["is_demo"]
    })

# Démarrer la boucle en arrière-plan
thread = threading.Thread(target=fetch_loop, daemon=True)
thread.start()

if __name__ == "__main__":
    print(f">> PO Bridge v3 (RÉEL) sur le port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)