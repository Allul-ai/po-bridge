# -*- coding: utf-8 -*-
"""
PO Bridge v4 — Connexion WebSocket directe (sans librairie externe)
"""

import os
import time
import threading
import json
import re
import websocket
import ssl
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
WS = None
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

def on_message(ws, message):
    """Traite les messages WebSocket"""
    global CACHE
    try:
        if message.startswith('42["candles"'):
            # Format: 42["candles", [asset, tf, [[time, open, close, high, low], ...]]]
            data = json.loads(message[2:])
            if len(data) >= 2 and len(data[1]) >= 3:
                asset = data[1][0]
                tf = data[1][1]
                candles = data[1][2]
                
                if candles and len(candles) > 0:
                    o = [float(c[1]) for c in candles[-NB_CANDLES:]]
                    h = [float(c[3]) for c in candles[-NB_CANDLES:]]
                    l = [float(c[4]) for c in candles[-NB_CANDLES:]]
                    c = [float(c[2]) for c in candles[-NB_CANDLES:]]
                    
                    with lock:
                        CACHE[(asset, tf)] = {
                            "o": o, "h": h, "l": l, "c": c,
                            "updated": time.time(),
                            "error": None
                        }
        elif message.startswith('42["auth"'):
            data = json.loads(message[2:])
            if len(data) >= 2 and data[1].get("status") == 1:
                CONNECTED["ok"] = True
                CONNECTED["msg"] = f"Connecté ({'DÉMO' if CONNECTED['is_demo'] else 'RÉEL'})"
                print(f">> ✅ Authentifié avec succès")
    except Exception as e:
        print(f">> ⚠️ Erreur traitement message: {e}")

def on_error(ws, error):
    print(f">> ❌ Erreur WebSocket: {error}")
    CONNECTED["ok"] = False
    CONNECTED["msg"] = f"Erreur: {str(error)[:50]}"

def on_close(ws, close_status_code, close_msg):
    print(f">> 🔌 WebSocket fermé")
    CONNECTED["ok"] = False
    CONNECTED["msg"] = "Déconnecté"

def on_open(ws):
    print(f">> 🔌 WebSocket ouvert, envoi de l'authentification...")
    ws.send(SSID)
    print(f">> 📤 Trame envoyée")

def connect_websocket():
    """Établit la connexion WebSocket"""
    global WS
    if not SSID:
        CONNECTED["msg"] = "PO_SSID manquant"
        return False
    
    detect_uid()
    detect_demo()
    
    print(f">> Connexion en mode {'DÉMO' if CONNECTED['is_demo'] else 'RÉEL'} (UID: {CONNECTED['uid']})")
    
    ws_url = "wss://ws.pocketoption.com/socket.io/?EIO=4&transport=websocket"
    
    WS = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Démarrer le thread WebSocket
    wst = threading.Thread(target=WS.run_forever, kwargs={
        'sslopt': {"cert_reqs": ssl.CERT_NONE}
    })
    wst.daemon = True
    wst.start()
    
    # Attendre la connexion
    time.sleep(3)
    return CONNECTED["ok"]

def fetch_loop():
    """Boucle de récupération et de requête"""
    connect_websocket()
    
    while True:
        if not CONNECTED["ok"]:
            print(">> Reconnexion en cours...")
            time.sleep(30)
            connect_websocket()
            continue
        
        # Demander des bougies pour chaque actif
        for asset in ASSETS:
            for tf in TIMEFRAMES:
                if WS and CONNECTED["ok"]:
                    # Requête de bougies
                    msg = f'42["candles",["{asset}",{tf}]]'
                    try:
                        WS.send(msg)
                    except Exception as e:
                        CONNECTED["ok"] = False
                        CONNECTED["msg"] = f"Erreur envoi: {str(e)[:30]}"
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
    print(f">> PO Bridge v4 sur le port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)