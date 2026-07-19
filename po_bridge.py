# -*- coding: utf-8 -*-
"""
PO Bridge v3 — OHLC complet + multi-timeframe. DEMO ou REEL (PO_DEMO).

Améliorations v3 :
  - tolérance aux erreurs ponctuelles (3 échecs consécutifs avant reconnexion)
  - backoff exponentiel sur la reconnexion (15s → 30s → 60s → 120s max)
  - comparaison de token en temps constant (hmac)
  - logging horodaté au lieu de print
  - validation des données (NaN / valeurs aberrantes filtrées)
  - /health enrichi (âge du cache par actif/timeframe)
  - configuration par variables d'environnement (PO_ASSETS, PO_TFS, PO_REFRESH)

Variables d'environnement Render :
    PO_SSID    = trame complete 42["auth",{"session":...}]
    PO_TOKEN   = mot de passe de ton choix
    PO_DEMO    = 1 (defaut, compte demo) ou 0 (compte reel)
    PO_ASSETS  = optionnel, ex: EURUSD_otc,GBPUSD_otc
    PO_TFS     = optionnel, ex: 60,300,900
    PO_REFRESH = optionnel, secondes entre cycles (defaut 5)

Start command Render : python po_bridge.py
"""

import os
import time
import hmac
import math
import logging
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("po_bridge")

# ============ CONFIGURATION ============
SSID = os.environ.get("PO_SSID", "")
TOKEN = os.environ.get("PO_TOKEN", "")
# PO_DEMO=1 (defaut) -> compte demo | PO_DEMO=0 -> compte reel
DEMO = os.environ.get("PO_DEMO", "1").strip().lower() not in ("0", "false", "non")
MODE_LABEL = "DEMO" if DEMO else "REEL"

_assets_env = os.environ.get("PO_ASSETS", "")
ASSETS = [a.strip() for a in _assets_env.split(",") if a.strip()] or \
         ["EURUSD_otc", "GBPUSD_otc", "AUDCAD_otc", "EURJPY_otc"]

_tfs_env = os.environ.get("PO_TFS", "")
try:
    TIMEFRAMES = [int(t) for t in _tfs_env.split(",") if t.strip()] or [60, 300, 900]
except ValueError:
    TIMEFRAMES = [60, 300, 900]

NB_CANDLES = 80
REFRESH = max(2, int(os.environ.get("PO_REFRESH", 5)))
MAX_CONSECUTIVE_ERRORS = 3      # échecs tolérés avant de déclarer la connexion perdue
PORT = int(os.environ.get("PORT", 5000))
# =======================================

from flask import Flask, jsonify, send_file, request, abort

app = Flask(__name__)

# cache par (asset, tf)
CACHE = {(a, tf): {"o": [], "h": [], "l": [], "c": [], "updated": 0, "error": None}
         for a in ASSETS for tf in TIMEFRAMES}
API = None
API_LOCK = threading.Lock()
CONNECTED = {"ok": False, "msg": "demarrage...", "since": 0, "reconnects": 0}


def check_token():
    """Comparaison en temps constant pour éviter les attaques par timing."""
    if TOKEN and not hmac.compare_digest(request.args.get("token", ""), TOKEN):
        abort(403)


def connect_api():
    global API
    try:
        from pocketoptionapi.stable_api import PocketOption
    except ImportError as e:
        CONNECTED["msg"] = f"librairie manquante: {e}"
        return False
    if not SSID:
        CONNECTED["msg"] = "variable PO_SSID absente sur Render"
        return False
    try:
        API = PocketOption(SSID, DEMO)
        API.connect()
        for _ in range(30):
            try:
                if API.check_connect():
                    CONNECTED["ok"] = True
                    CONNECTED["msg"] = f"connecte ({MODE_LABEL.lower()})"
                    CONNECTED["since"] = time.time()
                    log.info("Connecte a Pocket Option (%s)", MODE_LABEL)
                    return True
            except Exception:
                pass
            time.sleep(1)
        CONNECTED["msg"] = "connexion impossible : SSID expire ?"
    except Exception as e:
        CONNECTED["msg"] = f"erreur connexion: {e}"
        log.warning("Echec connexion: %s", e)
    return False


def _valid(x):
    """True si la valeur est un nombre exploitable."""
    try:
        f = float(x)
        return math.isfinite(f) and f > 0
    except (TypeError, ValueError):
        return False


def extract_ohlc(df):
    """Renvoie 4 listes o,h,l,c depuis un DataFrame pandas ou une liste de dicts.
    Filtre les lignes invalides (NaN, zéros)."""
    try:
        tail = df.tail(NB_CANDLES)
        rows = zip(tail["open"].tolist(), tail["high"].tolist(),
                   tail["low"].tolist(), tail["close"].tolist())
    except Exception:
        raw = list(df)[-NB_CANDLES:]
        rows = ((r["open"], r["high"], r["low"], r["close"]) for r in raw)

    o, h, l, c = [], [], [], []
    for ro, rh, rl, rc in rows:
        if _valid(ro) and _valid(rh) and _valid(rl) and _valid(rc):
            o.append(float(ro)); h.append(float(rh))
            l.append(float(rl)); c.append(float(rc))
    return o, h, l, c


def fetch_one(asset, tf):
    """Récupère les bougies d'un couple (asset, tf). Renvoie True si OK."""
    with API_LOCK:
        try:
            df = API.get_candles(asset, tf, NB_CANDLES)   # certaines versions acceptent count
        except TypeError:
            df = API.get_candles(asset, tf)
    if df is None:
        CACHE[(asset, tf)]["error"] = "aucune donnee"
        return True   # pas une erreur de connexion
    o, h, l, c = extract_ohlc(df)
    if c:
        CACHE[(asset, tf)] = {"o": o, "h": h, "l": l, "c": c,
                              "updated": time.time(), "error": None}
    else:
        CACHE[(asset, tf)]["error"] = "aucune donnee valide"
    return True


def fetch_loop():
    # La librairie PocketOptionAPI utilise asyncio en interne :
    # ce thread doit avoir sa propre boucle d'evenements, sinon
    # "There is no current event loop in thread 'Thread-1'".
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())

    backoff = 15
    errors = 0
    connect_api()
    while True:
        if not CONNECTED["ok"]:
            log.info("Reconnexion dans %ds...", backoff)
            time.sleep(backoff)
            CONNECTED["reconnects"] += 1
            if connect_api():
                backoff = 15
                errors = 0
            else:
                backoff = min(backoff * 2, 120)
            continue

        for asset in ASSETS:
            for tf in TIMEFRAMES:
                try:
                    fetch_one(asset, tf)
                    errors = 0
                except Exception as e:
                    errors += 1
                    CACHE[(asset, tf)]["error"] = str(e)
                    log.warning("%s tf%d: %s (erreur %d/%d)",
                                asset, tf, e, errors, MAX_CONSECUTIVE_ERRORS)
                    if errors >= MAX_CONSECUTIVE_ERRORS:
                        CONNECTED["ok"] = False
                        CONNECTED["msg"] = f"connexion perdue: {e}"
                        break
                time.sleep(0.5)
            if not CONNECTED["ok"]:
                break
        time.sleep(REFRESH)


@app.route("/")
def index():
    check_token()
    return send_file("signal-otc-auto.html")


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/health")
def health():
    ages = {}
    now = time.time()
    for (a, tf), d in CACHE.items():
        ages[f"{a}@{tf}"] = round(now - d["updated"], 1) if d["updated"] else None
    return jsonify({
        "mode": MODE_LABEL,
        "connected": CONNECTED["ok"],
        "status": CONNECTED["msg"],
        "uptime_sec": round(now - CONNECTED["since"], 0) if CONNECTED["since"] else None,
        "reconnects": CONNECTED["reconnects"],
        "cache_age_sec": ages,
    })


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
        return jsonify({"error": f"combinaison inconnue. actifs: {ASSETS}, tf: {TIMEFRAMES}",
                        "assets": ASSETS, "timeframes": TIMEFRAMES}), 404
    return jsonify({
        "mode": MODE_LABEL,
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
    log.info("PO Bridge v3 (%s) sur le port %d — actifs: %s, tfs: %s",
             MODE_LABEL, PORT, ASSETS, TIMEFRAMES)
    app.run(host="0.0.0.0", port=PORT, debug=False)
