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
    TG_BOT_TOKEN = token du bot Telegram (via @BotFather)
    TG_CHAT_ID   = ton chat id Telegram (via @userinfobot)
    TG_TFS       = timeframes alertes, defaut "60" (ex: 60,300)
    TG_MIN_CONF  = confluence minimum pour alerter, defaut 0
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

# --- Alertes Telegram (optionnel) ---
TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT_ID", "")
_tg_tfs = os.environ.get("TG_TFS", "60")
try:
    TG_TFS = [int(t) for t in _tg_tfs.split(",") if t.strip()]
except ValueError:
    TG_TFS = [60]
TG_MIN_CONF = int(os.environ.get("TG_MIN_CONF", 0))
TG_COOLDOWN = int(os.environ.get("TG_COOLDOWN", 600))   # sec avant re-alerte meme direction
TG_ENABLED = bool(TG_TOKEN and TG_CHAT)
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


_LIB_PATCHED = {"done": False}


def _patch_library():
    """Corrige un bug de la librairie : send_websocket_request cree une nouvelle
    boucle asyncio pour envoyer sur un websocket appartenant a une autre boucle.
    Resultat : l'authentification passe (envoyee depuis la bonne boucle) mais
    changeSymbol / getcandles ne partent jamais -> aucune bougie ne revient.
    On capture la boucle du websocket a la connexion, puis on programme les
    envois dessus via run_coroutine_threadsafe."""
    if _LIB_PATCHED["done"]:
        return
    import json as _json
    import asyncio as _asyncio
    from pocketoptionapi.ws.client import WebsocketClient
    from pocketoptionapi.api import PocketOptionAPI
    from pocketoptionapi import global_value as _gv

    _orig_connect = WebsocketClient.connect

    async def _connect_capture(self):
        _gv.main_ws_loop = _asyncio.get_running_loop()
        return await _orig_connect(self)

    WebsocketClient.connect = _connect_capture

    _orig_send = PocketOptionAPI.send_websocket_request

    def _send_fixed(self, name, msg, request_id="", no_force_send=True):
        data = f"42{_json.dumps(msg)}"
        loop = getattr(_gv, "main_ws_loop", None)
        conn = getattr(self.websocket, "websocket", None)
        if loop is not None and conn is not None and loop.is_running():
            fut = _asyncio.run_coroutine_threadsafe(conn.send(data), loop)
            fut.result(timeout=10)
            return
        return _orig_send(self, name, msg, request_id, no_force_send)

    PocketOptionAPI.send_websocket_request = _send_fixed

    # Le serveur PO ne repond pas aux pings natifs du protocole websocket
    # (il utilise le ping socket.io "2"/"3", deja gere par la librairie).
    # Sans cette desactivation, la connexion se ferme toutes les ~40s
    # avec "keepalive ping timeout".
    import pocketoptionapi.ws.client as _wsc
    _orig_ws_connect = _wsc.websockets.connect

    def _connect_sans_ping(*a, **kw):
        kw["ping_interval"] = None
        return _orig_ws_connect(*a, **kw)

    _wsc.websockets.connect = _connect_sans_ping

    _LIB_PATCHED["done"] = True
    log.info("Patch librairie applique (cross-boucle + ping natif desactive)")


def connect_api():
    global API
    try:
        from pocketoptionapi.stable_api import PocketOption
        _patch_library()
    except ImportError as e:
        CONNECTED["msg"] = f"librairie manquante: {e}"
        return False
    if not SSID:
        CONNECTED["msg"] = "variable PO_SSID absente sur Render"
        return False
    try:
        # ATTENTION : cette version de la librairie declare __init__(demo, ssid)
        # dans cet ordre. Les arguments nommes evitent toute inversion,
        # quelle que soit la version installee.
        try:
            API = PocketOption(ssid=SSID, demo=DEMO)
        except TypeError:
            API = PocketOption(SSID, DEMO)
        API.connect()
        for _ in range(30):
            try:
                if API.check_connect():
                    CONNECTED["ok"] = True
                    CONNECTED["msg"] = f"connecte ({MODE_LABEL.lower()})"
                    prev_since = CONNECTED.get("last_ok", 0)
                    CONNECTED["since"] = time.time()
                    CONNECTED["last_ok"] = time.time()
                    log.info("Connecte a Pocket Option (%s)", MODE_LABEL)
                    # n'annoncer que le premier demarrage ou un retour apres
                    # une vraie coupure (>5 min), pas chaque micro-reconnexion
                    if TG_ENABLED and (prev_since == 0 or time.time() - prev_since > 300):
                        send_telegram(f"\u2705 Pont connecte ({MODE_LABEL}) \u2014 alertes actives sur tf {TG_TFS}")
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
    rows = None
    try:
        tail = df.tail(NB_CANDLES)
        rows = list(zip(tail["open"].tolist(), tail["high"].tolist(),
                        tail["low"].tolist(), tail["close"].tolist()))
    except Exception:
        raw = list(df)[-NB_CANDLES:]
        if raw and isinstance(raw[0], dict):
            rows = [(r.get("open"), r.get("high"), r.get("low"), r.get("close"))
                    for r in raw]
        elif raw and isinstance(raw[0], (list, tuple)):
            n = len(raw[0])
            if n >= 5:      # [time, open, high?, low?, close?] ou [time,o,c,h,l] selon versions
                rows = [(r[1], r[2], r[3], r[4]) for r in raw]
            elif n == 4:    # [open, high, low, close]
                rows = [(r[0], r[1], r[2], r[3]) for r in raw]
    if rows is None:
        return [], [], [], []

    o, h, l, c = [], [], [], []
    for ro, rh, rl, rc in rows:
        if _valid(ro) and _valid(rh) and _valid(rl) and _valid(rc):
            o.append(float(ro)); h.append(float(rh))
            l.append(float(rl)); c.append(float(rc))
    return o, h, l, c


_FMT_LOGGED = {"done": False}


def _with_timeout(fn, timeout, label):
    """Execute fn() dans un thread avec limite de temps.
    La librairie contient des boucles d'attente sans limite : sans ce garde-fou,
    un actif qui ne repond pas gele tout le pont."""
    box = {}

    def run():
        try:
            box["result"] = fn()
        except Exception as e:
            box["exc"] = e

    th = threading.Thread(target=run, daemon=True)
    th.start()
    th.join(timeout)
    if th.is_alive():
        raise TimeoutError(f"{label} bloque > {timeout}s")
    if "exc" in box:
        raise box["exc"]
    return box.get("result")


def _fetch_m1_page(asset, end_time):
    """Demande une page (~150 bougies M1) via loadHistoryPeriod.
    Seule la periode 60 recoit une reponse fiable du serveur ; les autres
    timeframes sont construits localement par agregation."""
    api = API.api
    api.history_data = None
    api.getcandles(asset, 60, int(end_time))
    for _ in range(120):                    # attente max 12 s
        if api.history_data is not None:
            break
        time.sleep(0.1)
    return api.history_data


# Reserve M1 par actif : {timestamp: (o, h, l, c)}
# 1300 bougies M1 ~= 21 h -> de quoi construire 80 bougies M15
M1_KEEP = 1300
M1_STORE = {a: {} for a in ASSETS}


def _merge_m1(asset, data):
    st = M1_STORE[asset]
    for r in data or []:
        try:
            t = int(r["time"])
            o, h, l, c = (float(r["open"]), float(r["high"]),
                          float(r["low"]), float(r["close"]))
            if all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                st[t] = (o, h, l, c)
        except (KeyError, TypeError, ValueError):
            continue
    if len(st) > M1_KEEP:
        for t in sorted(st)[:len(st) - M1_KEEP]:
            del st[t]


def _aggregate(asset, tf):
    """Construit les bougies de periode tf a partir de la reserve M1."""
    st = M1_STORE[asset]
    if not st:
        return [], [], [], []
    buckets = {}
    for t in sorted(st):
        b = (t // tf) * tf
        o, h, l, c = st[t]
        if b not in buckets:
            buckets[b] = [o, h, l, c]
        else:
            bk = buckets[b]
            bk[1] = max(bk[1], h)
            bk[2] = min(bk[2], l)
            bk[3] = c
    keys = sorted(buckets)[-NB_CANDLES:]
    o = [buckets[k][0] for k in keys]
    h = [buckets[k][1] for k in keys]
    l = [buckets[k][2] for k in keys]
    c = [buckets[k][3] for k in keys]
    return o, h, l, c


def _server_now():
    try:
        return int(API.get_server_timestamp() or time.time())
    except Exception:
        return int(time.time())


def fetch_asset(asset):
    """Recupere la page M1 recente, complete l'historique si besoin (une page
    ancienne par cycle), puis reconstruit les caches de tous les timeframes."""
    if not API_LOCK.acquire(timeout=40):
        for tf in TIMEFRAMES:
            CACHE[(asset, tf)]["error"] = "verrou occupe"
        return
    try:
        data = _with_timeout(lambda: _fetch_m1_page(asset, _server_now()),
                             25, f"loadHistory({asset},M1)")
        _merge_m1(asset, data)
        st = M1_STORE[asset]
        if st and len(st) < M1_KEEP:        # backfill progressif
            oldest = min(st)
            older = _with_timeout(lambda: _fetch_m1_page(asset, oldest),
                                  25, f"backfill({asset})")
            _merge_m1(asset, older)
    finally:
        API_LOCK.release()

    if not _FMT_LOGGED["done"] and M1_STORE[asset]:
        _FMT_LOGGED["done"] = True
        log.info("Historique M1 recu pour %s (%d bougies en reserve)",
                 asset, len(M1_STORE[asset]))

    for tf in TIMEFRAMES:
        o, h, l, c = _aggregate(asset, tf)
        if c:
            CACHE[(asset, tf)] = {"o": o, "h": h, "l": l, "c": c,
                                  "updated": time.time(), "error": None}
        else:
            CACHE[(asset, tf)]["error"] = "reserve M1 vide"



# ==================== ANALYSE COTE SERVEUR (alertes Telegram) ====================
# Reproduction fidele de la logique du frontend : memes indicateurs, memes seuils,
# bougies cloturees uniquement (anti-repaint).

TF_NAMES = {60: "M1", 300: "M5", 900: "M15"}
TF_EXP = {60: (1, 2), 300: (5, 10), 900: (15, 30)}   # (range, tendance) en minutes


def _ema_series(arr, p):
    k = 2.0 / (p + 1)
    out, e, s = [], None, 0.0
    for i, x in enumerate(arr):
        if i < p - 1:
            s += x; out.append(None); continue
        if i == p - 1:
            s += x; e = s / p; out.append(e); continue
        e = x * k + e * (1 - k); out.append(e)
    return out


def _wilder_series(arr, p):
    out, s, v = [], 0.0, None
    for i, x in enumerate(arr):
        if i < p - 1:
            s += x; out.append(None); continue
        if i == p - 1:
            s += x; v = s / p; out.append(v); continue
        v = (v * (p - 1) + x) / p; out.append(v)
    return out


def _sma_last(arr, p):
    w = arr[-p:]
    return sum(w) / len(w)


def _rsi(c, p=14):
    g = l = 0.0
    for i in range(1, p + 1):
        d = c[i] - c[i - 1]
        if d > 0: g += d
        else: l -= d
    ag, al = g / p, l / p
    for i in range(p + 1, len(c)):
        d = c[i] - c[i - 1]
        ag = (ag * (p - 1) + (d if d > 0 else 0)) / p
        al = (al * (p - 1) + (-d if d < 0 else 0)) / p
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def _stoch_k(h, l, c, p=14):
    hi, lo = max(h[-p:]), min(l[-p:])
    return 50.0 if hi == lo else 100 * (c[-1] - lo) / (hi - lo)


def _cci(h, l, c, p=20):
    tp = [(h[i] + l[i] + c[i]) / 3 for i in range(len(c))]
    w = tp[-p:]
    m = sum(w) / p
    md = sum(abs(x - m) for x in w) / p
    return 0.0 if md == 0 else (tp[-1] - m) / (0.015 * md)


def _williams(h, l, c, p=14):
    hi, lo = max(h[-p:]), min(l[-p:])
    return -50.0 if hi == lo else -100 * (hi - c[-1]) / (hi - lo)


def _macd_hist(c, f=12, s=26, sig=9):
    ef, es = _ema_series(c, f), _ema_series(c, s)
    line = [ef[i] - es[i] for i in range(len(c)) if ef[i] is not None and es[i] is not None]
    sg = _ema_series(line, sig)
    return line[-1] - sg[-1]


def _bollinger(c, p=20, mult=2):
    w = c[-p:]
    m = sum(w) / p
    sd = (sum((x - m) ** 2 for x in w) / p) ** 0.5
    return m - mult * sd, m + mult * sd


def _atr(h, l, c, p=14):
    tr = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
          for i in range(1, len(c))]
    return _wilder_series(tr, p)[-1]


def _adx(h, l, c, p=14):
    tr, pdm, ndm = [], [], []
    for i in range(1, len(c)):
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
        up, dn = h[i] - h[i - 1], l[i - 1] - l[i]
        pdm.append(up if (up > dn and up > 0) else 0.0)
        ndm.append(dn if (dn > up and dn > 0) else 0.0)
    st, sp, sn = _wilder_series(tr, p), _wilder_series(pdm, p), _wilder_series(ndm, p)
    dx = []
    for i in range(len(st)):
        if st[i] is None or not st[i]:
            continue
        dip, din = 100 * sp[i] / st[i], 100 * sn[i] / st[i]
        dx.append(0.0 if (dip + din) == 0 else 100 * abs(dip - din) / (dip + din))
    if not dx or not st[-1]:
        return 0.0, 50.0, 50.0          # marche plat : aucune tendance mesurable
    adx_s = _wilder_series(dx, p)
    return (adx_s[-1] if adx_s and adx_s[-1] is not None else 0.0,
            100 * sp[-1] / st[-1], 100 * sn[-1] / st[-1])


def _sar_up(h, l, step=0.02, mx=0.2):
    up, sar, ep, af = True, l[0], h[0], step
    for i in range(1, len(h)):
        sar = sar + af * (ep - sar)
        if up:
            if l[i] < sar: up, sar, ep, af = False, ep, l[i], step
            elif h[i] > ep: ep, af = h[i], min(mx, af + step)
        else:
            if h[i] > sar: up, sar, ep, af = True, ep, h[i], step
            elif l[i] < ep: ep, af = l[i], min(mx, af + step)
    return up


def _ao(h, l):
    med = [(h[i] + l[i]) / 2 for i in range(len(h))]
    return _sma_last(med, 5) - _sma_last(med, 34)


def analyze_server(o, h, l, c):
    """Renvoie (dir, conf, regime, prix) ou None. Bougies cloturees uniquement."""
    if len(c) > 40:                       # ecarte la bougie en formation
        o, h, l, c = o[:-1], h[:-1], l[:-1], c[:-1]
    if len(c) < 40:
        return None
    # Marche ferme ou fige (paires reelles le week-end) : toutes les bougies
    # identiques -> aucun signal possible, et certains indicateurs planteraient
    if max(c) == min(c) or max(h) == min(l):
        return None
    r = _rsi(c); k = _stoch_k(h, l, c); cc = _cci(h, l, c); wr = _williams(h, l, c)
    mo = c[-1] - c[-11]
    hist = _macd_hist(c)
    blow, bup = _bollinger(c)
    adx_v, dip, din = _adx(h, l, c)
    sar = _sar_up(h, l); awo = _ao(h, l)
    e9 = _ema_series(c, 9)[-1]; e21 = _ema_series(c, 21)[-1]
    at_rel = _atr(h, l, c) / c[-1] * 10000

    trend = adx_v >= 25
    if trend:
        votes = [1 if hist > 0 else -1 if hist < 0 else 0,
                 1 if e9 > e21 else -1,
                 1 if sar else -1,
                 1 if dip > din else -1,
                 1 if awo > 0 else -1 if awo < 0 else 0,
                 1 if mo > 0 else -1 if mo < 0 else 0]
        need = 4
    else:
        votes = [1 if r < 30 else -1 if r > 70 else 0,
                 1 if k < 20 else -1 if k > 80 else 0,
                 1 if cc < -100 else -1 if cc > 100 else 0,
                 1 if wr < -80 else -1 if wr > -20 else 0,
                 1 if c[-1] <= blow else -1 if c[-1] >= bup else 0]
        need = 3

    b, s = votes.count(1), votes.count(-1)
    d = None
    if b >= need and b > s: d, aligned = "buy", b
    elif s >= need and s > b: d, aligned = "sell", s
    if d is None:
        return None
    if at_rel < 1:                       # marche mort
        return None
    if not trend and adx_v >= 20:        # tendance naissante : ne pas contrer
        return None
    conf = min(95, round(aligned / len(votes) * 100))
    return d, conf, ("T" if trend else "R"), c[-1]


# anti-doublon / anti-spam
_SENT = {}


def maybe_alert(asset, tf):
    if not TG_ENABLED or tf not in TG_TFS:
        return
    d = CACHE.get((asset, tf))
    if not d or not d["c"]:
        return
    res = analyze_server(d["o"], d["h"], d["l"], d["c"])
    if res is None:
        return
    direction, conf, regime, price = res
    if conf < TG_MIN_CONF:
        return
    now = time.time()
    prev = _SENT.get((asset, tf))
    # direction differente -> alerte immediate
    # meme direction -> re-alerte seulement apres TG_COOLDOWN
    if prev and prev["dir"] == direction and now - prev["t"] < TG_COOLDOWN:
        return
    _SENT[(asset, tf)] = {"dir": direction, "t": now}

    em = TF_EXP.get(tf, (1, 2))
    exp = em[1] if conf >= 80 else em[0]
    icon = "\U0001F7E2 ACHAT" if direction == "buy" else "\U0001F534 VENTE"
    reg = "TENDANCE" if regime == "T" else "RANGE"
    msg = (f"{icon} {asset} \u00b7 {TF_NAMES.get(tf, tf)}\n"
           f"Confluence {conf}% \u00b7 R\u00e9gime {reg}\n"
           f"Expiration conseill\u00e9e : {exp} min\n"
           f"Prix : {price}")
    send_telegram(msg)


def send_telegram(text):
    import requests
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text},
            timeout=10)
        log.info("Alerte Telegram envoyee")
    except Exception as e:
        log.warning("Echec envoi Telegram: %s", e)

# =================================================================================


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
            try:
                fetch_asset(asset)
                errors = 0
                for tf in TIMEFRAMES:
                    try:
                        maybe_alert(asset, tf)
                    except Exception as e:
                        log.warning("analyse/alerte %s tf%d: %s", asset, tf, e)
            except Exception as e:
                errors += 1
                for tf in TIMEFRAMES:
                    CACHE[(asset, tf)]["error"] = str(e)
                log.warning("%s: %s (erreur %d/%d)",
                            asset, e, errors, MAX_CONSECUTIVE_ERRORS)
                if errors >= MAX_CONSECUTIVE_ERRORS:
                    CONNECTED["ok"] = False
                    CONNECTED["msg"] = f"connexion perdue: {e}"
                    break
            time.sleep(0.5)
        time.sleep(REFRESH)


@app.route("/")
def index():
    check_token()
    return send_file("signal-otc-auto.html")


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/debug")
def debug():
    """Diagnostic : etat de la reserve M1 et test de recuperation en direct."""
    check_token()
    if API is None or not CONNECTED["ok"]:
        return jsonify({"error": "non connecte", "status": CONNECTED["msg"]})
    asset = request.args.get("asset", ASSETS[0])
    out = {"asset": asset,
           "reserve_m1": {a: len(s) for a, s in M1_STORE.items()}}
    try:
        if not API_LOCK.acquire(timeout=30):
            return jsonify({**out, "exception": "verrou occupe"})
        try:
            data = _with_timeout(lambda: _fetch_m1_page(asset, _server_now()),
                                 20, f"debug({asset})")
        finally:
            API_LOCK.release()
        out["page_recue"] = len(data) if data else 0
        if data:
            out["premiere"] = data[0]
            out["derniere"] = data[-1]
    except Exception as e:
        out["exception"] = f"{type(e).__name__}: {e}"
    return jsonify(out)


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
        "telegram": TG_ENABLED,
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
