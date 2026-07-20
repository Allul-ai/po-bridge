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
    PO_TRADE     = 1 (defaut) boutons d'execution actifs, 0 pour desactiver
    PO_TRADE_PCT = % du solde par trade, defaut 2.0
    PO_MAX_TRADES_JOUR = limite quotidienne, defaut 10
    PO_MODE      = auto | manuel (defaut) | off — aussi commutable par /auto /manuel /off sur Telegram
    PO_AUTO_MIN_CONF = confluence minimum en auto, defaut 80
    PO_AUTO_STOP_PERTES = pertes de suite avant suspension de l'auto, defaut 3
    PO_RATTRAPAGE = 1 pour activer le rattrapage a UN niveau (defaut 0)
    PO_RATTRAPAGE_MULT = multiplicateur du rattrapage, defaut 2.25
    PO_RATTRAPAGE_CAP  = plafond de la mise de rattrapage en % du solde, defaut 10
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

# --- Execution en un tap depuis Telegram ---
TRADE_ENABLED = os.environ.get("PO_TRADE", "1").strip().lower() not in ("0", "false", "non")
TRADE_PCT = float(os.environ.get("PO_TRADE_PCT", 2.0))        # % du solde par trade
TRADE_MIN = float(os.environ.get("PO_TRADE_MIN", 1.0))        # mise minimum ($)
TRADE_MAX_JOUR = int(os.environ.get("PO_MAX_TRADES_JOUR", 10))
SIGNAL_TTL = 45                                               # validite du bouton (sec)

# Mode d'execution : auto (le pont trade seul) | manuel (bouton) | off (alertes seules)
_m = os.environ.get("PO_MODE", "manuel").strip().lower()
MODE = {"mode": _m if _m in ("auto", "manuel", "off") else "manuel"}
AUTO_MIN_CONF = int(os.environ.get("PO_AUTO_MIN_CONF", 80))   # confluence min en auto
AUTO_STOP_PERTES = int(os.environ.get("PO_AUTO_STOP_PERTES", 3))  # pertes de suite -> repasse en manuel
_PERTES_SUITE = {"n": 0}

# Rattrapage a UN SEUL niveau (opt-in via PO_RATTRAPAGE=1) :
# apres une perte sur mise de base, le trade suivant est multiplie par
# PO_RATTRAPAGE_MULT (defaut 2.25 = recuperation + gain a payout 80%).
# Quel que soit son resultat, retour a la mise de base ensuite.
# Jamais de niveau 2 : c'est structurel, pas parametrable.
RATTRAPAGE_ON = os.environ.get("PO_RATTRAPAGE", "0").strip().lower() in ("1", "true", "oui")
RATTRAPAGE_MULT = float(os.environ.get("PO_RATTRAPAGE_MULT", 2.25))
RATTRAPAGE_CAP_PCT = float(os.environ.get("PO_RATTRAPAGE_CAP", 10.0))  # % max du solde en rattrapage
_RATTRAPAGE = {"arme": False}
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


def _fetch_m1_page(asset, end_time, ref_price=None):
    """Demande une page (~150 bougies M1) via loadHistoryPeriod.
    Le canal de reception est partage et les reponses peuvent arriver en
    retard : une reponse dont les prix ne collent pas a ref_price (reserve
    de l'actif demande) est une reponse tardive d'un AUTRE actif — on la
    jette et on continue d'attendre la bonne, dans la limite de 15 s."""
    api = API.api
    api.history_data = None
    api.getcandles(asset, 60, int(end_time))
    deadline = time.time() + 15
    while time.time() < deadline:
        data = api.history_data
        if data is None:
            time.sleep(0.1)
            continue
        if ref_price:
            try:
                closes = sorted(float(r["close"]) for r in data if "close" in r)
                med = closes[len(closes) // 2]
                if abs(med / ref_price - 1) > 0.008:   # >0.8% : mauvais actif
                    api.history_data = None            # jeter, attendre la suite
                    continue
            except (KeyError, ValueError, ZeroDivisionError, IndexError, TypeError):
                pass
        return data
    return None


# Reserve M1 par actif : {timestamp: (o, h, l, c)}
# 1300 bougies M1 ~= 21 h -> de quoi construire 80 bougies M15
M1_KEEP = 1300
M1_STORE = {a: {} for a in ASSETS}


def _merge_m1(asset, data):
    """Fusionne une page M1 dans la reserve. Renvoie le nombre de bougies
    reellement nouvelles. Rejette les lots aberrants : le canal de reception
    est partage entre actifs, une reponse tardive d'un autre actif ne doit
    pas contaminer la reserve (ex. prix EURUSD injectes dans GBPUSD)."""
    st = M1_STORE[asset]
    if not data:
        return 0
    # garde anti-contamination : prix du lot coherent avec la reserve
    if st:
        try:
            ref = st[max(st)][3]
            closes = sorted(float(r["close"]) for r in data if "close" in r)
            med = closes[len(closes) // 2]
            if ref > 0 and abs(med / ref - 1) > 0.008:  # >0.8% d'ecart : mauvais actif
                log.warning("%s: lot rejete (mediane %.5f vs reserve %.5f, "
                            "probable reponse d'un autre actif)", asset, med, ref)
                return 0
        except (KeyError, ValueError, ZeroDivisionError, IndexError, TypeError):
            pass
    now = time.time()
    added = 0
    for r in data:
        try:
            t = int(r["time"])
            if t < 1500000000 or t > now + 600:         # timestamp aberrant
                continue
            o, h, l, c = (float(r["open"]), float(r["high"]),
                          float(r["low"]), float(r["close"]))
            if all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                if t not in st:
                    added += 1
                st[t] = (o, h, l, c)
        except (KeyError, TypeError, ValueError):
            continue
    if len(st) > M1_KEEP:
        for t in sorted(st)[:len(st) - M1_KEEP]:
            del st[t]
    return added


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


# horodatage de la derniere bougie NOUVELLE recue, par actif
LAST_NEW = {a: 0.0 for a in ASSETS}


def fetch_asset(asset):
    """Recupere la page M1 recente, complete l'historique si besoin (une page
    ancienne par cycle), puis reconstruit les caches de tous les timeframes."""
    if not API_LOCK.acquire(timeout=40):
        for tf in TIMEFRAMES:
            CACHE[(asset, tf)]["error"] = "verrou occupe"
        return
    try:
        st0 = M1_STORE[asset]
        ref = st0[max(st0)][3] if st0 else None
        data = _with_timeout(lambda: _fetch_m1_page(asset, _server_now(), ref),
                             25, f"loadHistory({asset},M1)")
        if data is None:
            log.warning("%s: pas de reponse valide (15s)", asset)
        if _merge_m1(asset, data):
            LAST_NEW[asset] = time.time()
        st = M1_STORE[asset]
        if st and len(st) < M1_KEEP:        # backfill progressif
            oldest = min(st)
            ref2 = st[max(st)][3]
            older = _with_timeout(lambda: _fetch_m1_page(asset, oldest, ref2),
                                  25, f"backfill({asset})")
            _merge_m1(asset, older)         # le backfill ne compte pas comme "frais"
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
                                  "updated": LAST_NEW.get(asset, 0), "error": None}
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
    if len(line) < sig:
        return None
    sg = _ema_series(line, sig)
    if not sg or sg[-1] is None:
        return None
    return line[-1] - sg[-1]


def _bollinger(c, p=20, mult=2):
    w = c[-p:]
    m = sum(w) / p
    sd = (sum((x - m) ** 2 for x in w) / p) ** 0.5
    return m - mult * sd, m + mult * sd


def _atr(h, l, c, p=14):
    tr = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
          for i in range(1, len(c))]
    if len(tr) < p:
        return None
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
    atr_last = _atr(h, l, c)
    if atr_last is None or atr_last <= 0:
        return None                      # marche fige : rien a analyser
    at_rel = atr_last / c[-1] * 10000
    if at_rel < 1:
        return None                      # volatilite morte : filtre avance
    r = _rsi(c); k = _stoch_k(h, l, c); cc = _cci(h, l, c); wr = _williams(h, l, c)
    mo = c[-1] - c[-11]
    hist = _macd_hist(c)
    if hist is None:
        return None
    blow, bup = _bollinger(c)
    adx_v, dip, din = _adx(h, l, c)
    adx_v = adx_v if adx_v is not None else 0.0
    sar = _sar_up(h, l); awo = _ao(h, l)
    e9 = _ema_series(c, 9)[-1]; e21 = _ema_series(c, 21)[-1]
    if e9 is None or e21 is None:
        return None

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
    # Fraicheur : pas de nouvelle bougie depuis 3 minutes -> flux interrompu
    # ou marche ferme -> aucune alerte (evite les repetitions au meme prix
    # et les faux signaux du week-end sur les paires reelles)
    if time.time() - d.get("updated", 0) > 180:
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
    # Heure de fin cible (Paris) : duree comptee depuis MAINTENANT, pour
    # compenser la latence alerte -> ouverture de PO -> prise de position
    try:
        from zoneinfo import ZoneInfo
        import datetime as _dt
        fin = (_dt.datetime.now(ZoneInfo("Europe/Paris"))
               + _dt.timedelta(minutes=exp)).strftime("%H:%M:%S")
        ligne_fin = f"\u23F1 Fin visee ~{fin} (Paris)\n"
    except Exception:
        ligne_fin = ""
    msg = (f"{icon} {asset} \u00b7 {TF_NAMES.get(tf, tf)}\n"
           f"Confluence {conf}% \u00b7 R\u00e9gime {reg}\n"
           f"Expiration : {exp} min \u2014 entre vite\n"
           f"{ligne_fin}"
           f"Cl\u00f4ture signal : {price} (le prix live peut differer d'une bougie)")
    mode = MODE["mode"]
    markup = None
    if TRADE_ENABLED and mode == "manuel":
        sid = _register_signal(asset, direction, exp, price)
        markup = {"inline_keyboard": [[
            {"text": f"\u26A1 Prendre ({TRADE_PCT}% du solde)",
             "callback_data": f"go|{sid}"}]]}
    send_telegram(msg, markup)
    if TRADE_ENABLED and mode == "auto":
        if conf >= AUTO_MIN_CONF:
            result, _oid = _execute_trade({"asset": asset, "dir": direction,
                                           "exp": exp, "price": price,
                                           "t": time.time()})
            send_telegram("\U0001F916 AUTO \u2014 " + result)
        # confluence insuffisante pour l'auto : alerte simple, pas de trade


def send_telegram(text, reply_markup=None):
    import requests
    try:
        payload = {"chat_id": TG_CHAT, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json=payload, timeout=10)
        log.info("Alerte Telegram envoyee")
    except Exception as e:
        log.warning("Echec envoi Telegram: %s", e)


# ==================== EXECUTION EN UN TAP (Telegram) ====================
# Garde-fous integres :
#  - bouton valable SIGNAL_TTL secondes, un seul usage (anti double-tap)
#  - mise = TRADE_PCT % du solde (min TRADE_MIN $)
#  - maximum TRADE_MAX_JOUR trades par jour
#  - confirmation ou echec explicite apres chaque ordre

_PENDING = {}          # sid -> signal en attente d'un tap
_TRADES_JOUR = {"date": "", "count": 0}


def _paris_date():
    try:
        from zoneinfo import ZoneInfo
        import datetime as _dt
        return _dt.datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d")
    except Exception:
        return time.strftime("%Y-%m-%d")


def _register_signal(asset, direction, exp_min, price):
    import uuid
    # purge des signaux perimes
    now = time.time()
    for k in [k for k, v in _PENDING.items() if now - v["t"] > SIGNAL_TTL * 2]:
        _PENDING.pop(k, None)
    sid = uuid.uuid4().hex[:10]
    _PENDING[sid] = {"asset": asset, "dir": direction,
                     "exp": exp_min, "price": price, "t": now}
    return sid


def _tg_api(method, payload):
    import requests
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}",
                          json=payload, timeout=35)
        return r.json()
    except Exception as e:
        log.warning("Telegram %s: %s", method, e)
        return None


def _execute_trade(sig):
    """Place l'ordre sur Pocket Option. Renvoie (message, order_id_ou_None)."""
    today = _paris_date()
    if _TRADES_JOUR["date"] != today:
        _TRADES_JOUR["date"] = today
        _TRADES_JOUR["count"] = 0
    if _TRADES_JOUR["count"] >= TRADE_MAX_JOUR:
        return (f"\U0001F6D1 Limite atteinte : {TRADE_MAX_JOUR} trades aujourd'hui. "
                "Rien n'a ete place. (PO_MAX_TRADES_JOUR pour ajuster)", None)
    if not CONNECTED["ok"] or API is None:
        return ("\u274C Pont deconnecte de Pocket Option — rien n'a ete place.", None)
    try:
        balance = API.get_balance()
    except Exception:
        balance = None
    if not balance or balance <= 0:
        return ("\u274C Solde indisponible — rien n'a ete place. "
                "Reessaie dans une minute.", None)
    est_rattrapage = False
    # mise de base : % du solde, plancher TRADE_MIN
    amount = round(max(TRADE_MIN, balance * TRADE_PCT / 100.0), 2)
    if RATTRAPAGE_ON and _RATTRAPAGE["arme"]:
        _RATTRAPAGE["arme"] = False          # consomme immediatement (un seul trade)
        est_rattrapage = True
        # multiplicateur applique a la mise REELLE (sinon le plancher de 1$
        # l'avale sur les petits soldes), borne par le cap en % du solde
        amount = round(min(amount * RATTRAPAGE_MULT,
                           balance * RATTRAPAGE_CAP_PCT / 100.0), 2)
        amount = max(amount, TRADE_MIN)
    pct_reel = round(amount / balance * 100.0, 1)
    action = "call" if sig["dir"] == "buy" else "put"
    exp_sec = int(sig["exp"] * 60)
    if not API_LOCK.acquire(timeout=30):
        return ("\u274C Pont occupe — rien n'a ete place. Reessaie.", None)
    try:
        ok, oid = _with_timeout(
            lambda: API.buy(amount, sig["asset"], action, exp_sec),
            70, f"buy({sig['asset']})")
    except Exception as e:
        return (f"\u274C Erreur pendant l'ordre : {e}. Verifie sur PO si une "
                "position est ouverte avant de retenter.", None)
    finally:
        API_LOCK.release()
    if not ok:
        return ("\u274C Ordre refuse par Pocket Option — rien n'a ete place. "
                "(solde insuffisant, actif ferme, ou montant hors limites)", None)
    _TRADES_JOUR["count"] += 1
    sens = "\U0001F7E2 ACHAT" if sig["dir"] == "buy" else "\U0001F534 VENTE"
    try:
        threading.Thread(target=_suivre_resultat,
                         args=(oid, sig["exp"], sig["asset"], amount, est_rattrapage),
                         daemon=True).start()
    except Exception:
        pass
    tag = " \U0001F501 RATTRAPAGE" if est_rattrapage else ""
    return (f"\u2705 Ordre place : {sens} {sig['asset']}{tag}\n"
            f"Mise : {amount}$ ({pct_reel}% de {round(balance, 2)}$) \u00b7 "
            f"Expiration {sig['exp']} min\n"
            f"Trade {_TRADES_JOUR['count']}/{TRADE_MAX_JOUR} du jour \u00b7 id {oid}", oid)


def _suivre_resultat(oid, exp_min, asset, amount, est_rattrapage=False):
    """Attend l'expiration puis lit le resultat du trade. Alimente le compteur
    de pertes consecutives ; en mode auto, AUTO_STOP_PERTES pertes de suite
    repassent le pont en manuel."""
    time.sleep(exp_min * 60 + 8)
    try:
        profit, status = _with_timeout(lambda: API.check_win(oid),
                                       70, f"check_win({oid})")
    except Exception:
        return
    if status == "win":
        _PERTES_SUITE["n"] = 0
        _RATTRAPAGE["arme"] = False
        extra = " \U0001F501 (rattrapage reussi)" if est_rattrapage else ""
        send_telegram(f"\u2705 R\u00e9sultat {asset} : GAGN\u00c9 +{profit}${extra}")
    elif status == "draw":
        _PERTES_SUITE["n"] = 0
        _RATTRAPAGE["arme"] = False
        send_telegram(f"\u2796 R\u00e9sultat {asset} : \u00c9GALIT\u00c9 (mise rendue)")
    elif status == "loose":
        _PERTES_SUITE["n"] += 1
        n = _PERTES_SUITE["n"]
        if est_rattrapage:
            # rattrapage perdu : retour a la base, JAMAIS de niveau 2
            send_telegram(f"\u274C R\u00e9sultat {asset} : rattrapage PERDU -{amount}$ "
                          f"\u2014 retour a la mise de base ({n} pertes de suite)")
        elif RATTRAPAGE_ON:
            _RATTRAPAGE["arme"] = True
            send_telegram(f"\u274C R\u00e9sultat {asset} : PERDU -{amount}$ "
                          f"({n} perte{'s' if n > 1 else ''} de suite)\n"
                          f"\U0001F501 Prochain trade en rattrapage "
                          f"(x{RATTRAPAGE_MULT}, plafonn\u00e9 {RATTRAPAGE_CAP_PCT}%)")
        else:
            send_telegram(f"\u274C R\u00e9sultat {asset} : PERDU -{amount}$ "
                          f"({n} perte{'s' if n > 1 else ''} de suite)")
        if MODE["mode"] == "auto" and n >= AUTO_STOP_PERTES:
            MODE["mode"] = "manuel"
            send_telegram(f"\U0001F6D1 {AUTO_STOP_PERTES} pertes consecutives : "
                          "mode AUTO suspendu, retour en MANUEL.\n"
                          "Envoie /auto pour reactiver quand tu es pret.")
    # status "unknown" : pas de message, pas d'impact sur le compteur


def telegram_poller():
    """Ecoute les taps sur les boutons des alertes (long polling getUpdates)."""
    # ignorer l'historique accumule avant le demarrage
    offset = 0
    init = _tg_api("getUpdates", {"timeout": 0})
    if init and init.get("result"):
        offset = init["result"][-1]["update_id"] + 1
    _tg_api("setMyCommands", {"commands": [
        {"command": "auto", "description": "Execution automatique (conf >= %d%%)" % AUTO_MIN_CONF},
        {"command": "manuel", "description": "Alertes avec bouton (defaut)"},
        {"command": "off", "description": "Alertes seules, aucune execution"},
        {"command": "statut", "description": "Mode, solde, trades du jour"},
    ]})
    log.info("Ecoute Telegram active — mode %s (execution %s%% du solde, "
             "max %d trades/jour)", MODE["mode"].upper(), TRADE_PCT, TRADE_MAX_JOUR)
    while True:
        try:
            resp = _tg_api("getUpdates", {"timeout": 25, "offset": offset})
            if not resp or not resp.get("ok"):
                time.sleep(3)
                continue
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                # ---- commandes texte : /auto /manuel /off /statut ----
                m = upd.get("message")
                if m and str(m.get("from", {}).get("id", "")) == str(TG_CHAT):
                    cmd = (m.get("text") or "").strip().lower()
                    if cmd in ("/auto", "/manuel", "/off", "/statut", "/status"):
                        if cmd == "/auto":
                            MODE["mode"] = "auto"
                            _PERTES_SUITE["n"] = 0
                            send_telegram(
                                "\U0001F916 Mode AUTO actif.\n"
                                f"\u2022 Trades automatiques si confluence \u2265 {AUTO_MIN_CONF}%\n"
                                f"\u2022 Mise {TRADE_PCT}% du solde \u00b7 max {TRADE_MAX_JOUR}/jour\n"
                                f"\u2022 Suspension auto apres {AUTO_STOP_PERTES} pertes de suite\n"
                                "/manuel ou /off pour changer.")
                        elif cmd == "/manuel":
                            MODE["mode"] = "manuel"
                            send_telegram("\u26A1 Mode MANUEL : alertes avec bouton, "
                                          "rien ne se place sans ton tap.")
                        elif cmd == "/off":
                            MODE["mode"] = "off"
                            send_telegram("\U0001F4E2 Mode OFF : alertes seules, "
                                          "aucune execution possible.")
                        else:
                            try:
                                bal = API.get_balance() if API else None
                            except Exception:
                                bal = None
                            send_telegram(
                                f"\U0001F4CA Statut\n"
                                f"Mode : {MODE['mode'].upper()}\n"
                                f"Connexion : {'OK' if CONNECTED['ok'] else CONNECTED['msg']}\n"
                                f"Solde : {round(bal, 2) if bal else 'inconnu'}$\n"
                                f"Trades du jour : {_TRADES_JOUR['count']}/{TRADE_MAX_JOUR}\n"
                                f"Pertes de suite : {_PERTES_SUITE['n']}\n"
                                f"Rattrapage : "
                                + ("ARME (prochain trade x%s)" % RATTRAPAGE_MULT if _RATTRAPAGE["arme"]
                                   else ("actif (1 niveau)" if RATTRAPAGE_ON else "desactive")))
                    continue
                cq = upd.get("callback_query")
                if not cq:
                    continue
                cq_id = cq["id"]
                data = cq.get("data", "")
                if str(cq.get("from", {}).get("id", "")) != str(TG_CHAT):
                    _tg_api("answerCallbackQuery",
                            {"callback_query_id": cq_id, "text": "Non autorise"})
                    continue
                if not data.startswith("go|"):
                    _tg_api("answerCallbackQuery", {"callback_query_id": cq_id})
                    continue
                sid = data[3:]
                sig = _PENDING.pop(sid, None)       # usage unique
                if sig is None:
                    _tg_api("answerCallbackQuery",
                            {"callback_query_id": cq_id,
                             "text": "Signal deja utilise ou inconnu"})
                    continue
                age = time.time() - sig["t"]
                if age > SIGNAL_TTL:
                    _tg_api("answerCallbackQuery",
                            {"callback_query_id": cq_id,
                             "text": f"Signal perime ({int(age)}s > {SIGNAL_TTL}s)"})
                    send_telegram(f"\u23F3 Trop tard pour {sig['asset']} : "
                                  f"signal vieux de {int(age)}s. Rien n'a ete place.")
                    continue
                _tg_api("answerCallbackQuery",
                        {"callback_query_id": cq_id, "text": "Ordre en cours..."})
                result, _oid = _execute_trade(sig)
                send_telegram(result)
        except Exception as e:
            log.warning("Poller Telegram: %s", e)
            time.sleep(5)

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
                        import traceback
                        tb = traceback.format_exc().strip().splitlines()
                        log.warning("analyse/alerte %s tf%d: %s | %s",
                                    asset, tf, e, tb[-3] if len(tb) >= 3 else tb[-1])
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
        # Chien de garde : si AUCUN actif n'a recu de nouvelle bougie depuis
        # 5 minutes (l'OTC cote 24h/24, c'est donc anormal), la connexion est
        # consideree comme morte et on force une reconnexion propre.
        freshest = max(LAST_NEW.values()) if LAST_NEW else 0
        if freshest and time.time() - freshest > 300:
            CONNECTED["ok"] = False
            CONNECTED["msg"] = "flux fige depuis 5 min — reconnexion forcee"
            log.warning("Chien de garde : %s", CONNECTED["msg"])
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
    now_ts = time.time()
    figes = [a for a, t in LAST_NEW.items() if t and now_ts - t > 180]
    return jsonify({
        "mode": MODE_LABEL,
        "flux_figes": figes,
        "connected": CONNECTED["ok"],
        "status": CONNECTED["msg"],
        "uptime_sec": round(now - CONNECTED["since"], 0) if CONNECTED["since"] else None,
        "reconnects": CONNECTED["reconnects"],
        "telegram": TG_ENABLED,
        "trade_un_tap": TRADE_ENABLED,
        "mode_execution": MODE["mode"],
        "pertes_de_suite": _PERTES_SUITE["n"],
        "trades_aujourdhui": _TRADES_JOUR["count"],
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

if TG_ENABLED and TRADE_ENABLED:
    tp = threading.Thread(target=telegram_poller, daemon=True)
    tp.start()

if __name__ == "__main__":
    log.info("PO Bridge v3 (%s) sur le port %d — actifs: %s, tfs: %s",
             MODE_LABEL, PORT, ASSETS, TIMEFRAMES)
    app.run(host="0.0.0.0", port=PORT, debug=False)
