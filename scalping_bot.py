#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot — ST AI 4H + Bias 1H + flip ST AI 15m
# Service Railway séparé

import json
import time
import requests
import logging
import threading
import os
import redis as redis_lib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

CONFIG = {
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID':   os.environ.get('TELEGRAM_CHAT_ID', ''),
    'REDIS_URL':          os.environ.get('REDIS_URL', ''),
    'MIN_COOLDOWN':       3600,   # 1H entrée
    'PYRA_COOLDOWN':      1800,   # 30min pyramiding

    'SYMBOLS': {
        'AVAX/USDT':    {'exchange': 'okx'},
        'INJ/USDT':     {'exchange': 'okx'},
        'LTC/USDT':     {'exchange': 'okx'},
        'SUI/USDT':     {'exchange': 'okx'},
        'XRP/USDT':     {'exchange': 'okx'},
    }
}

# ============================================================================
# OKX — Calcul Bias 1H
# ============================================================================

def fetch_ohlcv_okx(symbol, tf, limit=100):
    """Fetch OHLCV depuis OKX API publique."""
    try:
        inst_id = symbol.replace('/', '-')
        bar_map = {'1h': '1H', '4h': '4H', '1d': '1D'}
        bar = bar_map.get(tf, '1H')
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
        resp = requests.get(url, timeout=10)
        data = resp.json().get('data', [])
        if not data:
            return None
        df = pd.DataFrame(data, columns=['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'])
        df = df[df['confirm'] == '1'].copy()
        df['close'] = df['c'].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        logger.debug(f"[OKX] {symbol} {tf}: {e}")
        return None

def calc_bias(df, ema_len=13, sma_len=30):
    """Calcule le bias EMA/SMA."""
    try:
        if df is None or len(df) < sma_len:
            return None
        close = df['close']
        ema = close.ewm(span=ema_len, adjust=False).mean().iloc[-1]
        sma = close.rolling(sma_len).mean().iloc[-1]
        c   = close.iloc[-1]
        if c > ema and ema > sma:
            return 'bull'
        if c < ema and ema < sma:
            return 'bear'
        return None
    except:
        return None

def update_bias_1h():
    """Met à jour le Bias 1H pour tous les assets toutes les 15min."""
    logger.info("📊 Scheduler Bias 1H démarré")
    while True:
        try:
            # Calculer tous les bias HORS du lock (les fetches OKX peuvent être longs)
            results = {}
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df = fetch_ohlcv_okx(symbol, '1h', limit=50)
                    if df is not None:
                        results[symbol] = calc_bias(df, ema_len=13, sma_len=30)
                except Exception as e:
                    logger.debug(f"[BIAS] {symbol}: {e}")
            # Mettre à jour l'état avec des locks courts symbol par symbol
            for symbol, bias in results.items():
                with STATE_LOCK:
                    init_symbol(symbol)
                    MOMENTUM_STATE[symbol]['bias_1h'] = bias
            logger.info("[BIAS] Mise à jour Bias 1H terminée")
        except Exception as e:
            logger.error(f"[BIAS] Erreur: {e}")
        time.sleep(900)  # toutes les 15min


# ============================================================================
# STATE
# ============================================================================

STATE_LOCK       = threading.RLock()  # RLock pour éviter deadlock (should_send appelé dans le lock)
MOMENTUM_STATE   = {}   # symbol -> {st_ai_15m, st_ai_4h, bias_1h, last_st_15m, ...}
SCALP_POSITIONS  = {}   # f"{symbol}_SCALP" -> {direction, entry_count}
PYRA_ENABLED     = {}   # f"{symbol}_SCALP" -> True
LAST_SIGNALS     = {}
LAST_SIGNAL_EVENTS = {}
REDIS_CLIENT     = None

# ============================================================================
# REDIS
# ============================================================================

def init_redis():
    global REDIS_CLIENT
    url = CONFIG.get('REDIS_URL', '')
    if not url:
        logger.warning("⚠️ REDIS_URL non défini — démarrage sans Redis")
        return
    try:
        REDIS_CLIENT = redis_lib.from_url(url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info("✅ Redis connecté")
    except Exception as e:
        logger.error(f"❌ Redis connexion: {e}")
        REDIS_CLIENT = None

def persist_state():
    if not REDIS_CLIENT:
        return
    try:
        with STATE_LOCK:
            payload = {
                'momentum':   dict(MOMENTUM_STATE),
                'positions':  dict(SCALP_POSITIONS),
                'pyra':       dict(PYRA_ENABLED),
                'signals':    dict(LAST_SIGNALS),
                'events':     dict(LAST_SIGNAL_EVENTS),
            }
            serialized = json.dumps(payload)
        REDIS_CLIENT.set('scalp_bot_state', serialized)
    except Exception as e:
        logger.error(f"Redis save error: {e}")

def load_state():
    global MOMENTUM_STATE, SCALP_POSITIONS, PYRA_ENABLED, LAST_SIGNALS, LAST_SIGNAL_EVENTS
    if not REDIS_CLIENT:
        return
    try:
        raw = REDIS_CLIENT.get('scalp_bot_state')
        if not raw:
            return
        payload = json.loads(raw)
        MOMENTUM_STATE  = payload.get('momentum', {})
        SCALP_POSITIONS    = payload.get('positions', {})
        PYRA_ENABLED       = payload.get('pyra', {})
        LAST_SIGNALS       = payload.get('signals', {})
        LAST_SIGNAL_EVENTS = payload.get('events', {})
        # Nettoyer les assets hors watchlist
        stale = [s for s in list(MOMENTUM_STATE) if s not in CONFIG['SYMBOLS']]
        for s in stale:
            del MOMENTUM_STATE[s]
        logger.info(f"✅ State Redis chargé ({len(MOMENTUM_STATE)} assets)")
    except Exception as e:
        logger.error(f"Redis load error: {e}")

# ============================================================================
# HELPERS
# ============================================================================

def init_symbol(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'st_ai_15m':      None,
            'st_ai_1h':       None,
            'st_ai_4h':       None,
            'bias_1h':        None,
            'last_st_15m':    None,
            'last_st_1h':     None,
            'st_4h_flipped':  False,
            'st_context_5m':    None,
            'st_context_15m':   None,
            'st_context_lt_5m': None,
            'st_context_1h':    None,
        }

def format_price(price):
    if price is None:
        return '?'
    try:
        p = float(price)
        if p >= 1000:  return f"{p:,.0f}"
        if p >= 1:     return f"{p:.4f}"
        if p >= 0.01:  return f"{p:.5f}"
        return f"{p:.8f}"
    except:
        return str(price)

def parse_st_value(val):
    try:
        v = float(val)
        return 'buy' if v == 1 else 'sell' if v == 0 else None
    except:
        return None

def should_send(symbol, key, cooldown=3600, event_id=None):
    now = time.time()
    k   = f"{symbol}:{key}"
    with STATE_LOCK:
        if event_id and LAST_SIGNAL_EVENTS.get(k) == event_id:
            return False
        if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > cooldown):
            LAST_SIGNALS[k] = now
            if event_id:
                LAST_SIGNAL_EVENTS[k] = event_id
            return True
    return False

def send_telegram(msg):
    tok  = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat = os.environ.get('TELEGRAM_CHAT_ID', '')
    if not tok or not chat:
        logger.warning("⚠️ Token ou chat_id manquant")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("✅ Telegram envoyé")
        else:
            logger.error(f"❌ Telegram {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def send_telegram_with_buttons(msg, callback_key):
    tok  = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat = os.environ.get('TELEGRAM_CHAT_ID', '')
    if not tok or not chat:
        logger.warning("⚠️ Token ou chat_id manquant — position créée sans notification")
        return False
    try:
        keyboard = {"inline_keyboard": [[
            {"text": "📈 Activer pyramiding", "callback_data": f"pyra_on:{callback_key}"},
            {"text": "❌ Ignorer",             "callback_data": f"pyra_off:{callback_key}"}
        ]]}
        resp = requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML", "reply_markup": keyboard},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("✅ Telegram avec boutons envoyé")
            return True
        else:
            logger.error(f"❌ Telegram buttons {resp.status_code}: {resp.text[:100]}")
            return False
    except Exception as e:
        logger.error(f"Telegram buttons error: {e}")
        return False

# ============================================================================
# WEBHOOK
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'status': 'error', 'reason': 'no_json'}), 400

    # Parser les champs
    raw_symbol = str(data.get('symbol', '')).strip().upper()
    tf         = str(data.get('tf', '')).strip().lower()
    alert_type = str(data.get('type', '')).strip().lower()
    val        = data.get('value')
    price      = data.get('price', 0)
    event_id   = data.get('event_id') or data.get('time') or str(time.time())

    # Normaliser tf
    tf_aliases = {'15': '15m', '60': '1h', '180': '3h', '3hr': '3h', '3hour': '3h', '240': '4h', '4hr': '4h', '4hour': '4h'}
    tf = tf_aliases.get(tf, tf)

    # Normaliser symbol
    if '/' not in raw_symbol:
        for q in ['USDT', 'USDC']:
            if raw_symbol.endswith(q):
                raw_symbol = raw_symbol[:-len(q)] + '/' + q
                break
    if not raw_symbol.endswith('/USDT'):
        raw_symbol = raw_symbol.replace('/USDC', '/USDT')

    symbol = raw_symbol
    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]

    logger.info(f"📥 Webhook: {symbol} | tf={tf} | type={alert_type} | val={val}")

    # ── Mise à jour état ──────────────────────────────────────────────
    if alert_type == 'supertrend':
        parsed = parse_st_value(val)
        if tf == '15m':
            prev_15m = m.get('st_ai_15m')
            m['st_ai_15m'] = parsed
            if prev_15m and parsed and parsed != prev_15m:
                m['last_st_15m'] = prev_15m
        elif tf == '4h':
            prev_4h = m.get('st_ai_4h')
            m['st_ai_4h'] = parsed
            m['st_4h_flipped'] = bool(prev_4h and parsed and parsed != prev_4h)
            if m['st_4h_flipped'] and prev_4h:
                m['last_st_4h'] = prev_4h

    elif alert_type == 'supertrend' and tf == '1h':
        parsed_1h = parse_st_value(val)
        prev_1h   = m.get('st_ai_1h')
        m['st_ai_1h']   = parsed_1h
        m['last_st_1h'] = prev_1h
        # Alerte flip ST AI 1H + Context 1H aligné
        flipped_1h = (parsed_1h is not None and prev_1h is not None and parsed_1h != prev_1h)
        if flipped_1h:
            ctx_1h = m.get('st_context_1h')
            exp    = parsed_1h  # 'buy' ou 'sell'
            if ctx_1h == exp:
                direction_1h = 'LONG' if parsed_1h == 'buy' else 'SHORT'
                emoji = '🟢' if direction_1h == 'LONG' else '🔴'
                msg_1h = (
                    f"{emoji} <b>[INFO - ST AI 1H + CTX 1H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction_1h}\n"
                    f"💰 Price: ${format_price(price)}\n"
                    f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                    f"✅ Flip ST AI 1H: {parsed_1h.upper()}\n"
                    f"✅ ST Context 1H: {ctx_1h.upper()}"
                )
                send_telegram(msg_1h)
                logger.info(f"[INFO] Flip ST AI 1H + Ctx 1H aligné: {symbol} {direction_1h}")

    elif alert_type == 'bias':
        bias_val = str(val).lower() if val else None
        if bias_val in ('bull', 'bear', 'neutral') and tf == '1h':
            prev_bias = m.get('bias_1h')
            new_bias_val = bias_val if bias_val != 'neutral' else None
            m['bias_1h'] = new_bias_val
            # Alerte changement Bias 1H + Context 1H aligné
            if prev_bias != new_bias_val and new_bias_val is not None:
                ctx_1h = m.get('st_context_1h')
                exp_ctx = 'buy' if new_bias_val == 'bull' else 'sell'
                if ctx_1h == exp_ctx:
                    direction_b = 'LONG' if new_bias_val == 'bull' else 'SHORT'
                    emoji = '🟢' if direction_b == 'LONG' else '🔴'
                    msg_bias = (
                        f"{emoji} <b>[INFO - BIAS 1H + CTX 1H]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_b}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"✅ Bias 1H: {new_bias_val.upper()} (changement)\n"
                        f"✅ ST Context 1H: {ctx_1h.upper()}"
                    )
                    send_telegram(msg_bias)
                    logger.info(f"[INFO] Bias 1H changé + Ctx 1H aligné: {symbol} {direction_b}")

    elif alert_type == 'st_context_lt' and tf == '5m':
        try:
            lt_val = float(val)
            lt_parsed = 'buy' if lt_val < -1.96 else 'sell' if lt_val > 1.96 else None
        except:
            lt_parsed = None
        m['st_context_lt_5m'] = lt_parsed

    elif alert_type == 'st_context':
        try:
            ctx_val = float(val)
            ctx_parsed = 'buy' if ctx_val < -1.96 else 'sell' if ctx_val > 1.96 else None
        except:
            ctx_parsed = None
        if tf == '5m':
            m['st_context_5m'] = ctx_parsed
        elif tf == '15m':
            m['st_context_15m'] = ctx_parsed
        elif tf == '1h':
            m['st_context_1h'] = ctx_parsed

    # ── Logique SCALP ─────────────────────────────────────────────────
    # ENTRÉE PRINCIPALE  : ST Context 5m  + ST AI 4H + Bias 1H
    #   Anti-chop : ST Context 15m opposé OU ST Context LT 5m même sens
    # ENTRÉE SECONDAIRE  : flip ST AI 15m + ST AI 4H + Bias 1H
    #   Anti-chop : ST Context 5m opposé
    # PYRAMIDING         : flip ST AI 15m + ST AI 4H + Bias 1H (stop si Bias 1H change)
    #   Cooldown 30min

    if alert_type in ('st_context', 'supertrend') and tf in ('5m', '15m'):
        st_4h       = m.get('st_ai_4h')
        bias_1h     = m.get('bias_1h')
        ctx_5m      = m.get('st_context_5m')
        ctx_15m     = m.get('st_context_15m')
        ctx_lt_5m   = m.get('st_context_lt_5m')
        st_15m      = m.get('st_ai_15m')
        prev_15m    = m.get('last_st_15m')
        flipped_15m = (st_15m is not None and prev_15m is not None and st_15m != prev_15m)

        # ── Déterminer signal et direction ──────────────────────────
        if alert_type == 'st_context' and tf == '5m':
            try:
                ctx_val    = float(val)
                ctx_parsed = 'buy' if ctx_val < -1.96 else 'sell' if ctx_val > 1.96 else None
            except:
                ctx_parsed = None
            if ctx_parsed is None:
                return jsonify({'status': 'ok'}), 200
            m['st_context_5m'] = ctx_parsed
            ctx_5m             = ctx_parsed
            signal_direction   = "LONG" if ctx_parsed == 'buy' else "SHORT"
            signal_type        = 'ctx5m'
        elif alert_type == 'supertrend' and tf == '15m' and flipped_15m:
            signal_direction = "LONG" if st_15m == 'buy' else "SHORT"
            signal_type      = 'flip15m'
        else:
            return jsonify({'status': 'ok'}), 200

        exp_st_4h = 'buy'  if signal_direction == 'LONG' else 'sell'
        exp_bias  = 'bull' if signal_direction == 'LONG' else 'bear'
        exp_ctx   = 'buy'  if signal_direction == 'LONG' else 'sell'
        opp_ctx   = 'sell' if signal_direction == 'LONG' else 'buy'

        st_4h_ok   = st_4h   == exp_st_4h
        bias_1h_ok = bias_1h == exp_bias

        # ── Anti-chop selon le signal ────────────────────────────────
        if signal_type == 'ctx5m':
            antichop_15m  = (ctx_15m   == opp_ctx) if ctx_15m   is not None else False
            antichop_lt5m = (ctx_lt_5m == exp_ctx) if ctx_lt_5m is not None else False
            antichop_blocked = antichop_15m or antichop_lt5m
        else:  # flip15m
            antichop_blocked = (ctx_5m == opp_ctx) if ctx_5m is not None else False

        pos_key = f"{symbol}_SCALP"
        with STATE_LOCK:
            pos = SCALP_POSITIONS.get(pos_key)
            # Retournement → reset position
            if pos and pos['direction'] != signal_direction:
                SCALP_POSITIONS.pop(pos_key, None)
                PYRA_ENABLED.pop(pos_key, None)
                pos = None

            # Entrée principale (ctx5m) ou secondaire (flip15m)
            is_entry = (
                st_4h_ok and bias_1h_ok
                and not antichop_blocked
                and pos is None
            )

            # Pyramiding — uniquement sur flip15m, Bias 1H doit rester aligné
            is_pyra = bool(
                signal_type == 'flip15m'
                and pos and pos['direction'] == signal_direction
                and st_4h_ok and bias_1h_ok
                and not antichop_blocked
                and PYRA_ENABLED.get(pos_key, False)
            )

            if is_entry and should_send(symbol, f"scalp_entry_{exp_ctx}", event_id=event_id, cooldown=3600):
                SCALP_POSITIONS[pos_key] = {'direction': signal_direction, 'entry_count': 1}
                PYRA_ENABLED.pop(pos_key, None)
                pos = SCALP_POSITIONS[pos_key]
            else:
                is_entry = False

        if is_entry and pos:
            emoji = "\U0001f7e2" if signal_direction == "LONG" else "\U0001f534"
            if signal_type == 'ctx5m':
                signal_txt   = f"ST Context 5m: {(ctx_5m or '?').upper()} (SIGNAL PRINCIPAL)"
                antichop_txt = f"ST Context 15m: {(ctx_15m or 'NEUTRE').upper()} | LT 5m: {(ctx_lt_5m or 'NEUTRE').upper()}"
            else:
                signal_txt   = f"Flip ST AI 15m: {(st_15m or '?').upper()} (SIGNAL SECONDAIRE)"
                antichop_txt = f"ST Context 5m: {(ctx_5m or 'NEUTRE').upper()} (anti-chop)"
            tg_sent = send_telegram_with_buttons(
                f"{emoji} <b>[SCALP - ENTREE]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📈 Direction: {signal_direction}\n"
                f"💰 Price: ${format_price(price)}\n"
                f"🏦 Exchange: OKX\n"
                f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"✅ ST AI 4H: {(st_4h or '?').upper()} (filtre)\n"
                f"✅ Bias 1H: {(bias_1h or '?').upper()} (EMA13/SMA30)\n"
                f"✅ {signal_txt}\n"
                f"ℹ️ {antichop_txt}",
                pos_key
            )
            if not tg_sent:
                logger.warning(f"[SCALP] Entrée {symbol} créée mais notification Telegram échouée")
            logger.info(f"[SCALP] Entrée: {symbol} {signal_direction} | signal={signal_type}")
            persist_state()

        elif is_pyra and should_send(symbol, f"scalp_pyra_{exp_ctx}", event_id=event_id, cooldown=1800):
            with STATE_LOCK:
                pos['entry_count'] += 1
                count = pos['entry_count']
            emoji = "\U0001f7e2" if signal_direction == "LONG" else "\U0001f534"
            send_telegram(
                f"{emoji} <b>[SCALP - PYRAMIDING #{count}]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📈 Direction: {signal_direction}\n"
                f"💰 Price: ${format_price(price)}\n"
                f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"✅ ST AI 4H: {(st_4h or '?').upper()}\n"
                f"✅ Bias 1H: {(bias_1h or '?').upper()}\n"
                f"✅ Flip ST AI 15m: {(st_15m or '?').upper()}\n"
                f"ℹ️ ST Context 5m: {(ctx_5m or 'NEUTRE').upper()} (anti-chop)"
            )
            logger.info(f"[SCALP] Pyramiding #{count}: {symbol} {signal_direction}")
            persist_state()


    return jsonify({'status': 'ok'}), 200

# ============================================================================
# TELEGRAM CALLBACK (boutons)
# ============================================================================

@app.route('/telegram_callback', methods=['POST'])
def telegram_callback():
    tg_secret = os.environ.get('SCALP_TELEGRAM_SECRET', '')
    if tg_secret:
        if request.headers.get('X-Telegram-Bot-Api-Secret-Token', '') != tg_secret:
            return jsonify({'ok': False}), 403

    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': True}), 200
    try:
        cb      = data.get('callback_query', {})
        cb_id   = cb.get('id')
        cb_data = cb.get('data', '')
        chat_id = cb.get('message', {}).get('chat', {}).get('id')
        msg_id  = cb.get('message', {}).get('message_id')
        user    = cb.get('from', {}).get('first_name', 'User')
        tok     = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')

        if tok and cb_id:
            requests.post(f"https://api.telegram.org/bot{tok}/answerCallbackQuery",
                         json={"callback_query_id": cb_id}, timeout=5)

        if cb_data.startswith('pyra_on:'):
            key = cb_data[len('pyra_on:'):]
            with STATE_LOCK:
                PYRA_ENABLED[key] = True
            persist_state()
            logger.info(f"[PYRA] Activé: {key}")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "✅ Pyramiding activé", "callback_data": "noop"}
                                   ]]}}, timeout=5)

        elif cb_data.startswith('pyra_off:'):
            key = cb_data[len('pyra_off:'):]
            with STATE_LOCK:
                PYRA_ENABLED.pop(key, None)
            persist_state()
            logger.info(f"[PYRA] Ignoré: {key}")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "❌ Pyramiding ignoré", "callback_data": "noop"}
                                   ]]}}, timeout=5)

    except Exception as e:
        logger.error(f"[CALLBACK] Erreur: {e}")
    return jsonify({'ok': True}), 200

# ============================================================================
# ROUTES UTILITAIRES
# ============================================================================

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'status': 'ok',
        'bot': 'Scalping Bot',
        'assets': len(CONFIG['SYMBOLS']),
        'positions': len(SCALP_POSITIONS),
    })

@app.route('/reset', methods=['POST'])
def reset():
    secret = os.environ.get('ADMIN_SECRET', '')
    if not secret:
        logger.error('ADMIN_SECRET non défini — /reset refusé')
        return jsonify({'error': 'unauthorized'}), 401
    if request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    with STATE_LOCK:
        SCALP_POSITIONS.clear()
        PYRA_ENABLED.clear()
        LAST_SIGNALS.clear()
        LAST_SIGNAL_EVENTS.clear()
    persist_state()

    # Auto sync_scalp depuis le bot principal
    main_url   = os.environ.get('MAIN_BOT_URL', '').rstrip('/')
    admin_secret = os.environ.get('ADMIN_SECRET', '')
    if main_url and admin_secret:
        try:
            resp = requests.post(
                f'{main_url}/sync_scalp',
                headers={'X-Admin-Secret': admin_secret},
                timeout=10
            )
            logger.info(f'[RESET] sync_scalp auto: {resp.status_code}')
        except Exception as e:
            logger.warning(f'[RESET] sync_scalp auto échoué: {e}')

    return jsonify({'status': 'reset'}), 200

# ============================================================================
# DÉMARRAGE
# ============================================================================

def startup():
    init_redis()
    load_state()

    tok      = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
    base_url = os.environ.get('SCALP_PUBLIC_URL', '').rstrip('/')
    if tok and base_url:
        try:
            wh_url    = f"{base_url}/telegram_callback"
            wh_payload = {'url': wh_url}
            tg_secret  = os.environ.get('SCALP_TELEGRAM_SECRET', '')
            if tg_secret:
                wh_payload['secret_token'] = tg_secret
            requests.post(f"https://api.telegram.org/bot{tok}/setWebhook",
                         json=wh_payload, timeout=10)
            logger.info(f"✅ Telegram webhook configuré: {wh_url}")
        except Exception as e:
            logger.warning(f"⚠️ Webhook setup: {e}")

    # Démarrer le scheduler Bias 1H
    bias_thread = threading.Thread(target=update_bias_1h, daemon=True)
    bias_thread.start()

    send_telegram(
        "🚀 <b>Scalping Bot démarré</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"⚙️ Stratégie: ST AI 4H + Bias 1H + flip ST AI 15m\n"
        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}"
    )

if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    t = threading.Thread(target=startup, daemon=True)
    t.start()

