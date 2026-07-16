#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot — ST AI 1H + Bias 15m + Zone Context 1m
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
        'BTC/USDT':     {'exchange': 'okx'},
        'ETH/USDT':     {'exchange': 'okx'},
        'INJ/USDT':     {'exchange': 'okx'},
        'LTC/USDT':     {'exchange': 'okx'},
        'SOL/USDT':     {'exchange': 'okx'},
        'SUI/USDT':     {'exchange': 'okx'},
        'XRP/USDT':     {'exchange': 'okx'},
    }
}

# ============================================================================
# OKX — Calcul Bias
# ============================================================================

def fetch_ohlcv_okx(symbol, tf, limit=100):
    """Fetch OHLCV depuis OKX API publique."""
    try:
        inst_id = symbol.replace('/', '-')
        bar_map = {'1m': '1m', '15m': '15m', '1h': '1H', '2h': '2H', '4h': '4H', '1d': '1D'}
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

def update_bias_15m():
    """Met à jour le Bias 15m pour tous les assets toutes les 5min."""
    logger.info("📊 Scheduler Bias 15m démarré")
    while True:
        try:
            # Calculer tous les bias HORS du lock (les fetches OKX peuvent 锚tre longs)
            results = {}
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df = fetch_ohlcv_okx(symbol, '15m', limit=50)
                    if df is not None:
                        results[symbol] = {
                            'bias': calc_bias(df, ema_len=13, sma_len=30),
                            'price': float(df['close'].iloc[-1]) if len(df) else None,
                        }
                except Exception as e:
                    logger.debug(f"[BIAS] {symbol}: {e}")
            # Mettre à jour l'état avec des locks courts symbol par symbol
            pending_alerts = []
            for symbol, result in results.items():
                bias = result.get('bias')
                price = result.get('price')
                with STATE_LOCK:
                    init_symbol(symbol)
                    m = MOMENTUM_STATE[symbol]
                    m['bias_15m'] = bias
            for msg, log_msg in pending_alerts:
                send_telegram(msg)
                logger.info(log_msg)
            persist_state()
            logger.info("[BIAS] Mise à jour Bias 15m terminée")
        except Exception as e:
            logger.error(f"[BIAS] Erreur: {e}")
        time.sleep(300)  # toutes les 5min


# ============================================================================
# STATE
# ============================================================================

STATE_LOCK       = threading.RLock()  # RLock pour éviter deadlock (should_send appelé dans le lock)
MOMENTUM_STATE   = {}   # symbol -> {st_ai_15m, st_ai_4h, bias_2h, last_st_15m, ...}
SCALP_POSITIONS  = {}   # f"{symbol}_SCALP" -> {direction, entry_count}
PYRA_ENABLED     = {}   # f"{symbol}_SCALP" -> True
LAST_SIGNALS     = {}
LAST_SIGNAL_EVENTS = {}
SCALP_ENABLED    = True
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
                'enabled':    SCALP_ENABLED,
            }
            serialized = json.dumps(payload)
        REDIS_CLIENT.set('scalp_bot_state', serialized)
    except Exception as e:
        logger.error(f"Redis save error: {e}")

def load_state():
    global MOMENTUM_STATE, SCALP_POSITIONS, PYRA_ENABLED, LAST_SIGNALS, LAST_SIGNAL_EVENTS, SCALP_ENABLED
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
        SCALP_ENABLED      = bool(payload.get('enabled', True))
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
            'st_ai_2h':       None,
            'st_ai_4h':       None,
            'bias_1h':        None,
            'bias_15m':       None,
            'bias_2h':        None,
            'last_st_15m':    None,
            'last_st_1h':     None,
            'last_st_2h':     None,
            'st_4h_flipped':  False,
            'st_context_1m':    None,
            'st_context_lt_1m': None,
            'st_context_1m_ts': None,
            'st_context_lt_1m_ts': None,
            'st_context_5m':    None,
            'st_context_15m':   None,
            'st_context_lt_5m': None,
            'st_context_1h':    None,
            'st_context_2h':    None,
            'bias_2h_ready':    False,
            'last_bias_2h_change_ts': None,
            'last_st_ai_2h_flip_ts': None,
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
    normalized = str(val).strip().lower()
    if normalized in ('1', 'buy'):  return 'buy'
    if normalized in ('0', 'sell'): return 'sell'
    return None

def is_fresh(ts, max_age_seconds):
    try:
        return ts is not None and (time.time() - float(ts)) <= max_age_seconds
    except (TypeError, ValueError):
        return False

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

def build_ctx2h_bias2h_alert(symbol, price=None):
    m = MOMENTUM_STATE.get(symbol, {})
    bias_2h = m.get('bias_2h')
    ctx_2h = m.get('st_context_2h')
    change_ts = m.get('last_bias_2h_change_ts')
    if bias_2h not in ('bull', 'bear') or not change_ts:
        return None

    exp_ctx = 'buy' if bias_2h == 'bull' else 'sell'
    if ctx_2h != exp_ctx:
        return None

    # Tolere l'ordre d'arrivee entre le scheduler Bias 2H et TradingView Context 2H.
    if time.time() - float(change_ts) > 3 * 3600:
        return None

    event_id = f"ctx2h_bias2h:{symbol}:{bias_2h}:{int(float(change_ts))}"
    if not should_send(symbol, 'ctx2h_bias2h', cooldown=6 * 3600, event_id=event_id):
        return None

    direction = 'LONG' if bias_2h == 'bull' else 'SHORT'
    emoji = '\U0001f7e2' if direction == 'LONG' else '\U0001f534'
    msg = (
        f"{emoji} <b>[INFO - CTX 2H + BIAS 2H]</b> {symbol}\n"
        f"━━━━━━━━━━\n"
        f"📈 Direction: {direction}\n"
        f"💰 Price: ${format_price(price)}\n"
        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"✅ ST Context 2H: {ctx_2h.upper()}\n"
        f"✅ Bias 2H: {bias_2h.upper()} (changement)"
    )
    log_msg = f"[INFO] Context 2H + Bias 2H change: {symbol} {direction}"
    return msg, log_msg

def build_ctx2h_stai2h_alert(symbol, price=None):
    m = MOMENTUM_STATE.get(symbol, {})
    st_ai_2h = m.get('st_ai_2h')
    ctx_2h = m.get('st_context_2h')
    flip_ts = m.get('last_st_ai_2h_flip_ts')
    if st_ai_2h not in ('buy', 'sell') or not flip_ts:
        return None

    if ctx_2h != st_ai_2h:
        return None

    # Tolere l'ordre d'arrivee entre le flip ST AI 2H et TradingView Context 2H.
    if time.time() - float(flip_ts) > 3 * 3600:
        return None

    event_id = f"ctx2h_stai2h:{symbol}:{st_ai_2h}:{int(float(flip_ts))}"
    if not should_send(symbol, 'ctx2h_stai2h', cooldown=6 * 3600, event_id=event_id):
        return None

    direction = 'LONG' if st_ai_2h == 'buy' else 'SHORT'
    emoji = '\U0001f7e2' if direction == 'LONG' else '\U0001f534'
    msg = (
        f"{emoji} <b>[INFO - CTX 2H + ST AI 2H]</b> {symbol}\n"
        f"━━━━━━━━━━\n"
        f"📈 Direction: {direction}\n"
        f"💰 Price: ${format_price(price)}\n"
        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"✅ ST Context 2H: {ctx_2h.upper()}\n"
        f"✅ Flip ST AI 2H: {st_ai_2h.upper()}"
    )
    log_msg = f"[INFO] Context 2H + Flip ST AI 2H: {symbol} {direction}"
    return msg, log_msg

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
            {"text": "Activer pyramiding", "callback_data": f"pyra_on:{callback_key}"},
            {"text": "Ignorer",            "callback_data": f"pyra_off:{callback_key}"}
        ], [
            {"text": "Scalp OFF",          "callback_data": "scalp_off"}
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
    tf_aliases = {'1': '1m', '1min': '1m', '1minute': '1m', '5': '5m', '5min': '5m', '5minute': '5m', '15': '15m', '60': '1h', '120': '2h', '2hr': '2h', '2hour': '2h', '180': '3h', '3hr': '3h', '3hour': '3h', '240': '4h', '4hr': '4h', '4hour': '4h'}
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
    flipped_15m  = False  # calculé ci-dessous si supertrend 15m
    state_changed = False  # True si état modifié → persist à la fin
    if alert_type == 'supertrend':
        parsed = parse_st_value(val)
        if parsed is None:
            logger.warning(f"[WEBHOOK] SuperTrend invalide: {symbol} tf={tf} value={val!r}")
            return jsonify({'status': 'ignored', 'reason': 'invalid_supertrend'}), 200

        if tf == '15m':
            # Point 2 : calculer flip avant mise à jour
            prev_15m    = m.get('st_ai_15m')
            m['st_ai_15m'] = parsed
            state_changed = True
            flipped_15m = (prev_15m is not None and parsed is not None and parsed != prev_15m)
            if flipped_15m:
                m['last_st_15m'] = prev_15m  # garder pour guard pyramiding

        elif tf == '1h':
            # Point 1 : 1H intégré dans le bloc supertrend
            prev_1h      = m.get('st_ai_1h')
            m['st_ai_1h']   = parsed
            m['last_st_1h'] = prev_1h
            state_changed = True
            flipped_1h   = (prev_1h is not None and parsed is not None and parsed != prev_1h)
        elif tf == '2h':
            prev_2h = m.get('st_ai_2h')
            m['st_ai_2h'] = parsed
            m['last_st_2h'] = prev_2h
            state_changed = True
            flipped_2h = (prev_2h is not None and parsed is not None and parsed != prev_2h)
            if flipped_2h:
                m['last_st_ai_2h_flip_ts'] = time.time()
                alert = build_ctx2h_stai2h_alert(symbol, price)
                if alert:
                    msg, log_msg = alert
                    send_telegram(msg)
                    logger.info(log_msg)
        elif tf == '4h':
            prev_4h = m.get('st_ai_4h')
            m['st_ai_4h'] = parsed
            state_changed = True
            m['st_4h_flipped'] = bool(prev_4h and parsed and parsed != prev_4h)
            if m['st_4h_flipped'] and prev_4h:
                m['last_st_4h'] = prev_4h
            state_changed = True


    elif alert_type == 'bias':
        bias_val = str(val).lower() if val else None
        if bias_val in ('bull', 'bear', 'neutral') and tf == '1h':
            prev_bias = m.get('bias_1h')
            new_bias_val = bias_val if bias_val != 'neutral' else None
            m['bias_1h'] = new_bias_val
            state_changed = True
        elif bias_val in ('bull', 'bear', 'neutral') and tf == '15m':
            m['bias_15m'] = bias_val if bias_val != 'neutral' else None
            state_changed = True
            
    elif alert_type == 'st_context_lt' and tf in ('1m', '5m'):
        try:
            lt_val = float(val)
            lt_parsed = 'buy' if lt_val < -1.96 else 'sell' if lt_val > 1.96 else None
        except (TypeError, ValueError):
            logger.warning(f"[WEBHOOK] ST Context LT invalide: {symbol} tf={tf} value={val!r}")
            return jsonify({'status': 'ignored', 'reason': 'invalid_st_context_lt'}), 200
        if tf == '1m':
            m['st_context_lt_1m'] = lt_parsed
            m['st_context_lt_1m_ts'] = time.time()
        else:
            m['st_context_lt_5m'] = lt_parsed
        state_changed = True

    elif alert_type == 'st_context':
        try:
            ctx_val = float(val)
            ctx_parsed = 'buy' if ctx_val < -1.96 else 'sell' if ctx_val > 1.96 else None
        except (TypeError, ValueError):
            logger.warning(f"[WEBHOOK] ST Context invalide: {symbol} tf={tf} value={val!r}")
            return jsonify({'status': 'ignored', 'reason': 'invalid_st_context'}), 200
        if tf == '1m':
            m['st_context_1m'] = ctx_parsed
            m['st_context_1m_ts'] = time.time()
            state_changed = True
        elif tf == '5m':
            m['st_context_5m'] = ctx_parsed
            state_changed = True
        elif tf == '15m':
            m['st_context_15m'] = ctx_parsed
            state_changed = True
        elif tf == '1h':
            m['st_context_1h'] = ctx_parsed
            state_changed = True
        elif tf == '2h':
            m['st_context_2h'] = ctx_parsed
            state_changed = True
            alert = build_ctx2h_bias2h_alert(symbol, price)
            if alert:
                msg, log_msg = alert
                send_telegram(msg)
                logger.info(log_msg)
            alert = build_ctx2h_stai2h_alert(symbol, price)
            if alert:
                msg, log_msg = alert
                send_telegram(msg)
                logger.info(log_msg)

    # ?? Logique SCALP ?????????????????????????????????????????????????
    # ENTREE : Zone ST Context 1m + ST AI 1H + Bias 15m
    # Anti-chop : ST Context LT 1m dans le meme sens => bloque

    if alert_type in ('st_context', 'st_context_lt', 'supertrend', 'bias') and tf in ('1m', '1h', '15m'):
        st_1h = m.get('st_ai_1h')
        bias_15m = m.get('bias_15m')
        ctx_1m = m.get('st_context_1m')
        ctx_lt_1m = m.get('st_context_lt_1m')
        ctx_1m_fresh = bool(ctx_1m) and is_fresh(m.get('st_context_1m_ts'), 5 * 60)
        ctx_lt_1m_fresh = bool(ctx_lt_1m) and is_fresh(m.get('st_context_lt_1m_ts'), 5 * 60)

        should_evaluate = ctx_1m is not None and ctx_1m_fresh
        if not should_evaluate:
            if state_changed:
                persist_state()
            return jsonify({'status': 'ok'}), 200

        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] Signal ignore: {symbol}")
            if state_changed:
                persist_state()
            return jsonify({'status': 'ok', 'enabled': False}), 200

        signal_direction = 'LONG' if ctx_1m == 'buy' else 'SHORT'
        exp_st_1h = 'buy' if signal_direction == 'LONG' else 'sell'
        exp_bias = 'bull' if signal_direction == 'LONG' else 'bear'
        exp_ctx = 'buy' if signal_direction == 'LONG' else 'sell'

        st_1h_ok = st_1h == exp_st_1h
        bias_15m_ok = bias_15m == exp_bias
        ctx_1m_ok = ctx_1m == exp_ctx
        antichop_blocked = ctx_lt_1m_fresh and ctx_lt_1m == exp_ctx
        all_ok = st_1h_ok and bias_15m_ok and ctx_1m_ok and not antichop_blocked

        pos_key = f"{symbol}_SCALP"
        is_pyra = False
        with STATE_LOCK:
            pos = SCALP_POSITIONS.get(pos_key)
            if pos and pos['direction'] != signal_direction:
                SCALP_POSITIONS.pop(pos_key, None)
                PYRA_ENABLED.pop(pos_key, None)
                pos = None

            candidate = bool(all_ok and pos is None)
            if candidate and should_send(symbol, f"scalp_entry_{exp_ctx}", event_id=event_id, cooldown=1800):
                SCALP_POSITIONS[pos_key] = {'direction': signal_direction, 'entry_count': 1}
                PYRA_ENABLED.pop(pos_key, None)
                pos = SCALP_POSITIONS[pos_key]
                is_entry = True
            else:
                is_entry = False
                # Pyramiding : position deja ouverte + nouvelle zone ST Context 1m dans le meme sens
                if (pos and pos['direction'] == signal_direction and st_1h_ok and bias_15m_ok and ctx_1m_ok
                        and not antichop_blocked and PYRA_ENABLED.get(pos_key, False)
                        and should_send(symbol, f"scalp_pyra_{exp_ctx}", event_id=event_id, cooldown=CONFIG['PYRA_COOLDOWN'])):
                    pos['entry_count'] += 1
                    is_pyra = True
                if not all_ok and not is_pyra:
                    logger.info(
                        f"[SCALP BLOCKED] {symbol} dir={signal_direction} "
                        f"st1h={st_1h}/{exp_st_1h} bias15m={bias_15m}/{exp_bias} "
                        f"ctx1m={ctx_1m}/{exp_ctx} ctx1m_fresh={ctx_1m_fresh} "
                        f"lt1m={ctx_lt_1m} lt1m_fresh={ctx_lt_1m_fresh} antichop={antichop_blocked} "
                        f"pos={pos['direction'] if pos else None}"
                    )

        if is_entry and pos:
            emoji = "\U0001f7e2" if signal_direction == "LONG" else "\U0001f534"
            tg_sent = send_telegram_with_buttons(
                f"{emoji} <b>[SCALP - ENTREE]</b> {symbol}\n"
                f"--------------------\n"
                f"Direction: {signal_direction}\n"
                f"Price: ${format_price(price)}\n"
                f"Exchange: OKX\n"
                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"[OK] ST AI 1H: {(st_1h or 'N/A').upper()}\n"
                f"[OK] Bias 15m: {(bias_15m or 'N/A').upper()}\n"
                f"[OK] Zone ST Context 1m: {(ctx_1m or 'N/A').upper()}\n"
                f"[ANTI-CHOP] LT 1m: {(ctx_lt_1m or 'NEUTRE').upper()}",
                pos_key
            )
            if not tg_sent:
                logger.warning(f"[SCALP] Entree {symbol} creee mais notification Telegram echouee")
            logger.info(f"[SCALP] Entree: {symbol} {signal_direction}")
            state_changed = True

        elif is_pyra and pos:
            emoji = "\U0001f7e2" if signal_direction == "LONG" else "\U0001f534"
            send_telegram(
                f"{emoji} <b>[SCALP - PYRAMIDING #{pos['entry_count']}]</b> {symbol}\n"
                f"--------------------\n"
                f"Direction: {signal_direction}\n"
                f"Price: ${format_price(price)}\n"
                f"Exchange: OKX\n"
                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"[OK] Nouvelle zone ST Context 1m: {(ctx_1m or 'N/A').upper()}"
            )
            logger.info(f"[SCALP] Pyramiding #{pos['entry_count']}: {symbol} {signal_direction}")
            state_changed = True

    # Persister si état modifié
    if state_changed:
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

        if cb_data == 'scalp_off':
            global SCALP_ENABLED
            with STATE_LOCK:
                SCALP_ENABLED = False
            persist_state()
            logger.info(f"[SCALP] Desactive par Telegram ({user})")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "Scalp OFF", "callback_data": "noop"},
                                       {"text": "Scalp ON", "callback_data": "scalp_on"}
                                   ]]}}, timeout=5)

        elif cb_data == 'scalp_on':
            with STATE_LOCK:
                SCALP_ENABLED = True
            persist_state()
            logger.info(f"[SCALP] Active par Telegram ({user})")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "Scalp ON", "callback_data": "noop"},
                                       {"text": "Scalp OFF", "callback_data": "scalp_off"}
                                   ]]}}, timeout=5)

        elif cb_data.startswith('pyra_on:'):
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
        'enabled': SCALP_ENABLED,
        'assets': len(CONFIG['SYMBOLS']),
        'positions': len(SCALP_POSITIONS),
    })

@app.route('/scalp_status', methods=['GET'])
def scalp_status():
    return jsonify({
        'status': 'ok',
        'enabled': SCALP_ENABLED,
        'positions': len(SCALP_POSITIONS),
        'assets': len(CONFIG['SYMBOLS']),
    })

@app.route('/scalp_on', methods=['POST'])
def scalp_on():
    secret = os.environ.get('ADMIN_SECRET', '')
    if not secret or request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    global SCALP_ENABLED
    with STATE_LOCK:
        SCALP_ENABLED = True
    persist_state()
    return jsonify({'status': 'ok', 'enabled': True}), 200

@app.route('/scalp_off', methods=['POST'])
def scalp_off():
    secret = os.environ.get('ADMIN_SECRET', '')
    if not secret or request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    global SCALP_ENABLED
    with STATE_LOCK:
        SCALP_ENABLED = False
    persist_state()
    return jsonify({'status': 'ok', 'enabled': False}), 200

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
    if main_url and not main_url.startswith(('https://', 'http://')):
        main_url = f'https://{main_url}'
    admin_secret = os.environ.get('ADMIN_SECRET', '')
    if main_url and admin_secret:
        def _sync_after_reset():
            time.sleep(1)
            try:
                resp = requests.post(
                    f'{main_url}/sync_scalp',
                    headers={'X-Admin-Secret': admin_secret},
                    timeout=15,
                )
                if not 200 <= resp.status_code < 300:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                data   = resp.json()
                sent   = data.get('sent', [])
                errors = data.get('errors', [])
                logger.info(f"[RESET] sync_scalp: {len(sent)} assets, erreurs: {len(errors)}")
                if errors:
                    logger.warning(f"[RESET] sync_scalp erreurs: {errors}")
            except Exception as e:
                logger.warning(f"[RESET] sync_scalp échoué: {e}")
        threading.Thread(target=_sync_after_reset, daemon=True).start()

    return jsonify({'status': 'reset'}), 200

# ============================================================================
# D脡MARRAGE
# ============================================================================

def startup():
    init_redis()
    load_state()

    tok      = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
    base_url = os.environ.get('SCALP_PUBLIC_URL', '').rstrip('/')
    if base_url and not base_url.startswith(('https://', 'http://')):
        base_url = f'https://{base_url}'
    if tok and base_url:
        try:
            wh_url    = f"{base_url}/telegram_callback"
            wh_payload = {'url': wh_url}
            tg_secret  = os.environ.get('SCALP_TELEGRAM_SECRET', '')
            if tg_secret:
                wh_payload['secret_token'] = tg_secret
            resp_wh = requests.post(f"https://api.telegram.org/bot{tok}/setWebhook",
                         json=wh_payload, timeout=10)
            if resp_wh.status_code == 200 and resp_wh.json().get('ok'):
                logger.info(f"✅ Telegram webhook configuré: {wh_url}")
            else:
                logger.warning(f"⚠️ Telegram webhook erreur: {resp_wh.text[:100]}")
        except Exception as e:
            logger.warning(f"⚠️ Webhook setup: {e}")

    # Démarrer le scheduler Bias 15m
    bias_thread = threading.Thread(target=update_bias_15m, daemon=True)
    bias_thread.start()

    # Sync état 4H depuis bot principal au démarrage
    main_url     = os.environ.get('MAIN_BOT_URL', '').rstrip('/')
    if main_url and not main_url.startswith(('https://', 'http://')):
        main_url = f'https://{main_url}'
    admin_secret = os.environ.get('ADMIN_SECRET', '')
    if main_url and admin_secret:
        def _sync():
            import time as _time
            _time.sleep(5)  # laisser le bot principal répondre
            try:
                resp = requests.post(
                    f'{main_url}/sync_scalp',
                    headers={'X-Admin-Secret': admin_secret},
                    timeout=15
                )
                if not 200 <= resp.status_code < 300:
                    raise RuntimeError(f"sync_scalp HTTP {resp.status_code}: {resp.text[:200]}")
                data   = resp.json()
                sent   = data.get('sent', [])
                errors = data.get('errors', [])
                logger.info(f"[STARTUP] sync_scalp: {len(sent)} assets, erreurs: {len(errors)}")
                if errors:
                    logger.warning(f"[STARTUP] sync_scalp erreurs: {errors}")
            except Exception as e:
                logger.warning(f'[STARTUP] sync_scalp échoué: {e}')
        threading.Thread(target=_sync, daemon=True).start()

    send_telegram(
        "🚀 <b>Scalping Bot démarré</b>\n"
        f"━━━━━━━━━━\n"
        f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"⚙️ Stratégie: ST AI 1H + Bias 15m + Zone Context 1m\n"
        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}"
    )

if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    t = threading.Thread(target=startup, daemon=True)
    t.start()
