"""
SCALP BOT v3 — Stratégie A
Stratégie A : Bias 4H + ST Context 15min + flip ST AI 15min
Pyramiding illimité, SL swing low -> break even
"""

import os, time, json, logging, requests, threading
from datetime import datetime, timezone, timedelta

import ccxt
import pandas as pd
import numpy as np
import redis
from flask import Flask, jsonify, request

# ============================================================================ #
# CONFIG
# ============================================================================ #

CONFIG = {
    'SYMBOLS': [
        'AAVE/USDT',
        'APT/USDT',
        'ARB/USDT',
        'ATOM/USDT',
        'AVAX/USDT',
        'AXS/USDT',
        'BNB/USDT',
        'BONK/USDT',
        'BTC/USDT',
        'CRV/USDT',
        'CVX/USDT',
        'DOGE/USDT',
        'DOT/USDT',
        'ENA/USDT',
        'ETH/USDT',
        'FET/USDT',
        'FIL/USDT',
        'FLOKI/USDT',
        'GALA/USDT',
        'HBAR/USDT',
        'HYPE/USDT',
        'IMX/USDT',
        'INJ/USDT',
        'JTO/USDT',
        'JUP/USDT',
        'LINK/USDT',
        'LTC/USDT',
        'MANA/USDT',
        'MOVE/USDT',
        'NEAR/USDT',
        'ONDO/USDT',
        'OP/USDT',
        'PENDLE/USDT',
        'PENGU/USDT',
        'PEPE/USDT',
        'PYTH/USDT',
        'RAY/USDT',
        'RENDER/USDT',
        'SAND/USDT',
        'SEI/USDT',
        'SOL/USDT',
        'STX/USDT',
        'SUI/USDT',
        'TIA/USDT',
        'TON/USDT',
        'VIRTUAL/USDT',
        'WIF/USDT',
        'WLD/USDT',
        'XRP/USDT',
        'ZK/USDT',
        'ZRO/USDT',
        'UNI/USDT',
        'SHIB/USDT',
        'GRT/USDT',
        'ENJ/USDT',
        'APE/USDT',
        'CORE/USDT',
        'TURBO/USDT',
        'MEW/USDT',
        'NEIRO/USDT',
        'STRK/USDT',
        'BERA/USDT',
        'SONIC/USDT',
    ],
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID':   os.environ.get('TELEGRAM_CHAT_ID', ''),
    'TELEGRAM_WEBHOOK_SECRET': os.environ.get('TELEGRAM_WEBHOOK_SECRET', ''),
    'BIAS_EMA_LEN':  13,
    'BIAS_SMA_LEN':  30,
    'SWING_LOOKBACK': 5,
    'MACD_FAST':     12,
    'MACD_SLOW':     26,
    'MACD_SIGNAL':   9,
    'PORT':          int(os.environ.get('PORT', 5001)),
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

POSITIONS: dict = {}
PREP_BUFFER: list = []
SCAN_STATE:  dict = {}
ST_CONTEXT_15M: dict = {}  # symbol -> 'buy' | 'sell' | None
ST_AI_15M: dict = {}       # symbol -> 'buy' | 'sell' | None (recu via webhook TradingView)
CONFIRMED_TRADES: dict = {}  # pos_key -> True si entré manuellement
STATE_LOCK = threading.Lock()
LAST_SCAN_TIME = None
LAST_ALERT_TS: dict = {}  # pos_key -> timestamp dernière alerte envoyée
SCAN_IN_PROGRESS = False  # flag anti-scan concurrent
ALERT_COOLDOWN = 900  # 15min minimum entre deux alertes pour le même asset/strat
_LAST_PERSIST_TS = 0.0  # timestamp du dernier persist_weekly_state
REDIS_CLIENT = None

# Stats hebdomadaires — créneaux horaires (heure Taiwan UTC+8)
# clé: "HH" (ex: "14") -> {'LONG': int, 'SHORT': int}
HOURLY_STATS: dict = {}
WEEKLY_START: datetime = datetime.now(timezone.utc)

# ============================================================================ #
# REDIS
# ============================================================================ #

def init_redis():
    global REDIS_CLIENT
    url = os.environ.get('REDIS_URL')
    if not url:
        logger.warning('REDIS_URL non defini')
        return
    try:
        REDIS_CLIENT = redis.from_url(url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info('Redis connecte')
    except Exception as e:
        logger.error('Redis erreur: ' + str(e))
        REDIS_CLIENT = None

def audit_log(entry):
    if REDIS_CLIENT:
        try:
            REDIS_CLIENT.lpush('scalp_audit_v3', json.dumps(entry))
            REDIS_CLIENT.ltrim('scalp_audit_v3', 0, 999)
        except Exception as e:
            logger.error('Redis audit: ' + str(e))

def persist_weekly_state(force=False):
    global _LAST_PERSIST_TS
    if not REDIS_CLIENT:
        return
    now = time.time()
    if not force and now - _LAST_PERSIST_TS < 60:
        return  # throttle: max 1 persist/60s
    _LAST_PERSIST_TS = now
    try:
        with STATE_LOCK:
            ctx_snapshot = dict(ST_CONTEXT_15M)
        with STATE_LOCK:
            st_ai_snapshot = dict(ST_AI_15M)
        payload = {
            'hourly_stats':   HOURLY_STATS,
            'weekly_start':   WEEKLY_START.isoformat(),
            'st_context_15m': ctx_snapshot,
            'st_ai_15m':      st_ai_snapshot,
        }
        REDIS_CLIENT.set('scalp_weekly_state', json.dumps(payload))
    except Exception as e:
        logger.error('Redis persist weekly: ' + str(e))

def load_weekly_state():
    global HOURLY_STATS, WEEKLY_START
    if not REDIS_CLIENT:
        return
    try:
        raw = REDIS_CLIENT.get('scalp_weekly_state')
        if not raw:
            return
        payload = json.loads(raw)
        HOURLY_STATS = payload.get('hourly_stats', {})
        ws = payload.get('weekly_start')
        if ws:
            WEEKLY_START = datetime.fromisoformat(ws)
        ctx = payload.get('st_context_15m', {})
        st_ai = payload.get('st_ai_15m', {})
        with STATE_LOCK:
            ST_CONTEXT_15M.update(ctx)
            ST_AI_15M.update(st_ai)
        logger.info(
            'Stats hebdo restaurees depuis Redis | créneaux=' + str(len(HOURLY_STATS))
            + ' | ctx_15m=' + str(len(ST_CONTEXT_15M)) + ' assets'
        )
    except Exception as e:
        logger.error('Redis load weekly: ' + str(e))

def persist_positions():
    if not REDIS_CLIENT:
        return
    try:
        with STATE_LOCK:
            pos_snapshot = dict(POSITIONS)
            conf_snapshot = dict(CONFIRMED_TRADES)
        REDIS_CLIENT.set('scalp_positions', json.dumps({
            'positions': pos_snapshot,
            'confirmed': conf_snapshot,
        }))
    except Exception as e:
        logger.error('Redis persist positions: ' + str(e))

def load_positions():
    if not REDIS_CLIENT:
        return
    try:
        raw = REDIS_CLIENT.get('scalp_positions')
        if not raw:
            return
        payload = json.loads(raw)
        # Rétrocompatibilité: ancien format était juste le dict positions
        if isinstance(payload, dict) and 'positions' in payload:
            pos = payload.get('positions', {})
            conf = payload.get('confirmed', {})
        else:
            pos = payload
            conf = {}
        with STATE_LOCK:
            POSITIONS.update(pos)
            CONFIRMED_TRADES.update(conf)
        logger.info('Positions restaurees depuis Redis: ' + str(len(POSITIONS)) + ' positions | ' + str(len(CONFIRMED_TRADES)) + ' confirmees')
    except Exception as e:
        logger.error('Redis load positions: ' + str(e))

def track_signal_hour(signal):
    """Incrémente le compteur du créneau horaire courant (heure Taiwan UTC+8)."""
    tw_hour = datetime.now(timezone(timedelta(hours=8))).strftime('%H')
    with STATE_LOCK:
        if tw_hour not in HOURLY_STATS:
            HOURLY_STATS[tw_hour] = {'LONG': 0, 'SHORT': 0}
        HOURLY_STATS[tw_hour][signal] = HOURLY_STATS[tw_hour].get(signal, 0) + 1

def should_send_alert(pos_key):
    """Retourne True si aucune alerte n'a été envoyée pour ce pos_key dans les 15 dernières minutes."""
    now = time.time()
    with STATE_LOCK:
        last = LAST_ALERT_TS.get(pos_key, 0)
        if now - last >= ALERT_COOLDOWN:
            LAST_ALERT_TS[pos_key] = now
            return True
    return False

# ============================================================================ #
# EXCHANGE
# ============================================================================ #

exchange = ccxt.okx({'enableRateLimit': True})

def parse_st_context_value(val, trend_level=1.96):
    """
    Convertit la valeur brute du ST Context (plot_1 = Short time context) en 'buy', 'sell' ou None.
    Accepte les strings 'buy'/'sell' (rétrocompatibilité) et les valeurs numériques
    envoyées par TradingView via {{plot_1}}.
      plot_1 > +trend_level  → zone baissière → 'sell'
      plot_1 < -trend_level  → zone haussière → 'buy'
      entre les deux         → neutre         → None
    """
    s = str(val).strip().lower()
    if s in ('buy', 'sell'):
        return s
    if s in ('', 'none', 'null', 'neutral', 'na', 'n/a', 'nan'):
        return None
    try:
        fval = float(s)
        if fval > trend_level:    return 'sell'
        elif fval < -trend_level: return 'buy'
        else:                     return None
    except (ValueError, TypeError):
        logger.warning('[WARN] ST Context valeur invalide: \'' + str(val) + '\'')
        return None

def parse_supertrend_value(val):
    """Convertit la valeur brute du SuperTrend AI en 'buy' ou 'sell'.
    Accepte 'buy'/'sell' (ancien format) et '1'/'0' (nouveau format via {{plot_2}}).
    """
    s = str(val).strip().lower()
    if s == 'buy'  or s == '1': return 'buy'
    if s == 'sell' or s == '0': return 'sell'
    try:
        return 'buy' if float(s) >= 0.5 else 'sell'
    except (ValueError, TypeError):
        logger.warning('[WARN] SuperTrend valeur invalide: '' + str(val) + ''')
        return None

def fetch_ohlcv(symbol, timeframe, limit=250):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df  = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        return df
    except Exception as e:
        logger.error('fetch_ohlcv ' + symbol + ' ' + timeframe + ': ' + str(e))
        return None

# ============================================================================ #
# INDICATEURS
# ============================================================================ #

def calc_bias(df, ema_len=13, sma_len=30):
    close   = df['close']
    ema_val = close.ewm(span=ema_len, adjust=False).mean().iloc[-1]
    sma_val = close.rolling(window=sma_len).mean().iloc[-1]
    return 'bull' if ema_val > sma_val else 'bear'

def calc_macd_histogram(df, fast=12, slow=26, signal=9, candle=-2):
    close       = df['close']
    ema_fast    = close.ewm(span=fast,   adjust=False).mean()
    ema_slow    = close.ewm(span=slow,   adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram   = macd_line - signal_line
    return float(histogram.iloc[candle])


def supertrend_ai(df, atr_len=6, min_mult=1.0, max_mult=2.0, step=1.0,
                  perf_alpha=100, from_cluster='Best', max_iter=100):
    high  = df['high'].values
    low   = df['low'].values
    close = df['close'].values
    n     = len(close)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    atr = pd.Series(tr).ewm(alpha=1/atr_len, adjust=False).mean().values
    hl2 = (high + low) / 2.0
    factors, f = [], min_mult
    while f <= max_mult + 1e-9:
        factors.append(round(f, 10))
        f += step
    nf = len(factors)
    upper_arr  = np.full((n, nf), hl2[0])
    lower_arr  = np.full((n, nf), hl2[0])
    trend_arr  = np.zeros((n, nf), dtype=int)
    output_arr = np.full((n, nf), hl2[0])
    perf_arr   = np.zeros((n, nf))
    alpha_perf = 2.0 / (perf_alpha + 1)
    for i in range(1, n):
        for k, factor in enumerate(factors):
            up = hl2[i] + atr[i] * factor
            dn = hl2[i] - atr[i] * factor
            if close[i] > upper_arr[i-1, k]:   trend_arr[i, k] = 1
            elif close[i] < lower_arr[i-1, k]: trend_arr[i, k] = 0
            else:                               trend_arr[i, k] = trend_arr[i-1, k]
            upper_arr[i, k] = min(up, upper_arr[i-1, k]) if close[i-1] < upper_arr[i-1, k] else up
            lower_arr[i, k] = max(dn, lower_arr[i-1, k]) if close[i-1] > lower_arr[i-1, k] else dn
            output_arr[i, k] = lower_arr[i, k] if trend_arr[i, k] == 1 else upper_arr[i, k]
            diff = np.sign(close[i-1] - output_arr[i-1, k]) if output_arr[i-1, k] != 0 else 0
            perf_arr[i, k] = perf_arr[i-1, k] + alpha_perf * ((close[i] - close[i-1]) * diff - perf_arr[i-1, k])
    perf_final   = perf_arr[-1]
    factor_final = np.array(factors)
    centroids    = np.percentile(perf_final, [25, 50, 75])
    clusters_p = [[], [], []]
    clusters_f = [[], [], []]
    for _ in range(max_iter):
        clusters_p = [[], [], []]
        clusters_f = [[], [], []]
        for j, val in enumerate(perf_final):
            idx = int(np.argmin([abs(val - c) for c in centroids]))
            clusters_p[idx].append(val)
            clusters_f[idx].append(factor_final[j])
        new_c = [np.mean(cp) if cp else 0.0 for cp in clusters_p]
        if np.max(np.abs(np.array(new_c) - centroids)) < 0.0001:
            centroids = np.array(new_c)
            break
        centroids = np.array(new_c)
    from_idx = {'Best': 2, 'Average': 1, 'Worst': 0}.get(from_cluster, 2)
    sorted_idx = np.argsort(centroids)
    target_idx = sorted_idx[from_idx]
    target_factor = np.mean(clusters_f[target_idx]) if clusters_f[target_idx] else factors[0]
    upper_f = lower_f = hl2[0]
    os_f = 0
    direction = pd.Series('', index=df.index, dtype=str)
    for i in range(1, n):
        up = hl2[i] + atr[i] * target_factor
        dn = hl2[i] - atr[i] * target_factor
        upper_f = min(up, upper_f) if close[i-1] < upper_f else up
        lower_f = max(dn, lower_f) if close[i-1] > lower_f else dn
        if close[i] > upper_f:   os_f = 1
        elif close[i] < lower_f: os_f = 0
        direction.iloc[i] = 'buy' if os_f == 1 else 'sell'
    return direction
def get_swing_low(df, lookback=5):
    return float(df['low'].iloc[-lookback-2:-2].min())

def get_swing_high(df, lookback=5):
    return float(df['high'].iloc[-lookback-2:-2].max())

def calc_sl(direction, df_15m, avg_price, entry_count):
    if entry_count <= 1:
        return get_swing_low(df_15m, CONFIG['SWING_LOOKBACK']) if direction == 'LONG' else get_swing_high(df_15m, CONFIG['SWING_LOOKBACK'])
    return avg_price

# ============================================================================ #
# TELEGRAM
# ============================================================================ #

def send_telegram(msg):
    if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHAT_ID']:
        logger.warning('Telegram non configuré (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID manquants)')
        return
    url     = 'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/sendMessage'
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info('Telegram envoye')
        elif resp.status_code == 429:
            retry = resp.json().get('parameters', {}).get('retry_after', 30)
            time.sleep(retry)
            requests.post(url, json=payload, timeout=10)
        else:
            logger.error('Telegram HTTP ' + str(resp.status_code) + ': ' + resp.text)
    except Exception as e:
        logger.error('Telegram: ' + str(e))

def send_telegram_with_button(msg, callback_data, button_label='✅ Entré en trade'):
    """Envoie un message Telegram avec un bouton inline."""
    if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHAT_ID']:
        logger.warning('Telegram non configuré (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID manquants)')
        return
    url = 'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/sendMessage'
    payload = {
        'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
        'text': msg,
        'parse_mode': 'HTML',
        'reply_markup': {
            'inline_keyboard': [[
                {'text': button_label, 'callback_data': callback_data}
            ]]
        }
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info('Telegram envoye (avec bouton)')
        elif resp.status_code == 429:
            retry = resp.json().get('parameters', {}).get('retry_after', 30)
            time.sleep(retry)
            requests.post(url, json=payload, timeout=10)
        else:
            logger.error('Telegram HTTP ' + str(resp.status_code))
    except Exception as e:
        logger.error('Telegram button: ' + str(e))

def answer_callback_query(callback_query_id, text='✅ Position confirmée !'):
    """Répond au callback Telegram pour effacer le spinner."""
    url = 'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/answerCallbackQuery'
    try:
        requests.post(url, json={'callback_query_id': callback_query_id, 'text': text}, timeout=5)
    except Exception:
        pass

def is_authorized_telegram_callback(callback_query):
    """
    Vérifie qu'un callback Telegram provient du chat autorisé et, si configuré,
    que le secret token du webhook est valide.
    """
    secret = CONFIG.get('TELEGRAM_WEBHOOK_SECRET')
    if secret:
        header_secret = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if header_secret != secret:
            return False
    expected_chat_id = str(CONFIG.get('TELEGRAM_CHAT_ID', '')).strip()
    callback_chat_id = str(callback_query.get('message', {}).get('chat', {}).get('id', '')).strip()
    if expected_chat_id and callback_chat_id and callback_chat_id != expected_chat_id:
        return False
    return True

def format_entry_msg(symbol, direction, price, avg_price, sl, entry_count, strat, bias_4h=None, bias_1h=None, bias_15m=None, ctx_15m=None, macd_15m=None):
    emoji = '\U0001f7e2' if direction == 'LONG' else '\U0001f534'
    sl_label = 'Swing low/high' if entry_count == 1 else 'Break even'
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    lines = [
        emoji + ' <b>[SCALP ' + direction + ' #' + str(entry_count) + ' | STRAT ' + strat + '] ' + symbol + '</b>',
        '\u2501' * 20,
        '\U0001f4b0 Prix entree: $' + str(round(price, 6)),
    ]
    if entry_count > 1:
        lines.append('\U0001f4ca Prix moyen: $' + str(round(avg_price, 6)))
    lines += [
        '\U0001f6d1 SL (' + sl_label + '): $' + str(round(sl, 6)),
        '\U0001f3e6 Exchange: OKX',
        '\u23f0 ' + now,
        '\u2501' * 20,
    ]
    if bias_4h:
        e4 = '\U0001f7e2' if bias_4h == 'bull' else '\U0001f534'
        lines.append(e4 + ' Bias 4H: ' + bias_4h.upper())
    if bias_1h:
        e1 = '\U0001f7e2' if bias_1h == 'bull' else '\U0001f534'
        lines.append(e1 + ' Bias 1H: ' + bias_1h.upper())
    if bias_15m:
        e15 = '\U0001f7e2' if bias_15m == 'bull' else '\U0001f534'
        lines.append(e15 + ' Bias 15m: ' + bias_15m.upper())
    if ctx_15m:
        ec = '\U0001f7e2' if ctx_15m == 'buy' else '\U0001f534'
        lines.append(ec + ' ST Context 15m: ' + ctx_15m.upper())
    if macd_15m is not None:
        m15_str = ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4))
        lines.append('\U0001f4ca MACD 15min: ' + m15_str)
    lines.append('\u2705 ST AI 15min: flip ' + direction.lower())
    if entry_count > 1:
        lines.append('\U0001f4c8 Positions accumulees: ' + str(entry_count))
    return '\n'.join(lines)

def format_prep_msg(symbol, direction, price, strat, bias_4h=None, macd_15m=None):
    emoji = '\U0001f7e1' if direction == 'LONG' else '\U0001f7e0'
    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    lines = [
        emoji + ' <b>[PREP ' + direction + ' | STRAT ' + strat + '] ' + symbol + '</b>',
        '\u2501' * 20,
        '\U0001f4b0 Price: $' + str(round(price, 6)),
    ]
    if strat == 'A' and bias_4h:
        e4 = '\U0001f7e2' if bias_4h == 'bull' else '\U0001f534'
        lines.append(e4 + ' Bias 4H: ' + bias_4h.upper())
    if macd_15m is not None:
        m15_str = ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4))
        lines.append('\U0001f4ca MACD 15min: ' + m15_str)
    lines.append('\u23f3 En attente flip ST AI 15min ' + direction)
    return '\n'.join(lines)

def process_symbol(symbol):
    try:
        df_4h  = fetch_ohlcv(symbol, '4h',  limit=200)
        df_1h  = fetch_ohlcv(symbol, '1h',  limit=200)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_4h is None or df_1h is None or df_15m is None: return
        if len(df_4h) < 35 or len(df_1h) < 35 or len(df_15m) < 50: return

        # Indicateurs
        bias_4h   = calc_bias(df_4h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_1h   = calc_bias(df_1h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_15m  = calc_bias(df_15m, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        macd_15m  = calc_macd_histogram(df_15m, CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-2)

        price = float(df_15m['close'].iloc[-1])

        # ST AI 15min et ST Context — reçus via webhook TradingView
        with STATE_LOCK:
            ctx_15m  = ST_CONTEXT_15M.get(symbol)
            curr_15m = ST_AI_15M.get(symbol)

        # Flip détecté via webhook : on compare avec l'état précédent en SCAN_STATE
        prev_st = SCAN_STATE.get(symbol, {}).get('st_15m')
        flip_buy  = curr_15m == 'buy'  and prev_st == 'sell'
        flip_sell = curr_15m == 'sell' and prev_st == 'buy'
        flip      = flip_buy or flip_sell

        # Conditions Strat A: Bias 4H + ST Context 15min zone
        a_long  = bias_4h == 'bull' and ctx_15m == 'buy'
        a_short = bias_4h == 'bear' and ctx_15m == 'sell'

        # Conditions de signal
        # Bias 15m opposé (zone de value)
        b15m_ok_long  = bias_15m == 'bear'  # pour LONG: bias 15m bear
        b15m_ok_short = bias_15m == 'bull'  # pour SHORT: bias 15m bull

        # 1ère entrée: Bias 4H + Context 15m + Bias 15m opposé + flip
        sig_long_1st  = flip_buy  and a_long  and b15m_ok_long
        sig_short_1st = flip_sell and a_short and b15m_ok_short

        # Pyramiding: Bias 4H + Bias 1H + Bias 15m opposé + flip (pas de ctx requis)
        pyra_long_ok  = bias_4h == 'bull' and bias_1h == 'bull' and b15m_ok_long
        pyra_short_ok = bias_4h == 'bear' and bias_1h == 'bear' and b15m_ok_short
        sig_long_pyra  = flip_buy  and pyra_long_ok
        sig_short_pyra = flip_sell and pyra_short_ok

        # Debug log
        reason_a = 'no flip' if not flip else ('LONG A' if (sig_long_1st or sig_long_pyra) else 'SHORT A' if (sig_short_1st or sig_short_pyra) else 'filtre A (B4H=' + bias_4h + ' B1H=' + bias_1h + ' B15m=' + bias_15m + ' CTX=' + str(ctx_15m) + ')')
        logger.info('[SCAN] ' + symbol.ljust(20) + ' B4H=' + bias_4h + ' B1H=' + bias_1h + ' B15m=' + bias_15m + ' CTX15m=' + str(ctx_15m) + ' ST=' + str(curr_15m) + ' flip=' + str(flip) + ' A:' + reason_a)

        # Update scan state
        with STATE_LOCK:
            SCAN_STATE[symbol] = {
                'bias_4h': bias_4h, 'bias_1h': bias_1h, 'bias_15m': bias_15m,
                'ctx_15m': ST_CONTEXT_15M.get(symbol),
                'macd_15m': round(macd_15m, 6),
                'st_15m': curr_15m, 'price': price,
                'ts': datetime.now(timezone.utc).isoformat(),
            }

        now_ts = time.time()

        # Collecte des assets en preparation (rapport groupé toutes les 15min)
        prep_entries = []
        if a_long  and b15m_ok_long  and not flip_buy:
            prep_entries.append({'sym': symbol, 'dir': 'LONG',  'strat': 'A', 'price': price, 'bias_4h': bias_4h, 'macd_15m': macd_15m})
        if a_short and b15m_ok_short and not flip_sell:
            prep_entries.append({'sym': symbol, 'dir': 'SHORT', 'strat': 'A', 'price': price, 'bias_4h': bias_4h, 'macd_15m': macd_15m})
        if prep_entries:
            with STATE_LOCK:
                PREP_BUFFER.extend(prep_entries)

        # Signaux
        for strat, sig_long, sig_short, kw in [
            ('A', sig_long_1st or sig_long_pyra, sig_short_1st or sig_short_pyra,
             {'bias_4h': bias_4h, 'bias_1h': bias_1h, 'bias_15m': bias_15m, 'ctx_15m': ctx_15m, 'macd_15m': macd_15m}),
        ]:
            signal = 'LONG' if sig_long else ('SHORT' if sig_short else None)
            if not signal: continue

            pos_key = symbol + '_' + strat
            with STATE_LOCK:
                pos = POSITIONS.get(pos_key)
                if pos and pos['direction'] != signal:
                    del POSITIONS[pos_key]
                    CONFIRMED_TRADES.pop(pos_key, None)
                    LAST_ALERT_TS.pop(pos_key, None)  # reset cooldown au flip de direction
                    pos = None

                if pos is None:
                    # 1ère entrée : Context 15m obligatoire
                    if not (sig_long_1st or sig_short_1st):
                        continue  # pyramiding sans position existante = ignorer
                    POSITIONS[pos_key] = {
                        'direction': signal,
                        'entries': [{'price': price, 'ts': datetime.now(timezone.utc).isoformat()}],
                        'avg_price': price,
                        'entry_count': 1,
                        'sl': 0,
                    }
                    pos = POSITIONS[pos_key]
                else:
                    # Pyramiding : confirmation manuelle requise + Bias 4H + Bias 1H + Bias 15m opposé
                    if not CONFIRMED_TRADES.get(pos_key):
                        logger.info('[PYRA] ' + symbol + ' non confirmé manuellement, skip')
                        continue
                    pyra_ok = pyra_long_ok if signal == 'LONG' else pyra_short_ok
                    if not pyra_ok:
                        continue
                    # Cooldown entre entrées
                    last_entry_ts = pos['entries'][-1]['ts']
                    last_ts = datetime.fromisoformat(last_entry_ts).timestamp()
                    if now_ts - last_ts < 900:  # 15min minimum entre entrées
                        continue
                    pos['entries'].append({'price': price, 'ts': datetime.now(timezone.utc).isoformat()})
                    pos['entry_count'] += 1
                    pos['avg_price'] = sum(e['price'] for e in pos['entries']) / len(pos['entries'])

                pos['sl'] = calc_sl(signal, df_15m, pos['avg_price'], pos['entry_count'])
                entry_count = pos['entry_count']
                avg_price   = pos['avg_price']
                sl          = pos['sl']

            if not should_send_alert(pos_key):
                logger.info('[COOLDOWN] ' + pos_key + ' skip (cooldown 15min)')
                continue
            msg = format_entry_msg(symbol, signal, price, avg_price, sl, entry_count, strat, **kw)
            if entry_count == 1:
                # 1ere entree: bouton de confirmation
                cb_data = 'confirm:' + pos_key
                send_telegram_with_button(msg, cb_data)
            else:
                send_telegram(msg)
            track_signal_hour(signal)
            persist_positions()
            logger.info('[STRAT ' + strat + '] ' + signal + ' #' + str(entry_count) + ' ' + symbol + ' @ ' + str(price))
            audit_log({'ts': datetime.now(timezone.utc).isoformat(), 'sym': symbol, 'signal': signal, 'strat': strat, 'price': price, 'avg_price': avg_price, 'sl': sl, 'entry_count': entry_count})

    except Exception as e:
        logger.error(symbol + ': ' + str(e))

# ============================================================================ #
# SCANNER
# ============================================================================ #

def wait_next_15m_close():
    now  = time.time()
    wait = 15 * 60 - (now % (15 * 60))
    logger.info('Prochain scan dans ' + str(int(wait)) + 's')
    time.sleep(wait + 2)

def send_prep_report():
    with STATE_LOCK:
        entries = list(PREP_BUFFER)
        PREP_BUFFER.clear()
    if not entries:
        return
    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    msg = '⏳ <b>En attente de signal</b> ' + now + '\n' + '━' * 20
    groups = {}
    for e in entries:
        key = e['strat'] + '_' + e['dir']
        if key not in groups:
            groups[key] = []
        m15 = ('+' if e['macd_15m'] >= 0 else '') + str(round(e['macd_15m'], 4))
        em = '🟢' if e['dir'] == 'LONG' else '🔴'
        groups[key].append(em + ' ' + e['sym'].replace('/USDT', '') + ' $' + str(round(e['price'], 4)) + ' MACD15m:' + m15)
    for key in sorted(groups.keys()):
        strat, direction = key.split('_', 1)
        msg += '\n\n<b>STRAT ' + strat + ' ' + direction + '</b>\n'
        msg += '\n'.join(groups[key])
    send_telegram(msg)
    logger.info('[PREP] Rapport groupe envoye ' + str(len(entries)) + ' assets')

def scan_all():
    global LAST_SCAN_TIME, SCAN_IN_PROGRESS
    with STATE_LOCK:
        if SCAN_IN_PROGRESS:
            logger.warning('[SCAN] Scan déjà en cours — skip')
            return
        SCAN_IN_PROGRESS = True
    try:
        logger.info('Scan ' + str(len(CONFIG['SYMBOLS'])) + ' assets...')
        LAST_SCAN_TIME = datetime.now(timezone.utc).isoformat()
        for symbol in CONFIG['SYMBOLS']:
            process_symbol(symbol)
            time.sleep(0.3)
        send_prep_report()
        logger.info('Scan termine')
    finally:
        with STATE_LOCK:
            SCAN_IN_PROGRESS = False

def scanner_loop():
    while True:
        wait_next_15m_close()
        scan_all()

# ============================================================================ #
# RAPPORT HORAIRE
# ============================================================================ #

def send_hourly_aligned():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)

    bull_a, bear_a = [], []
    for symbol, s in state_copy.items():
        b4 = s.get('bias_4h')
        st = s.get('st_15m', '?')
        e  = '\U0001f7e2' if st == 'buy' else '\U0001f534'
        pr = '$' + str(round(s.get('price', 0), 4))
        base = e + ' ' + symbol.replace('/USDT', '') + ' ' + pr
        ctx = s.get('ctx_15m')
        if b4 == 'bull' and ctx == 'buy': bull_a.append(base)
        if b4 == 'bear' and ctx == 'sell': bear_a.append(base)

    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    msg = '\U0001f4ca <b>Aligned Report</b> ' + now + '\n' + '\u2501' * 20

    if bull_a or bear_a:
        msg += '\n\n<b>STRAT A (Bias 4H + ST Context 15min)</b>'
        if bull_a: msg += '\n\U0001f7e2 BULL (' + str(len(bull_a)) + '):\n' + '\n'.join(sorted(bull_a))
        if bear_a: msg += '\n\U0001f534 BEAR (' + str(len(bear_a)) + '):\n' + '\n'.join(sorted(bear_a))


    if not any([bull_a, bear_a]):
        return
    send_telegram(msg)
    logger.info('[HOURLY] Rapport envoye')

def hourly_scheduler():
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(minute=5, second=0, microsecond=0)
        if now.minute >= 5:
            next_run = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
        wait = (next_run - now).total_seconds()
        logger.info('[HOURLY] Prochain rapport dans ' + str(int(wait)) + 's')
        time.sleep(wait)
        send_hourly_aligned()

# ============================================================================ #
# TELEGRAM COMMANDS
# ============================================================================ #

def handle_telegram_command(message):
    chat_id = message.get('chat', {}).get('id')
    text    = message.get('text', '').strip().lower()
    if not chat_id: return

    if '/aligned' in text:
        with STATE_LOCK:
            state_copy = dict(SCAN_STATE)
        bull_a, bear_a = [], []
        for symbol, s in state_copy.items():
            b4 = s.get('bias_4h')
            st = s.get('st_15m', '?')
            e  = '\U0001f7e2' if st == 'buy' else '\U0001f534'
            pr = '$' + str(round(s.get('price', 0), 4))
            base = e + ' ' + symbol.replace('/USDT', '') + ' ' + pr
            ctx = s.get('ctx_15m')
            if b4 == 'bull' and ctx == 'buy': bull_a.append(base)
            if b4 == 'bear' and ctx == 'sell': bear_a.append(base)
        now = datetime.now(timezone.utc).strftime('%H:%M UTC')
        msg = '\U0001f4ca <b>Aligned</b> ' + now + '\n' + '\u2501' * 20
        if bull_a or bear_a:
            msg += '\n\n<b>STRAT A</b>'
            if bull_a: msg += '\n\U0001f7e2 BULL: ' + ', '.join(sorted([x.split(' ')[1] for x in bull_a]))
            if bear_a: msg += '\n\U0001f534 BEAR: ' + ', '.join(sorted([x.split(' ')[1] for x in bear_a]))
        send_telegram(msg)

    elif '/status' in text:
        with STATE_LOCK:
            state_copy = dict(SCAN_STATE)
            pos_copy   = dict(POSITIONS)
        ba = sum(1 for s in state_copy.values() if s.get('bias_4h') == 'bull' and s.get('ctx_15m') == 'buy')
        sa = sum(1 for s in state_copy.values() if s.get('bias_4h') == 'bear' and s.get('ctx_15m') == 'sell')
        now = datetime.now(timezone.utc).strftime('%H:%M UTC')
        msg = (
            '\U0001f916 <b>Scalp Bot v3</b> ' + now + '\n'
            + '\u2501' * 20 + '\n'
            + '\U0001f4ca Assets: ' + str(len(state_copy)) + '\n'
            + '\U0001f4cc Positions: ' + str(len(pos_copy)) + '\n'
            + '\n<b>STRAT A:</b> \U0001f7e2' + str(ba) + ' BULL | \U0001f534' + str(sa) + ' BEAR\n'
            + '\u23f0 Dernier scan: ' + (LAST_SCAN_TIME or 'N/A') + '\n'
        )
        send_telegram(msg)

# ============================================================================ #
# FLASK
# ============================================================================ #

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({'bot': 'Scalp Bot v3', 'status': 'running', 'assets': len(CONFIG['SYMBOLS'])})


@app.route('/webhook', methods=['POST'])
def webhook():
    """Reçoit le ST Context 15min depuis TradingView."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': False}), 400

    # Callback query (bouton inline Telegram) — traité en premier, pas de symbol requis
    if 'callback_query' in data:
        cq = data['callback_query']
        if not is_authorized_telegram_callback(cq):
            logger.warning('[SECURITY] Callback Telegram refuse (secret/chat non autorise)')
            return jsonify({'ok': False, 'reason': 'unauthorized_callback'}), 403
        cq_id   = cq.get('id')
        cb_data = cq.get('data', '')
        if cb_data.startswith('confirm:'):
            pos_key = cb_data[len('confirm:'):]
            with STATE_LOCK:
                CONFIRMED_TRADES[pos_key] = True
            answer_callback_query(cq_id, '✅ Position confirmée ! Pyramiding activé.')
            logger.info('[CONFIRM] Position confirmée: ' + pos_key)
            try:
                msg_id = cq.get('message', {}).get('message_id')
                chat_id = cq.get('message', {}).get('chat', {}).get('id')
                requests.post(
                    'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/editMessageReplyMarkup',
                    json={'chat_id': chat_id, 'message_id': msg_id, 'reply_markup': {'inline_keyboard': [[{'text': '✅ Entré en trade', 'callback_data': 'done'}]]}},
                    timeout=5
                )
            except Exception:
                pass
        return jsonify({'ok': True}), 200

    raw_symbol  = data.get('symbol', '')
    alert_type  = data.get('type', '').lower()
    tf          = data.get('tf', '').lower()
    val         = str(data.get('value', '')).strip().lower()
    try:
        price = float(data.get('price', 0) or 0)
    except (TypeError, ValueError):
        price = 0.0

    # Normaliser le symbole: BTCUSDT -> BTC/USDT
    symbol = raw_symbol.upper()
    if '/' not in symbol:
        for q in ['USDT', 'USDC']:
            if symbol.endswith(q):
                symbol = symbol[:-len(q)] + '/' + q
                break

    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({'ok': False, 'reason': 'not_in_watchlist'}), 200

    if alert_type == 'st_context' and tf == '15m':
        ctx_val = parse_st_context_value(val)
        with STATE_LOCK:
            ST_CONTEXT_15M[symbol] = ctx_val
        persist_weekly_state()
        logger.info('[WEBHOOK] ' + symbol + ' ST Context 15min: ' + str(ctx_val) + ' (val=' + val + ')')
        return jsonify({'ok': True, 'symbol': symbol, 'ctx_15m': ctx_val}), 200

    if alert_type == 'supertrend' and tf == '15m':
        st_val = parse_supertrend_value(val)
        with STATE_LOCK:
            ST_AI_15M[symbol] = st_val
        persist_weekly_state()
        logger.info('[WEBHOOK] ' + symbol + ' ST AI 15min: ' + str(st_val) + ' (val=' + val + ')')
        return jsonify({'ok': True, 'symbol': symbol, 'st_15m': st_val}), 200

    return jsonify({'ok': False, 'reason': 'unknown_type'}), 200

@app.route('/status')
def status():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)
        pos_copy   = dict(POSITIONS)
    assets = []
    for symbol in CONFIG['SYMBOLS']:
        s = state_copy.get(symbol, {})
        assets.append({
            'symbol':  symbol,
            'bias_4h': s.get('bias_4h', 'N/A'),
            'ctx_15m': s.get('ctx_15m', 'N/A'),
            'macd_15m': s.get('macd_15m', 0),
            'st_15m':  s.get('st_15m', 'N/A'),
            'price':   s.get('price', 0),
            'pos_a':   pos_copy.get(symbol + '_A'),
        })
    return jsonify({'last_scan': LAST_SCAN_TIME, 'assets': assets})

@app.route('/aligned')
def aligned():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)
    bull_a, bear_a = [], []
    for symbol, s in state_copy.items():
        b4 = s.get('bias_4h')
        st = s.get('st_15m')
        ctx = s.get('ctx_15m')
        entry = {'symbol': symbol, 'price': s.get('price'), 'st_15m': st, 'ctx_15m': ctx}
        if b4 == 'bull' and ctx == 'buy':  bull_a.append(entry)
        if b4 == 'bear' and ctx == 'sell': bear_a.append(entry)
    return jsonify({'strat_a': {'bull': bull_a, 'bear': bear_a}})

@app.route('/health')
def health():
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'assets': len(CONFIG['SYMBOLS']),
        'positions': len(POSITIONS),
        'redis': 'ok' if REDIS_CLIENT else 'unavailable',
        'last_scan': LAST_SCAN_TIME,
    }), 200

@app.route('/positions')
def positions():
    with STATE_LOCK:
        return jsonify(dict(POSITIONS))

@app.route('/audit')
def audit():
    symbol_filter = request.args.get('symbol')
    try:
        limit = max(1, min(500, int(request.args.get('limit', 100))))
    except (ValueError, TypeError):
        return jsonify({'error': 'limit doit être un entier entre 1 et 500'}), 400
    if not REDIS_CLIENT:
        return jsonify({'error': 'Redis non connecte'}), 503
    try:
        raw     = REDIS_CLIENT.lrange('scalp_audit_v3', 0, 999)
        entries = [json.loads(e) for e in raw]
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    if symbol_filter:
        sf = symbol_filter.upper()
        if '/USDT' not in sf: sf += '/USDT'
        entries = [e for e in entries if e.get('sym') == sf]
    return jsonify(entries[:limit])

@app.route('/scan', methods=['POST'])
def force_scan():
    with STATE_LOCK:
        if SCAN_IN_PROGRESS:
            return jsonify({'status': 'scan déjà en cours'}), 409
    threading.Thread(target=scan_all, daemon=True).start()
    return jsonify({'status': 'scan lance'})

@app.route('/reset_all', methods=['POST'])
def reset_all():
    """Remet tout l'état à zéro (positions, confirmed trades, scan state, context)."""
    with STATE_LOCK:
        POSITIONS.clear()
        CONFIRMED_TRADES.clear()
        SCAN_STATE.clear()
        ST_CONTEXT_15M.clear()
        LAST_ALERT_TS.clear()
    if REDIS_CLIENT:
        try:
            REDIS_CLIENT.delete('scalp_positions')
        except Exception as e:
            logger.error('Redis reset_all: ' + str(e))
    logger.info('[RESET] État complet remis à zéro')
    return jsonify({'status': 'reset', 'message': 'État complet remis à zéro'}), 200

@app.route('/reset/<path:symbol>', methods=['POST'])
def reset_position(symbol):
    sym  = symbol.replace('-', '/').upper()
    strat = request.args.get('strat', 'A')
    key  = sym + '_' + strat
    found = False
    with STATE_LOCK:
        if key in POSITIONS:
            del POSITIONS[key]
            found = True
    if found:
        persist_positions()  # hors du lock pour éviter deadlock
        return jsonify({'status': 'reset ' + key})
    return jsonify({'status': 'pas de position'}), 404

@app.route('/telegram', methods=['POST'])
def telegram_webhook():
    data = request.get_json(silent=True)
    if not data: return jsonify({'ok': True})

    secret = CONFIG.get('TELEGRAM_WEBHOOK_SECRET')
    if secret:
        header_secret = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if header_secret != secret:
            logger.warning('[SECURITY] Telegram webhook refuse (secret invalide)')
            return jsonify({'ok': False, 'reason': 'unauthorized'}), 403

    message = data.get('message') or data.get('edited_message')
    if message:
        threading.Thread(target=handle_telegram_command, args=(message,), daemon=True).start()
    return jsonify({'ok': True})

def send_weekly_report():
    global HOURLY_STATS, WEEKLY_START
    tw = timezone(timedelta(hours=8))
    now = datetime.now(tw)
    week_start = WEEKLY_START.astimezone(tw)

    # Snapshot sous lock pour éviter les races avec track_signal_hour
    with STATE_LOCK:
        stats_snapshot = {k: dict(v) for k, v in HOURLY_STATS.items()}

    total = sum(v.get('LONG', 0) + v.get('SHORT', 0) for v in stats_snapshot.values())

    if total == 0:
        msg = (
            '📊 <b>[RAPPORT HEBDO SCALP]</b>\n'
            + '━' * 20 + '\n'
            + '📅 ' + week_start.strftime('%d/%m') + ' → ' + now.strftime('%d/%m/%Y') + '\n'
            + 'Aucune alerte cette semaine.'
        )
        send_telegram(msg)
        with STATE_LOCK:
            HOURLY_STATS.clear()
        WEEKLY_START = datetime.now(timezone.utc)
        persist_weekly_state(force=True)
        return

    # Trier les créneaux par total décroissant
    ranked = sorted(
        stats_snapshot.items(),
        key=lambda x: x[1].get('LONG', 0) + x[1].get('SHORT', 0),
        reverse=True
    )

    msg = (
        '📊 <b>[RAPPORT HEBDO SCALP]</b>\n'
        + '━' * 20 + '\n'
        + '📅 ' + week_start.strftime('%d/%m') + ' → ' + now.strftime('%d/%m/%Y') + '\n'
        + '🔔 Total alertes: <b>' + str(total) + '</b>\n\n'
        + '⏰ <b>Créneaux horaires (Taiwan UTC+8)</b>\n'
        + '─' * 20 + '\n'
    )

    medals = ['🥇', '🥈', '🥉']
    for i, (hour, counts) in enumerate(ranked):
        lon = counts.get('LONG', 0)
        sho = counts.get('SHORT', 0)
        tot = lon + sho
        pct = round(tot / total * 100) if total else 0
        bar = '█' * (pct // 10) + '░' * (10 - pct // 10)
        medal = medals[i] if i < 3 else '  '
        msg += (
            medal + ' <b>' + hour + 'h–' + str(int(hour) + 1).zfill(2) + 'h</b>  '
            + bar + '  ' + str(tot) + ' (' + str(pct) + '%)\n'
            + '   🟢 LONG: ' + str(lon) + '  🔴 SHORT: ' + str(sho) + '\n'
        )

    msg += '\n⏰ ' + now.strftime('%d/%m/%Y %H:%M') + ' (Taiwan)'
    send_telegram(msg)
    logger.info('[WEEKLY] Rapport hebdo créneaux envoyé')

    with STATE_LOCK:
        HOURLY_STATS.clear()
    WEEKLY_START = datetime.now(timezone.utc)
    persist_weekly_state(force=True)


def weekly_scheduler():
    """Envoie le rapport hebdo dimanche à minuit heure Taiwan (UTC+8)."""
    logger.info('[WEEKLY] Scheduler rapport hebdo démarré (dimanche minuit Taiwan)')
    while True:
        now = datetime.now(timezone(timedelta(hours=8)))
        if now.weekday() == 6 and now.hour == 0 and now.minute == 0:
            send_weekly_report()
            time.sleep(61)
        else:
            time.sleep(30)


# ============================================================================ #
# DÉMARRAGE
# ============================================================================ #

def send_start_notification():
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    msg = (
        '\U0001f916 <b>[SCALP BOT v3 STARTED]</b>\n'
        + '\u2501' * 20 + '\n\n'
        + '\U0001f4ca Assets: ' + str(len(CONFIG['SYMBOLS'])) + '\n'
        + '\U0001f4be Redis: ' + ('\u2705' if REDIS_CLIENT else '\u26a0\ufe0f non connecte') + '\n\n'
        + '\U0001f4cb <b>STRATEGIES:</b>\n\n'
        + '<b>STRAT A</b>\n'
        + '\U0001f535 Filtre: Bias 4H (EMA13 vs SMA30)\n'
        + '\U0001f535 Zone: ST Context 15min\n'
        + '\U0001f7e2 Signal: Flip ST AI 15min\n\n'

        + '\u2501' * 20 + '\n'
        + '\u23f0 ' + now
    )
    send_telegram(msg)

if __name__ == '__main__':
    logger.info('Demarrage Scalp Bot v3...')
    init_redis()
    load_weekly_state()
    load_positions()
    send_start_notification()
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=hourly_scheduler, daemon=True).start()
    threading.Thread(target=weekly_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=CONFIG['PORT'])
