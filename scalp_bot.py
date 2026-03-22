"""
SCALP BOT v3 — Double Strategy
Stratégie A : Bias 4H + ST Context 15min + flip ST AI 15min
Stratégie B : MACD 2H direction + Bias 1H + MACD 15min neg/pos + flip ST 15min
              TP partiel alerte sur retournement MACD 2H
Pyramiding illimité, SL swing low -> break even
"""

import os, time, json, logging, requests, threading
from datetime import datetime, timezone, timedelta
from typing import Optional

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
LAST_SIGNAL: dict = {}
PREP_BUFFER: list = []
SCAN_STATE:  dict = {}
ST_CONTEXT_15M: dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_1H:  dict = {}  # symbol -> 'buy' | 'sell' | None
STATE_LOCK = threading.Lock()
LAST_SCAN_TIME = None
REDIS_CLIENT = None

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

# ============================================================================ #
# EXCHANGE
# ============================================================================ #

exchange = ccxt.okx({'enableRateLimit': True})

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
        prev_os = os_f
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

def format_entry_msg(symbol, direction, price, avg_price, sl, entry_count, strat, bias_4h=None, bias_1h=None, macd_2h=None, macd_15m=None):
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
    ]
    if strat == 'A' and bias_4h:
        e4 = '\U0001f7e2' if bias_4h == 'bull' else '\U0001f534'
        lines.append(e4 + ' Bias 4H: ' + bias_4h.upper())
    elif strat == 'B' and macd_2h is not None:
        m2h_str = ('+' if macd_2h >= 0 else '') + str(round(macd_2h, 4))
        lines.append('\U0001f4ca MACD 2H: ' + m2h_str)
    if bias_1h:
        e1 = '\U0001f7e2' if bias_1h == 'bull' else '\U0001f534'
        lines.append(e1 + ' Bias 1H: ' + bias_1h.upper())
    if macd_15m is not None:
        m15_str = ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4))
        lines.append('\U0001f4ca MACD 15min: ' + m15_str)
    lines.append('\u2705 ST AI 15min: flip ' + direction.lower())
    if entry_count > 1:
        lines.append('\U0001f4c8 Positions accumulees: ' + str(entry_count))
    return '\n'.join(lines)

def format_prep_msg(symbol, direction, price, strat, bias_4h=None, bias_1h=None, macd_2h=None, macd_15m=None):
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
    elif strat == 'B' and macd_2h is not None:
        m2h_str = ('+' if macd_2h >= 0 else '') + str(round(macd_2h, 4))
        lines.append('\U0001f4ca MACD 2H: ' + m2h_str)
    if bias_1h:
        e1 = '\U0001f7e2' if bias_1h == 'bull' else '\U0001f534'
        lines.append(e1 + ' Bias 1H: ' + bias_1h.upper())
    if macd_15m is not None:
        m15_str = ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4))
        lines.append('\U0001f4ca MACD 15min: ' + m15_str)
    lines.append('\u23f3 En attente flip ST AI 15min ' + direction)
    return '\n'.join(lines)

def format_tp_msg(symbol, direction, price, strat, macd_2h):
    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    flip_dir = 'BEAR' if direction == 'LONG' else 'BULL'
    m2h_str = ('+' if macd_2h >= 0 else '') + str(round(macd_2h, 4))
    lines = [
        '\U0001f4ca <b>[TP PARTIEL | STRAT ' + strat + '] ' + symbol + '</b>',
        '\u2501' * 20,
        '\U0001f4b0 Price: $' + str(round(price, 6)),
        '\U0001f4ca MACD 2H flip ' + flip_dir + ': ' + m2h_str,
        '\u2705 TP partiel conseille sur position ' + direction,
        '\u23f0 ' + now,
    ]
    return '\n'.join(lines)

# ============================================================================ #
# PROCESS SYMBOL
# ============================================================================ #

def process_symbol(symbol):
    try:
        df_4h  = fetch_ohlcv(symbol, '4h',  limit=200)
        df_2h  = fetch_ohlcv(symbol, '2h',  limit=100)
        df_1h  = fetch_ohlcv(symbol, '1h',  limit=200)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_4h is None or df_2h is None or df_1h is None or df_15m is None: return
        if len(df_4h) < 35 or len(df_2h) < 35 or len(df_1h) < 35 or len(df_15m) < 50: return

        # Indicateurs
        bias_4h   = calc_bias(df_4h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_2h   = calc_bias(df_2h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_1h   = calc_bias(df_1h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        macd_2h   = calc_macd_histogram(df_2h,  CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-2)
        macd_2h_p = calc_macd_histogram(df_2h,  CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-3)
        macd_15m  = calc_macd_histogram(df_15m, CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-2)

        dir_15m  = supertrend_ai(df_15m)
        curr_15m  = dir_15m.iloc[-2]
        prev_15m  = dir_15m.iloc[-3]
        prev2_15m = dir_15m.iloc[-4]
        price     = float(df_15m['close'].iloc[-2])

        flip_buy  = (prev_15m == 'sell' and curr_15m == 'buy') or (prev2_15m == 'sell' and prev_15m == 'buy' and curr_15m == 'buy')
        flip_sell = (prev_15m == 'buy'  and curr_15m == 'sell') or (prev2_15m == 'buy' and prev_15m == 'sell' and curr_15m == 'sell')
        flip      = flip_buy or flip_sell

        # ST Context (recu via webhook TradingView)
        with STATE_LOCK:
            ctx_15m = ST_CONTEXT_15M.get(symbol)
            ctx_1h  = ST_CONTEXT_1H.get(symbol)

        # Conditions Strat A: Bias 4H + ST Context 15min zone
        a_long  = bias_4h == 'bull' and ctx_15m == 'buy'
        a_short = bias_4h == 'bear' and ctx_15m == 'sell'

        # Conditions Strat B
        b_long  = macd_2h > 0 and bias_1h == 'bull'
        b_short = macd_2h < 0 and bias_1h == 'bear'

        # Retournement MACD 2H (pour TP)
        macd_2h_flip_bear = macd_2h_p > 0 and macd_2h < 0
        macd_2h_flip_bull = macd_2h_p < 0 and macd_2h > 0

        # Debug log
        reason_a = 'no flip' if not flip else ('LONG A' if flip_buy and a_long else 'SHORT A' if flip_sell and a_short else 'filtre A (B4H=' + bias_4h + ' CTX=' + str(ctx_15m) + ')')
        reason_b = 'no flip' if not flip else ('LONG B' if flip_buy and b_long and macd_15m < 0 else 'SHORT B' if flip_sell and b_short and macd_15m > 0 else 'filtre B')
        logger.info('[SCAN] ' + symbol.ljust(20) + ' B4H=' + bias_4h + ' CTX15m=' + str(ctx_15m) + ' M2H=' + ('+' if macd_2h >= 0 else '') + str(round(macd_2h, 4)) + ' M15m=' + ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4)) + ' ST=' + curr_15m + ' flip=' + str(flip) + ' A:' + reason_a + ' B:' + reason_b)

        # Update scan state
        with STATE_LOCK:
            SCAN_STATE[symbol] = {
                'bias_4h': bias_4h, 'bias_2h': bias_2h, 'bias_1h': bias_1h,
                'ctx_15m': ST_CONTEXT_15M.get(symbol),
                'ctx_1h': ST_CONTEXT_1H.get(symbol),
                'macd_2h': round(macd_2h, 6), 'macd_15m': round(macd_15m, 6),
                'st_15m': curr_15m, 'price': price,
                'ts': datetime.now(timezone.utc).isoformat(),
            }

        # TP partiel Strat B
        now_ts = time.time()
        with STATE_LOCK:
            pos_b = POSITIONS.get(symbol + '_B')
        if pos_b:
            d = pos_b['direction']
            if (d == 'LONG' and macd_2h_flip_bear) or (d == 'SHORT' and macd_2h_flip_bull):
                send_telegram(format_tp_msg(symbol, d, price, 'B', macd_2h))
                logger.info('[TP B] ' + symbol + ' ' + d + ' MACD 2H flip')
                audit_log({'ts': datetime.now(timezone.utc).isoformat(), 'sym': symbol, 'event': 'tp_partiel_B', 'direction': d, 'price': price})

        # Collecte des assets en preparation (rapport groupé toutes les 15min)
        prep_entries = []
        if a_long  and macd_15m < 0 and not flip_buy:
            prep_entries.append({'sym': symbol, 'dir': 'LONG',  'strat': 'A', 'price': price, 'bias_4h': bias_4h, 'bias_1h': bias_1h, 'macd_15m': macd_15m})
        if a_short and macd_15m > 0 and not flip_sell:
            prep_entries.append({'sym': symbol, 'dir': 'SHORT', 'strat': 'A', 'price': price, 'bias_4h': bias_4h, 'bias_1h': bias_1h, 'macd_15m': macd_15m})
        if b_long  and macd_15m < 0 and not flip_buy:
            prep_entries.append({'sym': symbol, 'dir': 'LONG',  'strat': 'B', 'price': price, 'bias_1h': bias_1h, 'macd_2h': macd_2h, 'macd_15m': macd_15m})
        if b_short and macd_15m > 0 and not flip_sell:
            prep_entries.append({'sym': symbol, 'dir': 'SHORT', 'strat': 'B', 'price': price, 'bias_1h': bias_1h, 'macd_2h': macd_2h, 'macd_15m': macd_15m})
        if prep_entries:
            with STATE_LOCK:
                PREP_BUFFER.extend(prep_entries)

        # Signaux
        for strat, sig_long, sig_short, kw in [
            ('A', flip_buy and a_long, flip_sell and a_short, {'bias_4h': bias_4h, 'bias_1h': bias_1h, 'macd_2h': ctx_15m, 'macd_15m': macd_15m}),
            ('B', flip_buy and b_long  and macd_15m < 0, flip_sell and b_short and macd_15m > 0, {'bias_1h': bias_1h, 'macd_2h': macd_2h, 'macd_15m': macd_15m, 'ctx_1h': ctx_1h}),
        ]:
            signal = 'LONG' if sig_long else ('SHORT' if sig_short else None)
            if not signal: continue

            pos_key = symbol + '_' + strat
            with STATE_LOCK:
                pos = POSITIONS.get(pos_key)
                if pos and pos['direction'] != signal:
                    del POSITIONS[pos_key]
                    pos = None

                if pos is None:
                    # Premiere entree Strat B: ctx_1h obligatoire
                    if strat == 'B':
                        ctx_1h_needed = 'buy' if signal == 'LONG' else 'sell'
                        if kw.get('ctx_1h') != ctx_1h_needed:
                            logger.info('[STRAT B] ' + signal + ' ' + symbol + ' filtre: ctx_1h=' + str(kw.get('ctx_1h')) + ' requis=' + ctx_1h_needed)
                            continue
                    POSITIONS[pos_key] = {
                        'direction': signal,
                        'entries': [{'price': price, 'ts': datetime.now(timezone.utc).isoformat()}],
                        'avg_price': price,
                        'entry_count': 1,
                        'sl': 0,
                    }
                    pos = POSITIONS[pos_key]
                else:
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

            msg = format_entry_msg(symbol, signal, price, avg_price, sl, entry_count, strat, **kw)
            send_telegram(msg)
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
    global LAST_SCAN_TIME
    logger.info('Scan ' + str(len(CONFIG['SYMBOLS'])) + ' assets...')
    LAST_SCAN_TIME = datetime.now(timezone.utc).isoformat()
    for symbol in CONFIG['SYMBOLS']:
        process_symbol(symbol)
        time.sleep(0.3)
    send_prep_report()
    logger.info('Scan termine')

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

    bull_a, bear_a, bull_b, bear_b = [], [], [], []
    for symbol, s in state_copy.items():
        b4 = s.get('bias_4h'); b1 = s.get('bias_1h')
        m2 = s.get('macd_2h', 0); st = s.get('st_15m', '?')
        e  = '\U0001f7e2' if st == 'buy' else '\U0001f534'
        pr = '$' + str(round(s.get('price', 0), 4))
        base = e + ' ' + symbol.replace('/USDT', '') + ' ' + pr
        b2 = s.get('bias_2h', b4)
        if b2 == 'bull' and ctx == 'buy': bull_a.append(base)
        if b2 == 'bear' and ctx == 'sell': bear_a.append(base)
        if m2 > 0 and b1 == 'bull': bull_b.append(base)
        if m2 < 0 and b1 == 'bear': bear_b.append(base)

    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    msg = '\U0001f4ca <b>Aligned Report</b> ' + now + '\n' + '\u2501' * 20

    if bull_a or bear_a:
        msg += '\n\n<b>STRAT A (Bias 2H + ST Context 15min)</b>'
        if bull_a: msg += '\n\U0001f7e2 BULL (' + str(len(bull_a)) + '):\n' + '\n'.join(sorted(bull_a))
        if bear_a: msg += '\n\U0001f534 BEAR (' + str(len(bear_a)) + '):\n' + '\n'.join(sorted(bear_a))

    if bull_b or bear_b:
        msg += '\n\n<b>STRAT B (MACD 2H+Bias 1H)</b>'
        if bull_b: msg += '\n\U0001f7e2 BULL (' + str(len(bull_b)) + '):\n' + '\n'.join(sorted(bull_b))
        if bear_b: msg += '\n\U0001f534 BEAR (' + str(len(bear_b)) + '):\n' + '\n'.join(sorted(bear_b))

    if not any([bull_a, bear_a, bull_b, bear_b]):
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
        bull_a, bear_a, bull_b, bear_b = [], [], [], []
        for symbol, s in state_copy.items():
            b4 = s.get('bias_4h'); b1 = s.get('bias_1h')
            m2 = s.get('macd_2h', 0); st = s.get('st_15m', '?')
            e  = '\U0001f7e2' if st == 'buy' else '\U0001f534'
            pr = '$' + str(round(s.get('price', 0), 4))
            base = e + ' ' + symbol.replace('/USDT', '') + ' ' + pr
        b2 = s.get('bias_2h', b4)
        ctx = s.get('ctx_15m')
        if b2 == 'bull' and ctx == 'buy': bull_a.append(base)
        if b2 == 'bear' and ctx == 'sell': bear_a.append(base)
        if m2 > 0 and b1 == 'bull': bull_b.append(base)
        if m2 < 0 and b1 == 'bear': bear_b.append(base)
        now = datetime.now(timezone.utc).strftime('%H:%M UTC')
        msg = '\U0001f4ca <b>Aligned</b> ' + now + '\n' + '\u2501' * 20
        if bull_a or bear_a:
            msg += '\n\n<b>STRAT A</b>'
            if bull_a: msg += '\n\U0001f7e2 BULL: ' + ', '.join(sorted([x.split(' ')[1] for x in bull_a]))
            if bear_a: msg += '\n\U0001f534 BEAR: ' + ', '.join(sorted([x.split(' ')[1] for x in bear_a]))
        if bull_b or bear_b:
            msg += '\n\n<b>STRAT B</b>'
            if bull_b: msg += '\n\U0001f7e2 BULL: ' + ', '.join(sorted([x.split(' ')[1] for x in bull_b]))
            if bear_b: msg += '\n\U0001f534 BEAR: ' + ', '.join(sorted([x.split(' ')[1] for x in bear_b]))
        url = 'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/sendMessage'
        requests.post(url, json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'}, timeout=10)

    elif '/status' in text:
        with STATE_LOCK:
            state_copy = dict(SCAN_STATE)
            pos_copy   = dict(POSITIONS)
        ba = sum(1 for s in state_copy.values() if s.get('bias_4h') == 'bull' and s.get('bias_1h') == 'bull')
        sa = sum(1 for s in state_copy.values() if s.get('bias_4h') == 'bear' and s.get('bias_1h') == 'bear')
        bb = sum(1 for s in state_copy.values() if s.get('macd_2h', 0) > 0 and s.get('bias_1h') == 'bull')
        sb = sum(1 for s in state_copy.values() if s.get('macd_2h', 0) < 0 and s.get('bias_1h') == 'bear')
        now = datetime.now(timezone.utc).strftime('%H:%M UTC')
        msg = (
            '\U0001f916 <b>Scalp Bot v3</b> ' + now + '\n'
            + '\u2501' * 20 + '\n'
            + '\U0001f4ca Assets: ' + str(len(state_copy)) + '\n'
            + '\U0001f4cc Positions: ' + str(len(pos_copy)) + '\n'
            + '\n<b>STRAT A:</b> \U0001f7e2' + str(ba) + ' BULL | \U0001f534' + str(sa) + ' BEAR\n'
            + '<b>STRAT B:</b> \U0001f7e2' + str(bb) + ' BULL | \U0001f534' + str(sb) + ' BEAR\n'
            + '\u23f0 Dernier scan: ' + (LAST_SCAN_TIME or 'N/A') + '\n'
        )
        url = 'https://api.telegram.org/bot' + CONFIG['TELEGRAM_BOT_TOKEN'] + '/sendMessage'
        requests.post(url, json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'}, timeout=10)

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

    raw_symbol  = data.get('symbol', '')
    alert_type  = data.get('type', '').lower()
    tf          = data.get('tf', '').lower()
    val         = str(data.get('value', '')).strip().lower()
    price       = data.get('price', 0)

    # Normaliser le symbole: BTCUSDT -> BTC/USDT
    symbol = raw_symbol.upper()
    if '/' not in symbol:
        for q in ['USDT', 'USDC']:
            if symbol.endswith(q):
                symbol = symbol[:-len(q)] + '/' + q
                break

    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({'ok': False, 'reason': 'not_in_watchlist'}), 200

    if alert_type == 'st_context' and tf in ('15m', '1h'):
        ctx_val = None
        if val in ('buy', 'sell'):
            ctx_val = val
        else:
            try:
                fval = float(val)
                if fval < -1.96:   ctx_val = 'buy'
                elif fval > 1.96:  ctx_val = 'sell'
                else:              ctx_val = None
            except (ValueError, TypeError):
                pass

        with STATE_LOCK:
            if tf == '15m':
                ST_CONTEXT_15M[symbol] = ctx_val
            else:
                ST_CONTEXT_1H[symbol] = ctx_val
        logger.info('[WEBHOOK] ' + symbol + ' ST Context ' + tf + ': ' + str(ctx_val) + ' (val=' + val + ')')
        return jsonify({'ok': True, 'symbol': symbol, 'ctx': ctx_val, 'tf': tf}), 200

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
            'bias_1h': s.get('bias_1h', 'N/A'),
            'macd_2h': s.get('macd_2h', 0),
            'macd_15m': s.get('macd_15m', 0),
            'st_15m':  s.get('st_15m', 'N/A'),
            'price':   s.get('price', 0),
            'pos_a':   pos_copy.get(symbol + '_A'),
            'pos_b':   pos_copy.get(symbol + '_B'),
        })
    return jsonify({'last_scan': LAST_SCAN_TIME, 'assets': assets})

@app.route('/aligned')
def aligned():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)
    bull_a, bear_a, bull_b, bear_b = [], [], [], []
    for symbol, s in state_copy.items():
        b4 = s.get('bias_4h'); b1 = s.get('bias_1h')
        m2 = s.get('macd_2h', 0); st = s.get('st_15m')
        entry = {'symbol': symbol, 'price': s.get('price'), 'st_15m': st}
        if b4 == 'bull' and b1 == 'bull': bull_a.append(entry)
        if b4 == 'bear' and b1 == 'bear': bear_a.append(entry)
        if m2 > 0 and b1 == 'bull': bull_b.append(entry)
        if m2 < 0 and b1 == 'bear': bear_b.append(entry)
    return jsonify({'strat_a': {'bull': bull_a, 'bear': bear_a}, 'strat_b': {'bull': bull_b, 'bear': bear_b}})

@app.route('/positions')
def positions():
    with STATE_LOCK:
        return jsonify(dict(POSITIONS))

@app.route('/audit')
def audit():
    symbol_filter = request.args.get('symbol')
    limit = int(request.args.get('limit', 100))
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
    threading.Thread(target=scan_all, daemon=True).start()
    return jsonify({'status': 'scan lance'})

@app.route('/reset/<path:symbol>', methods=['POST'])
def reset_position(symbol):
    sym  = symbol.replace('-', '/').upper()
    strat = request.args.get('strat', 'A')
    key  = sym + '_' + strat
    with STATE_LOCK:
        if key in POSITIONS:
            del POSITIONS[key]
            return jsonify({'status': 'reset ' + key})
    return jsonify({'status': 'pas de position'}), 404

@app.route('/telegram', methods=['POST'])
def telegram_webhook():
    data = request.get_json(silent=True)
    if not data: return jsonify({'ok': True})
    message = data.get('message') or data.get('edited_message')
    if message:
        threading.Thread(target=handle_telegram_command, args=(message,), daemon=True).start()
    return jsonify({'ok': True})

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
        + '<b>STRAT B</b>\n'
        + '\U0001f535 Filtre: MACD 2H direction\n'
        + '\U0001f535 Confirmation: Bias 1H\n'
        + '\U0001f7e2 Signal: Flip ST AI 15min\n'
        + '\U0001f4ca TP: alerte retournement MACD 2H\n\n'
        + '\u2501' * 20 + '\n'
        + '\u23f0 ' + now
    )
    send_telegram(msg)

if __name__ == '__main__':
    logger.info('Demarrage Scalp Bot v3...')
    init_redis()
    send_start_notification()
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=hourly_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=CONFIG['PORT'])
