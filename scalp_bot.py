"""
SCALP BOT v3 — Double Strategy
Stratégie A : Bias 4H + Bias 1H + MACD 15min neg/pos + flip ST 15min
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
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'BNB/USDT', 'DOGE/USDT',
        'AVAX/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'SEI/USDT',
        'OP/USDT', 'ARB/USDT', 'TON/USDT', 'TIA/USDT', 'STX/USDT',
        'AAVE/USDT', 'LINK/USDT', 'ENA/USDT', 'PENDLE/USDT', 'ZRO/USDT', 'ONDO/USDT',
        'FET/USDT', 'RENDER/USDT', 'VIRTUAL/USDT', 'ZK/USDT',
        'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT',
        'HYPE/USDT', 'INJ/USDT', 'JUP/USDT', 'WLD/USDT', 'MOVE/USDT',
        'RAY/USDT', 'JTO/USDT',
        'AXS/USDT', 'IMX/USDT',
        'LTC/USDT', 'DOT/USDT', 'ATOM/USDT', 'FIL/USDT',
        'SAND/USDT', 'MANA/USDT', 'CHZ/USDT', 'GALA/USDT',
        'HBAR/USDT',
    ],
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID':   os.environ.get('TELEGRAM_CHAT_ID', ''),
    'ST_ATR_LEN':    10,
    'ST_FACTOR':     3.0,
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

def supertrend(df, atr_len=10, factor=3.0):
    high  = df['high'].copy()
    low   = df['low'].copy()
    close = df['close'].copy()
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr   = tr.ewm(alpha=1/atr_len, adjust=False).mean()
    hl2   = (high + low) / 2
    upper = (hl2 + factor * atr).copy()
    lower = (hl2 - factor * atr).copy()
    n     = len(df)
    trend = pd.Series(np.nan, index=df.index, dtype=float)
    direction = pd.Series('', index=df.index, dtype=str)
    for i in range(1, n):
        pc = close.iloc[i-1]; cc = close.iloc[i]
        fu = upper.iloc[i] if (upper.iloc[i] < upper.iloc[i-1] or pc > upper.iloc[i-1]) else upper.iloc[i-1]
        fl = lower.iloc[i] if (lower.iloc[i] > lower.iloc[i-1] or pc < lower.iloc[i-1]) else lower.iloc[i-1]
        upper.iloc[i] = fu; lower.iloc[i] = fl
        if i == 1:
            trend.iloc[i] = fl; direction.iloc[i] = 'buy'
        elif trend.iloc[i-1] == upper.iloc[i-1]:
            if cc > fu: trend.iloc[i] = fl; direction.iloc[i] = 'buy'
            else:       trend.iloc[i] = fu; direction.iloc[i] = 'sell'
        else:
            if cc < fl: trend.iloc[i] = fu; direction.iloc[i] = 'sell'
            else:       trend.iloc[i] = fl; direction.iloc[i] = 'buy'
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
        df_4h  = fetch_ohlcv(symbol, '4H',  limit=200)
        df_2h  = fetch_ohlcv(symbol, '2H',  limit=100)
        df_1h  = fetch_ohlcv(symbol, '1H',  limit=200)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_4h is None or df_2h is None or df_1h is None or df_15m is None: return
        if len(df_4h) < 35 or len(df_2h) < 35 or len(df_1h) < 35 or len(df_15m) < 50: return

        # Indicateurs
        bias_4h   = calc_bias(df_4h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_1h   = calc_bias(df_1h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        macd_2h   = calc_macd_histogram(df_2h,  CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-2)
        macd_2h_p = calc_macd_histogram(df_2h,  CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-3)
        macd_15m  = calc_macd_histogram(df_15m, CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'], candle=-2)

        dir_15m  = supertrend(df_15m, CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])
        curr_15m  = dir_15m.iloc[-2]
        prev_15m  = dir_15m.iloc[-3]
        prev2_15m = dir_15m.iloc[-4]
        price     = float(df_15m['close'].iloc[-2])

        flip_buy  = (prev_15m == 'sell' and curr_15m == 'buy') or (prev2_15m == 'sell' and prev_15m == 'buy' and curr_15m == 'buy')
        flip_sell = (prev_15m == 'buy'  and curr_15m == 'sell') or (prev2_15m == 'buy' and prev_15m == 'sell' and curr_15m == 'sell')
        flip      = flip_buy or flip_sell

        # Conditions Strat A
        a_long  = bias_4h == 'bull' and bias_1h == 'bull'
        a_short = bias_4h == 'bear' and bias_1h == 'bear'

        # Conditions Strat B
        b_long  = macd_2h > 0 and bias_1h == 'bull'
        b_short = macd_2h < 0 and bias_1h == 'bear'

        # Retournement MACD 2H (pour TP)
        macd_2h_flip_bear = macd_2h_p > 0 and macd_2h < 0
        macd_2h_flip_bull = macd_2h_p < 0 and macd_2h > 0

        # Debug log
        reason_a = 'no flip' if not flip else ('LONG A' if flip_buy and a_long and macd_15m < 0 else 'SHORT A' if flip_sell and a_short and macd_15m > 0 else 'filtre A')
        reason_b = 'no flip' if not flip else ('LONG B' if flip_buy and b_long and macd_15m < 0 else 'SHORT B' if flip_sell and b_short and macd_15m > 0 else 'filtre B')
        logger.info('[SCAN] ' + symbol.ljust(20) + ' B4H=' + bias_4h + ' B1H=' + bias_1h + ' M2H=' + ('+' if macd_2h >= 0 else '') + str(round(macd_2h, 4)) + ' M15m=' + ('+' if macd_15m >= 0 else '') + str(round(macd_15m, 4)) + ' ST=' + curr_15m + ' flip=' + str(flip) + ' A:' + reason_a + ' B:' + reason_b)

        # Update scan state
        with STATE_LOCK:
            SCAN_STATE[symbol] = {
                'bias_4h': bias_4h, 'bias_1h': bias_1h,
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
            ('A', flip_buy and a_long  and macd_15m < 0, flip_sell and a_short and macd_15m > 0, {'bias_4h': bias_4h, 'bias_1h': bias_1h, 'macd_15m': macd_15m}),
            ('B', flip_buy and b_long  and macd_15m < 0, flip_sell and b_short and macd_15m > 0, {'bias_1h': bias_1h, 'macd_2h': macd_2h, 'macd_15m': macd_15m}),
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
        if b4 == 'bull' and b1 == 'bull': bull_a.append(base)
        if b4 == 'bear' and b1 == 'bear': bear_a.append(base)
        if m2 > 0 and b1 == 'bull': bull_b.append(base)
        if m2 < 0 and b1 == 'bear': bear_b.append(base)

    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    msg = '\U0001f4ca <b>Aligned Report</b> ' + now + '\n' + '\u2501' * 20

    if bull_a or bear_a:
        msg += '\n\n<b>STRAT A (Bias 4H+1H)</b>'
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
            if b4 == 'bull' and b1 == 'bull': bull_a.append(base)
            if b4 == 'bear' and b1 == 'bear': bear_a.append(base)
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
        + '\U0001f535 Confirmation: Bias 1H\n'
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
