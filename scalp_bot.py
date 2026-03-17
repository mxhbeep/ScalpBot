"""
SCALP BOT v2 — Pyramiding Strategy
Bias 3H + Bias 1H → direction
Flip ST AI 15min → entrée (sizing progressif)
SL: swing low 15min → break even après 2ème entrée
Sortie: SL touché uniquement
"""

import os
import time
import json
import logging
import requests
import threading
from datetime import datetime, timezone
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
    'SWING_LOOKBACK': 5,   # bougies 15min pour trouver le swing low/high
    'MACD_FAST':     12,
    'MACD_SLOW':     26,
    'MACD_SIGNAL':   9,
    'PORT':          int(os.environ.get('PORT', 5001)),
}

# ============================================================================ #
# LOGGING
# ============================================================================ #

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================ #
# ÉTAT
# ============================================================================ #

# Position active par asset
# {symbol: {direction, entries: [{price, ts}], avg_price, sl, entry_count}}
POSITIONS: dict = {}

SCAN_STATE:  dict = {}
STATE_LOCK = threading.Lock()
LAST_SCAN_TIME = None
REDIS_CLIENT = None

# ============================================================================ #
# REDIS
# ============================================================================ #

def init_redis():
    global REDIS_CLIENT
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        logger.warning("REDIS_URL non defini")
        return
    try:
        REDIS_CLIENT = redis.from_url(redis_url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info("Redis connecte")
    except Exception as e:
        logger.error(f"Redis erreur: {e}")
        REDIS_CLIENT = None

def audit_log(entry: dict):
    if REDIS_CLIENT:
        try:
            REDIS_CLIENT.lpush('scalp_audit_v2', json.dumps(entry))
            REDIS_CLIENT.ltrim('scalp_audit_v2', 0, 999)
        except Exception as e:
            logger.error(f"Redis audit: {e}")

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
        logger.error(f"fetch_ohlcv {symbol} {timeframe}: {e}")
        return None

# ============================================================================ #
# INDICATEURS
# ============================================================================ #

def calc_bias(df, ema_len=13, sma_len=30):
    """EMA13 vs SMA30 — CarréBias Pine Script."""
    close   = df['close']
    ema_val = close.ewm(span=ema_len, adjust=False).mean().iloc[-2]
    sma_val = close.rolling(window=sma_len).mean().iloc[-2]
    return 'bull' if ema_val > sma_val else 'bear'

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


def calc_macd(df, fast=12, slow=26, signal=9):
    """Retourne la valeur de l'histogramme MACD sur la dernière bougie fermée."""
    close      = df['close']
    ema_fast   = close.ewm(span=fast,   adjust=False).mean()
    ema_slow   = close.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line
    return float(histogram.iloc[-2])

def get_swing_low(df, lookback=5):
    """Dernier swing low sur les N dernières bougies 15min."""
    return float(df['low'].iloc[-lookback-2:-2].min())

def get_swing_high(df, lookback=5):
    """Dernier swing high sur les N dernières bougies 15min."""
    return float(df['high'].iloc[-lookback-2:-2].max())

def calc_sl(direction, df_15m, avg_price, entry_count):
    """
    Entrée 1 : SL = swing low/high récent
    Entrée 2+ : SL = break even (prix moyen)
    """
    if entry_count <= 1:
        if direction == 'LONG':
            return get_swing_low(df_15m, CONFIG['SWING_LOOKBACK'])
        else:
            return get_swing_high(df_15m, CONFIG['SWING_LOOKBACK'])
    else:
        return avg_price  # break even

# ============================================================================ #
# TELEGRAM
# ============================================================================ #

def send_telegram(msg):
    url     = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("Telegram envoye")
        elif resp.status_code == 429:
            retry = resp.json().get('parameters', {}).get('retry_after', 30)
            time.sleep(retry)
            requests.post(url, json=payload, timeout=10)
        else:
            logger.error(f"Telegram HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Telegram: {e}")

def format_entry_message(symbol, direction, price, avg_price, sl, entry_count, bias_3h, bias_1h, macd_hist):
    emoji     = "🟢" if direction == "LONG" else "🔴"
    sl_label  = "Swing low" if entry_count == 1 else "Break even"
    sl_emoji  = "🆕" if entry_count == 1 else "⚖️"
    now       = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    b3_emoji  = "🟢" if bias_3h == 'bull' else "🔴"
    b1_emoji  = "🟢" if bias_1h == 'bull' else "🔴"

    msg = (
        f"{emoji} <b>[SCALP {direction} #{entry_count}] {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Prix entrée: ${price:.4f}\n"
    )
    if entry_count > 1:
        msg += f"📊 Prix moyen: ${avg_price:.4f}\n"
    msg += (
        f"{sl_emoji} SL ({sl_label}): ${sl:.4f}\n"
        f"🏦 Exchange: OKX\n"
        f"⏰ {now}\n"
        f"{b3_emoji} Bias 3H: {bias_3h.upper()}\n"
        f"{b1_emoji} Bias 1H: {bias_1h.upper()}\n"
        f"✅ ST AI 15min: flip {direction.lower()}\n"
        f"📊 MACD 15min: {macd_hist:+.4f}\n"
    )
    if entry_count > 1:
        msg += f"📈 Positions accumulées: {entry_count}\n"
    return msg

def format_invalidation_message(symbol, direction, bias_3h, bias_1h, position):
    emoji = "⚠️"
    now   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    return (
        f"{emoji} <b>[BIAIS INVALIDE] {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Direction précédente: {direction}\n"
        f"Positions ouvertes: {position['entry_count']}\n"
        f"Prix moyen: ${position['avg_price']:.4f}\n"
        f"SL actuel: ${position['sl']:.4f}\n\n"
        f"🔴 Bias 3H: {bias_3h.upper()} | Bias 1H: {bias_1h.upper()}\n"
        f"⚠️ Biais non alignés — gérer la position manuellement\n"
        f"⏰ {now}"
    )

# ============================================================================ #
# PROCESS SYMBOL
# ============================================================================ #

def process_symbol(symbol):
    try:
        df_3h  = fetch_ohlcv(symbol, '4h',  limit=100)
        df_1h  = fetch_ohlcv(symbol, '1h',  limit=100)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_3h is None or df_1h is None or df_15m is None: return
        if len(df_3h) < 35 or len(df_1h) < 35 or len(df_15m) < 50: return

        bias_3h  = calc_bias(df_3h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        bias_1h  = calc_bias(df_1h, CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        dir_15m  = supertrend(df_15m, CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])
        macd_hist = calc_macd(df_15m, CONFIG['MACD_FAST'], CONFIG['MACD_SLOW'], CONFIG['MACD_SIGNAL'])

        curr_15m = dir_15m.iloc[-2]
        prev_15m = dir_15m.iloc[-3]
        price    = float(df_15m['close'].iloc[-2])

        flip_buy  = (prev_15m == 'sell' and curr_15m == 'buy')
        flip_sell = (prev_15m == 'buy'  and curr_15m == 'sell')

        # Alignement biais
        bias_long  = (bias_3h == 'bull' and bias_1h == 'bull')
        bias_short = (bias_3h == 'bear' and bias_1h == 'bear')
        bias_aligned = bias_long or bias_short

        # DEBUG
        flip = flip_buy or flip_sell
        if not flip:
            reason = "no flip"
        elif flip_buy and bias_long and macd_hist < 0:
            reason = f"SIGNAL LONG (MACD={macd_hist:.4f})"
        elif flip_sell and bias_short and macd_hist > 0:
            reason = f"SIGNAL SHORT (MACD={macd_hist:.4f})"
        elif flip_buy and bias_long:
            reason = f"filtré: MACD={macd_hist:.4f} > 0 (pas négatif)"
        elif flip_sell and bias_short:
            reason = f"filtré: MACD={macd_hist:.4f} < 0 (pas positif)"
        elif flip_buy:
            reason = f"filtré: Bias3H={bias_3h} Bias1H={bias_1h}"
        else:
            reason = f"filtré: Bias3H={bias_3h} Bias1H={bias_1h}"

        logger.info(f"[SCAN] {symbol:<20} Bias3H={bias_3h:<4} Bias1H={bias_1h:<4} ST15m={curr_15m:<4} MACD={macd_hist:+.4f} flip={str(flip):<5} → {reason}")

        # Mise à jour scan state
        with STATE_LOCK:
            SCAN_STATE[symbol] = {
                'bias_3h': bias_3h, 'bias_1h': bias_1h, 'macd_hist': macd_hist,
                'st_15m': curr_15m, 'price': price,
                'ts': datetime.now(timezone.utc).isoformat(),
            }

        # Vérifie si position active → biais invalide
        with STATE_LOCK:
            position = POSITIONS.get(symbol)

        if position:
            direction = position['direction']
            pos_valid = (direction == 'LONG' and bias_long) or (direction == 'SHORT' and bias_short)
            if not pos_valid:
                logger.info(f"[SCALP] {symbol} biais invalide — position {direction} à gérer")
                send_telegram(format_invalidation_message(symbol, direction, bias_3h, bias_1h, position))
                with STATE_LOCK:
                    del POSITIONS[symbol]
                audit_log({'ts': datetime.now(timezone.utc).isoformat(), 'sym': symbol,
                           'event': 'invalidation', 'direction': direction, 'price': price})
                return

        # Nouveau signal
        signal = None
        if   flip_buy  and bias_long  and macd_hist < 0: signal = 'LONG'
        elif flip_sell and bias_short and macd_hist > 0: signal = 'SHORT'
        if not signal: return

        # Mise à jour position
        with STATE_LOCK:
            pos = POSITIONS.get(symbol)
            if pos and pos['direction'] != signal:
                # Direction opposée → reset position
                del POSITIONS[symbol]
                pos = None

            if pos is None:
                POSITIONS[symbol] = {
                    'direction':   signal,
                    'entries':     [{'price': price, 'ts': datetime.now(timezone.utc).isoformat()}],
                    'avg_price':   price,
                    'entry_count': 1,
                    'sl':          0,
                }
                pos = POSITIONS[symbol]
            else:
                pos['entries'].append({'price': price, 'ts': datetime.now(timezone.utc).isoformat()})
                pos['entry_count'] += 1
                pos['avg_price'] = sum(e['price'] for e in pos['entries']) / len(pos['entries'])

            # Calcul SL
            pos['sl'] = calc_sl(signal, df_15m, pos['avg_price'], pos['entry_count'])
            entry_count = pos['entry_count']
            avg_price   = pos['avg_price']
            sl          = pos['sl']

        msg = format_entry_message(symbol, signal, price, avg_price, sl, entry_count, bias_3h, bias_1h, macd_hist)
        send_telegram(msg)
        logger.info(f"[SCALP] {signal} #{entry_count} {symbol} @ {price} | avg={avg_price:.4f} SL={sl:.4f}")

        audit_log({
            'ts': datetime.now(timezone.utc).isoformat(),
            'sym': symbol, 'signal': signal, 'price': price,
            'avg_price': avg_price, 'sl': sl, 'entry_count': entry_count,
            'bias_3h': bias_3h, 'bias_1h': bias_1h, 'macd_hist': macd_hist,
        })

    except Exception as e:
        logger.error(f"{symbol}: {e}")

# ============================================================================ #
# SCANNER
# ============================================================================ #

def wait_next_15m_close():
    now  = time.time()
    wait = 15 * 60 - (now % (15 * 60))
    logger.info(f"Prochain scan dans {int(wait)}s")
    time.sleep(wait + 2)

def scan_all():
    global LAST_SCAN_TIME
    logger.info(f"Scan {len(CONFIG['SYMBOLS'])} assets...")
    LAST_SCAN_TIME = datetime.now(timezone.utc).isoformat()
    for symbol in CONFIG['SYMBOLS']:
        process_symbol(symbol)
        time.sleep(0.3)
    logger.info("Scan termine")

def scanner_loop():
    while True:
        wait_next_15m_close()
        scan_all()

# ============================================================================ #
# FLASK
# ============================================================================ #

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({'bot': 'Scalp Bot v2', 'status': 'running', 'assets': len(CONFIG['SYMBOLS'])})

@app.route('/status')
def status():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)
        pos_copy   = dict(POSITIONS)
    assets = []
    for symbol in CONFIG['SYMBOLS']:
        s = state_copy.get(symbol, {})
        p = pos_copy.get(symbol)
        assets.append({
            'symbol':      symbol,
            'bias_3h':     s.get('bias_3h', 'N/A'),
            'bias_1h':     s.get('bias_1h', 'N/A'),
            'st_15m':      s.get('st_15m', 'N/A'),
            'price':       s.get('price', 0),
            'position':    p,
        })
    return jsonify({
        'last_scan':       LAST_SCAN_TIME,
        'active_positions': len(pos_copy),
        'assets':          assets,
    })

@app.route('/positions')
def positions():
    with STATE_LOCK:
        pos_copy = dict(POSITIONS)
    return jsonify(pos_copy)

@app.route('/audit')
def audit():
    symbol_filter = request.args.get('symbol')
    limit = int(request.args.get('limit', 100))
    if not REDIS_CLIENT:
        return jsonify({'error': 'Redis non connecte'}), 503
    try:
        raw     = REDIS_CLIENT.lrange('scalp_audit_v2', 0, 999)
        entries = [json.loads(e) for e in raw]
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    if symbol_filter:
        sf = symbol_filter.upper()
        if '/USDT' not in sf: sf += '/USDT'
        entries = [e for e in entries if e.get('sym') == sf]
    return jsonify(entries[:limit])


@app.route('/aligned')
def aligned():
    with STATE_LOCK:
        state_copy = dict(SCAN_STATE)
    
    bull = []
    bear = []
    for symbol, s in state_copy.items():
        b3 = s.get('bias_3h')
        b1 = s.get('bias_1h')
        if b3 == 'bull' and b1 == 'bull':
            bull.append({'symbol': symbol, 'price': s.get('price'), 'st_15m': s.get('st_15m')})
        elif b3 == 'bear' and b1 == 'bear':
            bear.append({'symbol': symbol, 'price': s.get('price'), 'st_15m': s.get('st_15m')})
    
    return jsonify({
        'bull': sorted(bull, key=lambda x: x['symbol']),
        'bear': sorted(bear, key=lambda x: x['symbol']),
        'total_bull': len(bull),
        'total_bear': len(bear),
        'last_scan': LAST_SCAN_TIME,
    })

@app.route('/scan', methods=['POST'])
def force_scan():
    threading.Thread(target=scan_all, daemon=True).start()
    return jsonify({'status': 'scan lance'})

@app.route('/reset/<path:symbol>', methods=['POST'])
def reset_position(symbol):
    sym = symbol.replace('-', '/').upper()
    with STATE_LOCK:
        if sym in POSITIONS:
            del POSITIONS[sym]
            return jsonify({'status': f'position {sym} reset'})
    return jsonify({'status': 'pas de position active'}), 404

# ============================================================================ #
# DÉMARRAGE
# ============================================================================ #

def send_start_notification():
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    msg = (
        "🤖 <b>[SCALP BOT v2 STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"💾 Redis: {'✅' if REDIS_CLIENT else '⚠️ non connecté'}\n\n"
        "📋 <b>STRATÉGIE:</b>\n\n"
        "🔵 Filtre 1: Bias 4H (EMA13 vs SMA30)\n"
        "🔵 Filtre 2: Bias 1H (EMA13 vs SMA30)\n"
        "🟢 Signal: Flip ST AI 15min (MACD contre-tendance)\n"
        "📈 Sizing: pyramiding illimité\n"
        "🛑 SL: swing low → break even\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {now}"
    )
    send_telegram(msg)

if __name__ == '__main__':
    logger.info("Demarrage Scalp Bot v2...")
    init_redis()
    send_start_notification()
    threading.Thread(target=scanner_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=CONFIG['PORT'])
