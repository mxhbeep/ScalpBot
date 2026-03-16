"""
SCALP BOT — SuperTrend AI 15min/1H + Bias 3H
Stratégie : Bias 3H (EMA13 vs SMA30) comme filtre macro
            SuperTrend AI 1H comme filtre de tendance
            SuperTrend AI 15min flip comme signal d'entrée
Polling : à chaque bougie 15min fermée
"""

import os
import time
import logging
import requests
import threading
from datetime import datetime, timezone
from typing import Optional

import ccxt
import pandas as pd
import numpy as np

CONFIG = {
    'SYMBOLS': [
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'BNB/USDT', 'DOGE/USDT',
        'AVAX/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'SEI/USDT',
        'OP/USDT', 'ARB/USDT', 'TON/USDT', 'TIA/USDT', 'STX/USDT',
        'AAVE/USDT', 'LINK/USDT', 'ENA/USDT', 'PENDLE/USDT', 'ZRO/USDT', 'ONDO/USDT',
        'TAO/USDT', 'FET/USDT', 'RENDER/USDT', 'VIRTUAL/USDT', 'ZK/USDT',
        'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT',
        'HYPE/USDT', 'INJ/USDT', 'JUP/USDT', 'WLD/USDT', 'MOVE/USDT', 'POPCAT/USDT',
        'RAY/USDT', 'JTO/USDT',
        'AXS/USDT', 'IMX/USDT',
        'LTC/USDT', 'DOT/USDT', 'ATOM/USDT', 'FIL/USDT',
        'SAND/USDT', 'MANA/USDT', 'CHZ/USDT', 'GALA/USDT',
        'HBAR/USDT', 'QNT/USDT',
    ],
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID':   os.environ.get('TELEGRAM_CHAT_ID', ''),
    'ST_ATR_LEN':   10,
    'ST_FACTOR':    3.0,
    'BIAS_EMA_LEN': 13,
    'BIAS_SMA_LEN': 30,
    'COOLDOWN':     3600,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

LAST_SIGNAL: dict = {}
STATE_LOCK = threading.Lock()

exchange = ccxt.okx({'enableRateLimit': True})

def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 250) -> Optional[pd.DataFrame]:
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df  = pd.DataFrame(raw, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        return df
    except Exception as e:
        logger.error(f"❌ fetch_ohlcv {symbol} {timeframe}: {e}")
        return None

def calc_bias(df: pd.DataFrame, ema_len: int = 13, sma_len: int = 30) -> str:
    """EMA13 vs SMA30 — reproduction exacte du Pine Script CarréBias."""
    close   = df['close']
    ema_val = close.ewm(span=ema_len, adjust=False).mean().iloc[-2]
    sma_val = close.rolling(window=sma_len).mean().iloc[-2]
    return 'bull' if ema_val > sma_val else 'bear'

def supertrend(df: pd.DataFrame, atr_len: int = 10, factor: float = 3.0) -> pd.Series:
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

    n         = len(df)
    trend     = pd.Series(np.nan, index=df.index, dtype=float)
    direction = pd.Series('', index=df.index, dtype=str)

    for i in range(1, n):
        pc = close.iloc[i-1]
        cc = close.iloc[i]
        fu = upper.iloc[i] if (upper.iloc[i] < upper.iloc[i-1] or pc > upper.iloc[i-1]) else upper.iloc[i-1]
        fl = lower.iloc[i] if (lower.iloc[i] > lower.iloc[i-1] or pc < lower.iloc[i-1]) else lower.iloc[i-1]
        upper.iloc[i] = fu
        lower.iloc[i] = fl

        if i == 1:
            trend.iloc[i] = fl; direction.iloc[i] = 'buy'
        elif trend.iloc[i-1] == upper.iloc[i-1]:
            if cc > fu: trend.iloc[i] = fl; direction.iloc[i] = 'buy'
            else:       trend.iloc[i] = fu; direction.iloc[i] = 'sell'
        else:
            if cc < fl: trend.iloc[i] = fu; direction.iloc[i] = 'sell'
            else:       trend.iloc[i] = fl; direction.iloc[i] = 'buy'

    return direction

def send_telegram(msg: str):
    url     = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Telegram envoyé")
        elif resp.status_code == 429:
            retry = resp.json().get('parameters', {}).get('retry_after', 30)
            time.sleep(retry)
            requests.post(url, json=payload, timeout=10)
        else:
            logger.error(f"❌ Telegram HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"❌ Telegram: {e}")

def format_message(symbol, direction, price, bias_3h, st_1h, st_15m):
    emoji = "🟢" if direction == "LONG" else "🔴"
    b_emoji = "🟢" if bias_3h == 'bull' else "🔴"
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    return (
        f"{emoji} <b>[SCALP {direction}] {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: ${price:.4f}\n"
        f"🏦 Exchange: OKX\n"
        f"⏰ {now}\n"
        f"{b_emoji} Bias 3H: {bias_3h.upper()}\n"
        f"✅ ST AI 1H: {st_1h.upper()}\n"
        f"✅ ST AI 15min: flip {st_15m.upper()}\n"
        f"🛑 SL: Sous dernier swing low\n"
    )

def process_symbol(symbol: str):
    try:
        df_3h  = fetch_ohlcv(symbol, '3h',  limit=100)
        df_1h  = fetch_ohlcv(symbol, '1h',  limit=250)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_3h is None or df_1h is None or df_15m is None:
            return
        if len(df_3h) < 35 or len(df_1h) < 50 or len(df_15m) < 50:
            return

        bias_3h  = calc_bias(df_3h,  CONFIG['BIAS_EMA_LEN'], CONFIG['BIAS_SMA_LEN'])
        dir_1h   = supertrend(df_1h,  CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])
        dir_15m  = supertrend(df_15m, CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])

        curr_1h  = dir_1h.iloc[-2]
        curr_15m = dir_15m.iloc[-2]
        prev_15m = dir_15m.iloc[-3]
        price    = float(df_15m['close'].iloc[-2])

        flip_buy  = (prev_15m == 'sell' and curr_15m == 'buy')
        flip_sell = (prev_15m == 'buy'  and curr_15m == 'sell')

        signal = None
        if   flip_buy  and curr_1h == 'buy'  and bias_3h == 'bull': signal = 'LONG'
        elif flip_sell and curr_1h == 'sell' and bias_3h == 'bear': signal = 'SHORT'

        if not signal:
            return

        now_ts = time.time()
        with STATE_LOCK:
            last = LAST_SIGNAL.get(symbol, {})
            if last.get('signal') == signal and now_ts - last.get('ts', 0) < CONFIG['COOLDOWN']:
                return
            LAST_SIGNAL[symbol] = {'signal': signal, 'ts': now_ts}

        msg = format_message(symbol, signal, price, bias_3h, curr_1h, curr_15m)
        send_telegram(msg)
        logger.info(f"[SCALP] ✅ {signal} {symbol} @ {price} | Bias3H={bias_3h} ST1H={curr_1h}")

    except Exception as e:
        logger.error(f"❌ {symbol}: {e}")

def wait_next_15m_close():
    now  = time.time()
    wait = 15 * 60 - (now % (15 * 60))
    logger.info(f"⏳ Prochain scan dans {int(wait)}s")
    time.sleep(wait + 2)

def scan_all():
    logger.info(f"🔍 Scan {len(CONFIG['SYMBOLS'])} assets...")
    for symbol in CONFIG['SYMBOLS']:
        process_symbol(symbol)
        time.sleep(0.3)
    logger.info("✅ Scan terminé")

def send_start_notification():
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    msg = (
        "🤖 <b>[SCALP BOT STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
        "📋 <b>STRATÉGIE:</b>\n\n"
        "🔵 Filtre 1: Bias 3H (EMA13 vs SMA30)\n"
        "🔵 Filtre 2: SuperTrend AI 1H\n"
        "🟢 Signal: Flip SuperTrend AI 15min\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {now}"
    )
    send_telegram(msg)

if __name__ == '__main__':
    logger.info("🚀 Démarrage du Scalp Bot...")
    send_start_notification()
    while True:
        wait_next_15m_close()
        scan_all()
