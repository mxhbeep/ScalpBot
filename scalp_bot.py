"""
SCALP BOT — SuperTrend AI 15min/1H
Stratégie : SuperTrend AI 1H comme filtre de tendance
            SuperTrend AI 15min flip comme signal d'entrée
Polling : à chaque bougie 15min fermée
"""

import os
import time
import math
import logging
import requests
import threading
from datetime import datetime, timezone
from typing import Optional

import ccxt
import pandas as pd
import numpy as np

# ============================================================================ #
# CONFIG
# ============================================================================ #

CONFIG = {
    'SYMBOLS': [
        # Majors
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'BNB/USDT', 'DOGE/USDT',
        # L1/L2
        'AVAX/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'SEI/USDT',
        'OP/USDT', 'ARB/USDT', 'TON/USDT', 'TIA/USDT', 'STX/USDT',
        # DeFi
        'AAVE/USDT', 'LINK/USDT', 'ENA/USDT', 'PENDLE/USDT', 'ZRO/USDT', 'ONDO/USDT',
        # IA & Tech
        'TAO/USDT', 'FET/USDT', 'RENDER/USDT', 'VIRTUAL/USDT', 'ZK/USDT',
        # Memes
        'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT',
        # Wildcard
        'HYPE/USDT', 'INJ/USDT', 'JUP/USDT', 'WLD/USDT', 'MOVE/USDT', 'POPCAT/USDT',
        # Solana ecosystem
        'RAY/USDT', 'JTO/USDT',
        # Gaming
        'AXS/USDT', 'IMX/USDT',
        # Autres OKX
        'LTC/USDT', 'DOT/USDT', 'ATOM/USDT', 'FIL/USDT',
        'SAND/USDT', 'MANA/USDT', 'CHZ/USDT', 'GALA/USDT',
        'HBAR/USDT', 'QNT/USDT',
    ],
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID':   os.environ.get('TELEGRAM_CHAT_ID', ''),
    # SuperTrend AI params (identiques à TradingView par défaut)
    'ST_ATR_LEN':         10,
    'ST_FACTOR':          3.0,
    # Cooldown entre deux mêmes signaux sur un asset (secondes)
    'COOLDOWN':           3600,
}

# ============================================================================ #
# LOGGING
# ============================================================================ #

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================ #
# ÉTAT
# ============================================================================ #

# Dernier signal envoyé par asset + direction
LAST_SIGNAL: dict[str, dict] = {}
STATE_LOCK = threading.Lock()

# ============================================================================ #
# EXCHANGE
# ============================================================================ #

exchange = ccxt.okx({'enableRateLimit': True})

def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 250) -> Optional[pd.DataFrame]:
    """Récupère les bougies OHLCV depuis OKX."""
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        return df
    except Exception as e:
        logger.error(f"❌ fetch_ohlcv {symbol} {timeframe}: {e}")
        return None

# ============================================================================ #
# SUPERTREND AI (reproduction Pine Script)
# ============================================================================ #

def supertrend_ai(df: pd.DataFrame, atr_len: int = 10, factor: float = 3.0) -> pd.Series:
    """
    Calcule le SuperTrend classique (base du SuperTrend AI).
    Retourne une Series : 'buy' ou 'sell' par bougie.
    Le SuperTrend AI adapte le factor via clustering — ici on utilise
    le factor fixe (comportement identique quand clustering converge).
    """
    high  = df['high']
    low   = df['low']
    close = df['close']

    # ATR
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/atr_len, adjust=False).mean()

    # Bandes
    hl2     = (high + low) / 2
    upper   = hl2 + factor * atr
    lower   = hl2 - factor * atr

    # SuperTrend
    n = len(df)
    trend    = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=str)

    for i in range(1, n):
        prev_upper = upper.iloc[i-1]
        prev_lower = lower.iloc[i-1]
        prev_close = close.iloc[i-1]
        curr_close = close.iloc[i]

        # Ajuste les bandes
        curr_upper = upper.iloc[i]
        curr_lower = lower.iloc[i]

        if curr_lower > prev_lower or prev_close < prev_lower:
            final_lower = curr_lower
        else:
            final_lower = prev_lower

        if curr_upper < prev_upper or prev_close > prev_upper:
            final_upper = curr_upper
        else:
            final_upper = prev_upper

        upper.iloc[i] = final_upper
        lower.iloc[i] = final_lower

        # Direction
        if i == 1:
            trend.iloc[i]     = final_lower
            direction.iloc[i] = 'buy'
        elif trend.iloc[i-1] == upper.iloc[i-1]:
            if curr_close > final_upper:
                trend.iloc[i]     = final_lower
                direction.iloc[i] = 'buy'
            else:
                trend.iloc[i]     = final_upper
                direction.iloc[i] = 'sell'
        else:
            if curr_close < final_lower:
                trend.iloc[i]     = final_upper
                direction.iloc[i] = 'sell'
            else:
                trend.iloc[i]     = final_lower
                direction.iloc[i] = 'buy'

    return direction

# ============================================================================ #
# TELEGRAM
# ============================================================================ #

def send_telegram(msg: str):
    url     = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Telegram envoyé")
        elif resp.status_code == 429:
            retry = resp.json().get('parameters', {}).get('retry_after', 30)
            logger.warning(f"⚠️ Telegram rate limit — retry {retry}s")
            time.sleep(retry)
            requests.post(url, json=payload, timeout=10)
        else:
            logger.error(f"❌ Telegram HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"❌ Telegram exception: {e}")

def format_signal_message(symbol: str, direction: str, price: float,
                           st_1h: str, st_15m: str) -> str:
    emoji     = "🟢" if direction == "LONG" else "🔴"
    dir_emoji = "📈" if direction == "LONG" else "📉"
    base      = symbol.replace('/USDT', '')
    now       = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    return (
        f"{emoji} <b>[SCALP {direction}] {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_emoji} Direction: {direction}\n"
        f"💰 Price: ${price:.4f}\n"
        f"🏦 Exchange: OKX\n"
        f"⏰ {now}\n"
        f"✅ ST AI 1H: {st_1h.upper()} (filtre)\n"
        f"✅ ST AI 15min: {st_15m.upper()} (flip signal)\n"
        f"🛑 SL: Sous dernier swing low\n"
    )

# ============================================================================ #
# LOGIQUE PRINCIPALE PAR ASSET
# ============================================================================ #

def process_symbol(symbol: str):
    """Analyse un asset et envoie un signal si les conditions sont remplies."""
    try:
        # Fetch bougies
        df_1h  = fetch_ohlcv(symbol, '1h',  limit=250)
        df_15m = fetch_ohlcv(symbol, '15m', limit=250)

        if df_1h is None or df_15m is None:
            return
        if len(df_1h) < 50 or len(df_15m) < 50:
            logger.warning(f"⚠️ {symbol} — pas assez de bougies")
            return

        # Calcul SuperTrend AI
        dir_1h  = supertrend_ai(df_1h,  CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])
        dir_15m = supertrend_ai(df_15m, CONFIG['ST_ATR_LEN'], CONFIG['ST_FACTOR'])

        # Valeurs actuelles (dernière bougie fermée = iloc[-2] car [-1] est en cours)
        curr_1h  = dir_1h.iloc[-2]
        curr_15m = dir_15m.iloc[-2]
        prev_15m = dir_15m.iloc[-3]
        price    = float(df_15m['close'].iloc[-2])

        # Détection flip 15min
        flip_buy  = (prev_15m == 'sell' and curr_15m == 'buy')
        flip_sell = (prev_15m == 'buy'  and curr_15m == 'sell')

        # Condition : flip 15min dans le sens du filtre 1H
        signal = None
        if flip_buy  and curr_1h == 'buy':
            signal = 'LONG'
        elif flip_sell and curr_1h == 'sell':
            signal = 'SHORT'

        if not signal:
            return

        # Cooldown
        now_ts = time.time()
        with STATE_LOCK:
            last = LAST_SIGNAL.get(symbol, {})
            if last.get('signal') == signal and now_ts - last.get('ts', 0) < CONFIG['COOLDOWN']:
                logger.info(f"[SCALP] {symbol} — cooldown actif ({signal})")
                return
            LAST_SIGNAL[symbol] = {'signal': signal, 'ts': now_ts}

        # Envoi
        msg = format_signal_message(symbol, signal, price, curr_1h, curr_15m)
        send_telegram(msg)
        logger.info(f"[SCALP] ✅ Signal {signal} envoyé — {symbol} @ {price}")

    except Exception as e:
        logger.error(f"❌ process_symbol {symbol}: {e}")

# ============================================================================ #
# SCHEDULER — attend la prochaine bougie 15min fermée
# ============================================================================ #

def wait_next_15m_close():
    """Attend jusqu'à la prochaine fermeture de bougie 15min."""
    now     = time.time()
    seconds = 15 * 60
    wait    = seconds - (now % seconds)
    logger.info(f"⏳ Prochain scan dans {int(wait)}s")
    time.sleep(wait + 2)  # +2s buffer pour laisser OKX finaliser la bougie

def scan_all():
    """Scanne tous les assets."""
    logger.info(f"🔍 Scan de {len(CONFIG['SYMBOLS'])} assets...")
    for symbol in CONFIG['SYMBOLS']:
        process_symbol(symbol)
        time.sleep(0.3)  # rate limit OKX
    logger.info("✅ Scan terminé")

# ============================================================================ #
# DÉMARRAGE
# ============================================================================ #

def send_start_notification():
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    msg = (
        "🤖 <b>[SCALP BOT STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
        "📋 <b>STRATÉGIE:</b>\n\n"
        "🔵 Filtre: SuperTrend AI 1H\n"
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
