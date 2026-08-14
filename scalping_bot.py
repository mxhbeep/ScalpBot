#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot - ST AI 1H + ST AI 30m + Bias 30m + Context 1m + RF10m
# Tag haute qualite - ST Context 1H aligne
# Service Railway séparé

import json
import time
import requests
import logging
import threading
import os
import re
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
    'NTFY_TOPIC':         os.environ.get('NTFY_TOPIC', 'maxence-trading-3f8a72'),
    'MIN_COOLDOWN':       3600,   # 1H entrée
    'PYRA_COOLDOWN':      1800,   # 30min pyramiding

    'SYMBOLS': {
        'BTC/USDT':     {'exchange': 'okx'},
        'CRV/USDT':     {'exchange': 'okx'},
        'CVX/USDT':     {'exchange': 'okx'},
        'ETH/USDT':     {'exchange': 'okx'},
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
        bar_map = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1H', '2h': '2H', '4h': '4H', '1d': '1D'}
        bar = bar_map.get(tf, '1H')
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
        resp = requests.get(url, timeout=10)
        data = resp.json().get('data', [])
        if not data:
            return None
        df = pd.DataFrame(data, columns=['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'])
        df = df[df['confirm'] == '1'].copy()
        df['open'] = df['o'].astype(float)
        df['high'] = df['h'].astype(float)
        df['low'] = df['l'].astype(float)
        df['close'] = df['c'].astype(float)
        df['volume'] = df['vol'].astype(float)
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

def calc_williams_ema(df, length=14, ema_length=14):
    """Calcule Williams %R et son EMA."""
    try:
        if df is None or len(df) < length + ema_length:
            return None
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)
        upper = high.rolling(length).max()
        lower = low.rolling(length).min()
        spread = upper - lower
        williams = 100.0 * (close - upper) / spread.where(spread != 0)
        williams_ema = williams.ewm(span=ema_length, adjust=False).mean()
        value = float(williams.iloc[-1])
        ema_value = float(williams_ema.iloc[-1])
        if pd.isna(value) or pd.isna(ema_value):
            return None
        return {
            'value': value,
            'ema': ema_value,
            'trend': 'bull' if value > ema_value else 'bear' if value < ema_value else 'neutral',
        }
    except Exception as e:
        logger.debug(f"[WILLIAMS] Calcul impossible: {e}")
        return None

def get_williams_filter(symbol, timeframe, direction, max_age_seconds):
    expected_trend = 'bull' if str(direction or '').upper() == 'LONG' else 'bear'
    key = f'williams_{timeframe}'
    ts_key = f'{key}_ts'
    data = dict(MOMENTUM_STATE.get(symbol, {}).get(key) or {})
    ts = MOMENTUM_STATE.get(symbol, {}).get(ts_key)
    fresh = bool(data) and is_fresh(ts, max_age_seconds)
    trend = data.get('trend')
    return {
        'data': data,
        'fresh': fresh,
        'ok': fresh and trend == expected_trend,
        'trend': trend,
        'value': data.get('value'),
        'ema': data.get('ema'),
    }

def format_williams_filter_line(label, williams_filter):
    value = williams_filter.get('value')
    ema_value = williams_filter.get('ema')
    trend = williams_filter.get('trend')
    if value is None or ema_value is None:
        return f"[OK] Williams {label}: N/A"
    relation = ">" if trend == 'bull' else "<" if trend == 'bear' else "="
    return f"[OK] Williams {label}: W%R {value:.2f} {relation} EMA14 {ema_value:.2f}"

def calc_range_filter_signal(df, per=100, mult=3.0):
    """Reproduit le Range Filter Pine et retourne le dernier signal confirme."""
    try:
        if df is None or len(df) < (per * 2 + 5):
            return None

        close = df['close'].astype(float).reset_index(drop=True)
        wper = per * 2 - 1
        avrng = close.diff().abs().ewm(span=per, adjust=False).mean()
        smrng = avrng.ewm(span=wper, adjust=False).mean() * mult

        filt = []
        for i, x in enumerate(close):
            prev = x if i == 0 else filt[-1]
            r = smrng.iloc[i]
            if pd.isna(r):
                filt.append(prev)
            elif x > prev:
                filt.append(prev if x - r < prev else x - r)
            else:
                filt.append(prev if x + r > prev else x + r)
        filt = pd.Series(filt)

        upward = []
        downward = []
        for i, value in enumerate(filt):
            if i == 0:
                upward.append(0.0)
                downward.append(0.0)
                continue
            prev_up = upward[-1]
            prev_down = downward[-1]
            prev_filt = filt.iloc[i - 1]
            upward.append(prev_up + 1 if value > prev_filt else 0.0 if value < prev_filt else prev_up)
            downward.append(prev_down + 1 if value < prev_filt else 0.0 if value > prev_filt else prev_down)

        long_cond = (close > filt) & (pd.Series(upward) > 0)
        short_cond = (close < filt) & (pd.Series(downward) > 0)

        cond_ini = []
        for long_ok, short_ok in zip(long_cond, short_cond):
            prev = cond_ini[-1] if cond_ini else 0
            cond_ini.append(1 if long_ok else -1 if short_ok else prev)

        if len(close) < 2:
            return None

        idx = len(close) - 1
        prev_cond = cond_ini[idx - 1]
        direction = None
        if bool(long_cond.iloc[idx]) and prev_cond == -1:
            direction = 'buy'
        elif bool(short_cond.iloc[idx]) and prev_cond == 1:
            direction = 'sell'

        if direction is None:
            return None

        return {
            'direction': direction,
            'ts': str(df['ts'].iloc[idx]),
            'price': float(close.iloc[idx]),
        }
    except Exception as e:
        logger.info(f"[RANGE] calc failed: {e}")
        logger.debug(f"[RANGE] calc exception", exc_info=True)
        return None

def keep_confirmed_candles(df, timeframe_minutes):
    """Retourne uniquement les bougies dont la cloture est deja passee."""
    if df is None or df.empty:
        return None
    duration_ms = int(timeframe_minutes * 60 * 1000)
    now_ms = int(time.time() * 1000)
    confirmed = df[df['ts'].astype('int64') + duration_ms <= now_ms].copy()
    if confirmed.empty:
        return None
    return confirmed.reset_index(drop=True)

def build_confirmed_20m_candles(df_5m):
    """Agrège quatre bougies 5m confirmées sur les bornes UTC de 20 minutes."""
    if df_5m is None or df_5m.empty:
        return None
    work = df_5m.copy()
    work['ts'] = pd.to_numeric(work['ts'], errors='coerce')
    work = work.dropna(subset=['ts', 'close']).sort_values('ts')
    work['bucket_20m'] = (work['ts'].astype('int64') // 1_200_000) * 1_200_000
    grouped = work.groupby('bucket_20m', sort=True).agg(
        close=('close', 'last'),
        source_count=('close', 'count'),
    )
    grouped = grouped[grouped['source_count'] == 4].reset_index(names='ts')
    return grouped[['ts', 'close']]


def update_bias_20m():
    """Met à jour le Bias 20m pour tous les assets toutes les 5min."""
    logger.info("Scheduler Bias 20m démarré")
    while True:
        try:
            # Calculer tous les bias HORS du lock (les fetches OKX peuvent être longs)
            results = {}
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df_5m = fetch_ohlcv_okx(symbol, '5m', limit=160)
                    if df_5m is None:
                        logger.info(f"[BIAS] {symbol} bias20m=None reason=fetch_failed (df OKX 5m vide)")
                        results[symbol] = {'bias': None, 'price': None}
                        continue
                    df = build_confirmed_20m_candles(df_5m)
                    if df is None or len(df) < 30:
                        count = 0 if df is None else len(df)
                        logger.info(f"[BIAS] {symbol} bias20m=None reason=insufficient_confirmed_candles ({count}/30)")
                        results[symbol] = {'bias': None, 'price': None}
                        continue
                    bias = calc_bias(df, ema_len=8, sma_len=21)
                    williams_30m = calc_williams_ema(df, length=14, ema_length=14)
                    price = float(df['close'].iloc[-1]) if len(df) else None
                    results[symbol] = {'bias': bias, 'price': price, 'williams_30m': williams_30m}
                    if bias is None:
                        logger.info(f"[BIAS] {symbol} bias20m=None reason=neutral (pas d'alignement close/EMA/SMA)")
                    else:
                        logger.info(f"[BIAS] {symbol} bias20m={bias} price={price}")
                except Exception as e:
                    logger.info(f"[BIAS] {symbol} bias20m=None reason=exception:{e}")
                    logger.debug(f"[BIAS] {symbol}: {e}")
                    results[symbol] = {'bias': None, 'price': None}
            # Mettre à jour l'état avec des locks courts symbol par symbol
            pending_alerts = []
            for symbol, result in results.items():
                bias = result.get('bias')
                price = result.get('price')
                with STATE_LOCK:
                    init_symbol(symbol)
                    m = MOMENTUM_STATE[symbol]
                    m['bias_20m'] = bias
            for msg, log_msg in pending_alerts:
                send_telegram(msg)
                logger.info(log_msg)
            persist_state()
            bias_ok_count  = sum(1 for r in results.values() if r.get('bias') is not None)
            fetch_ok_count = sum(1 for r in results.values() if r.get('price') is not None)
            logger.info(f"[BIAS] Mise à jour Bias 20m terminée ({bias_ok_count}/{len(CONFIG['SYMBOLS'])} assets avec bias non-neutre, {fetch_ok_count}/{len(CONFIG['SYMBOLS'])} fetch OK)")
        except Exception as e:
            logger.error(f"[BIAS] Erreur: {e}")
        time.sleep(300)  # toutes les 5min


def update_bias_30m():
    """Met a jour le Bias 30m pour tous les assets toutes les 5min."""
    logger.info("Scheduler Bias 30m demarre")
    while True:
        try:
            results = {}
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df = fetch_ohlcv_okx(symbol, '30m', limit=80)
                    df_1h = fetch_ohlcv_okx(symbol, '1h', limit=80)
                    if df is None or len(df) < 30:
                        count = 0 if df is None else len(df)
                        logger.info(f"[BIAS] {symbol} bias30m=None reason=insufficient_confirmed_candles ({count}/30)")
                        results[symbol] = {'bias': None, 'price': None}
                        continue
                    bias = calc_bias(df, ema_len=8, sma_len=21)
                    williams_30m = calc_williams_ema(df, length=14, ema_length=14)
                    williams_1h = calc_williams_ema(df_1h, length=14, ema_length=14) if df_1h is not None else None
                    price = float(df['close'].iloc[-1]) if len(df) else None
                    results[symbol] = {'bias': bias, 'price': price, 'williams_30m': williams_30m, 'williams_1h': williams_1h}
                    if bias is None:
                        logger.info(f"[BIAS] {symbol} bias30m=None reason=neutral")
                    else:
                        logger.info(f"[BIAS] {symbol} bias30m={bias} price={price}")
                except Exception as e:
                    logger.info(f"[BIAS] {symbol} bias30m=None reason=exception:{e}")
                    logger.debug(f"[BIAS] {symbol}: {e}")
                    results[symbol] = {'bias': None, 'price': None}

            for symbol, result in results.items():
                with STATE_LOCK:
                    init_symbol(symbol)
                    MOMENTUM_STATE[symbol]['bias_30m'] = result.get('bias')
                    MOMENTUM_STATE[symbol]['bias_30m_ts'] = time.time()
                    if result.get('williams_30m') is not None:
                        MOMENTUM_STATE[symbol]['williams_30m'] = result.get('williams_30m')
                        MOMENTUM_STATE[symbol]['williams_30m_ts'] = time.time()
                    if result.get('williams_1h') is not None:
                        MOMENTUM_STATE[symbol]['williams_1h'] = result.get('williams_1h')
                        MOMENTUM_STATE[symbol]['williams_1h_ts'] = time.time()
            persist_state()
            bias_ok_count = sum(1 for r in results.values() if r.get('bias') is not None)
            fetch_ok_count = sum(1 for r in results.values() if r.get('price') is not None)
            logger.info(f"[BIAS] Mise a jour Bias 30m terminee ({bias_ok_count}/{len(CONFIG['SYMBOLS'])} assets avec bias non-neutre, {fetch_ok_count}/{len(CONFIG['SYMBOLS'])} fetch OK)")
        except Exception as e:
            logger.error(f"[BIAS] Erreur Bias 30m: {e}")
        time.sleep(300)

def build_confirmed_10m_candles(df_5m):
    """Agrege les bougies 5m confirmees par paires en bougies 10m."""
    df_5m = keep_confirmed_candles(df_5m, 5)
    if df_5m is None or len(df_5m) < 2:
        return None
    df = df_5m.copy().sort_values('ts').reset_index(drop=True)
    bucket_ms = 10 * 60 * 1000
    df['bucket'] = (df['ts'].astype('int64') // bucket_ms) * bucket_ms
    counts = df.groupby('bucket').size()
    complete = counts[counts >= 2].index
    df = df[df['bucket'].isin(complete)]
    if df.empty:
        return None
    return df.groupby('bucket', as_index=False).agg(
        ts=('bucket', 'first'), open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum'),
    )


def update_range_filter_10m():
    """Calcule le Range Filter 10m depuis OKX et declenche la strategie scalp."""
    logger.info("Scheduler Range Filter 10m demarre")
    while True:
        try:
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df_5m = fetch_ohlcv_okx(symbol, '5m', limit=300)
                    df_10m = build_confirmed_10m_candles(df_5m)
                    signal = calc_range_filter_signal(df_10m, per=100, mult=2.0)
                    if signal is None:
                        continue

                    direction = signal['direction']
                    signal_ts = signal['ts']
                    signal_price = signal['price']

                    should_process = False
                    with STATE_LOCK:
                        init_symbol(symbol)
                        m = MOMENTUM_STATE[symbol]
                        if m.get('last_range_filter_10m_signal_ts') != signal_ts:
                            m['last_range_filter_10m_signal_ts'] = signal_ts
                            should_process = True

                    if not should_process:
                        continue

                    logger.info(
                        f"[RANGE] Nouveau signal 10m {symbol} "
                        f"dir={direction} ts={signal_ts} price={signal_price}"
                    )
                    evaluate_range_scalp_signal(
                        symbol=symbol,
                        range_10m=direction,
                        price=signal_price,
                        event_id=f"okx_range_10m_{signal_ts}",
                    )
                except Exception as e:
                    logger.info(f"[RANGE] {symbol} reason=exception:{e}")
                    logger.debug(f"[RANGE] {symbol}: {e}", exc_info=True)
            persist_state()
        except Exception as e:
            logger.error(f"[RANGE] Erreur scheduler: {e}")
        time.sleep(60)


# ============================================================================
# STATE
# ============================================================================

STATE_LOCK       = threading.RLock()  # RLock pour éviter deadlock (should_send appelé dans le lock)
MOMENTUM_STATE   = {}   # symbol -> {st_ai_15m, st_ai_4h, bias_1h, last_st_15m, ...}
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
            'st_ai_1h_ts':    None,
            'st_ai_30m':      None,
            'st_ai_30m_ts':   None,
            'st_ai_2h':       None,
            'st_ai_2h_ts':    None,
            'st_ai_4h':       None,
            'bias_1h':        None,
            'bias_1h_ts':     None,
            'bias_2h':        None,
            'bias_2h_ts':     None,
            'bias_20m':       None,
            'bias_30m':       None,
            'bias_30m_ts':    None,
            'williams_30m':    None,
            'williams_30m_ts': None,
            'williams_1h':     None,
            'williams_1h_ts':  None,
            'williams_2h':     None,
            'williams_2h_ts':  None,
            'range_filter_5m': None,
            'range_filter_5m_ts': None,
            'last_range_filter_5m_signal_ts': None,
            'range_filter_1m': None,
            'range_filter_1m_ts': None,
            'last_range_filter_1m_signal_ts': None,
            'range_filter_10m': None,
            'range_filter_10m_ts': None,
            'last_range_filter_10m_signal_ts': None,
            'last_st_15m':    None,
            'last_st_1h':     None,
            'st_4h_flipped':  False,
            'st_context_1m':    None,
            'st_context_lt_1m': None,
            'st_context_1m_ts': None,
            'st_context_lt_1m_ts': None,
            'st_context_3m':    None,
            'st_context_lt_3m': None,
            'st_context_3m_ts': None,
            'st_context_lt_3m_ts': None,
            'st_context_10m':   None,
            'st_context_10m_ts': None,
            'st_context_2h':    None,
            'st_context_2h_ts': None,
            'st_context_30m':   None,
            'st_context_30m_ts': None,
            'st_context_5m':    None,
            'st_context_15m':   None,
            'st_context_15m_ts': None,
            'st_context_lt_5m': None,
            'st_context_lt_1h': None,
            'st_context_1h':    None,
            'st_context_5m_ts': None,
            'st_context_1h_ts': None,
            'st_context_lt_5m_ts': None,
            'st_context_lt_1h_ts': None,
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

def parse_direction_value(val):
    normalized = str(val).strip().lower()
    if normalized in ('1', 'buy', 'long'):  return 'buy'
    if normalized in ('0', 'sell', 'short'): return 'sell'
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

def strip_html(text: str) -> str:
    import re
    return re.sub(r'<[^>]+>', '', str(text or '')).strip()


def notification_title_from_message(msg: str, fallback: str = "Scalp Bot") -> str:
    lines = [strip_html(line).strip() for line in str(msg or '').splitlines() if strip_html(line).strip()]
    if not lines:
        return fallback
    title = lines[0].replace('[', '').replace(']', '').replace('*', '').strip()
    return title[:80] or fallback


def notification_body_for_ntfy(msg: str, max_chars: int = 700) -> str:
    body = strip_html(msg)
    if len(body) <= max_chars:
        return body
    return body[:max_chars - 3].rstrip() + "..."


def ntfy_header_value(value: str, fallback: str = "Scalp Bot", max_chars: int = 120) -> str:
    clean = strip_html(value).replace('\n', ' ').strip()
    clean = clean.encode('latin-1', errors='ignore').decode('latin-1').strip()
    return clean[:max_chars] or fallback


def notification_tags_from_text(text: str):
    plain = strip_html(text).lower()
    if 'take profit' in plain or 'tp' in plain:
        return ['tada']
    if 'stop loss' in plain or 'sl' in plain:
        return ['warning']
    if 'short' in plain or 'sell' in plain:
        return ['chart_with_downwards_trend']
    if 'long' in plain or 'buy' in plain:
        return ['chart_with_upwards_trend']
    return ['chart_with_upwards_trend']


class NotificationChannel:
    def send(self, title: str, message: str, priority=5, tags=None, **kwargs) -> bool:
        raise NotImplementedError


class TelegramChannel(NotificationChannel):
    def __init__(self, token_getter, chat_getter, label='Telegram'):
        self.token_getter = token_getter
        self.chat_getter = chat_getter
        self.label = label

    def send(self, title: str, message: str, priority=5, tags=None, reply_markup=None, **kwargs) -> bool:
        tok = self.token_getter()
        chat = self.chat_getter()
        if not tok or not chat:
            logger.warning("Token ou chat_id manquant")
            return False
        payload = {"chat_id": chat, "text": message, "parse_mode": "HTML"}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info(f"{self.label} envoye")
                return True
            logger.error(f"Telegram {resp.status_code}: {resp.text[:100]}")
            return False
        except Exception as e:
            logger.error(f"Telegram error: {e}")
            return False


class NtfyChannel(NotificationChannel):
    def __init__(self, topic_getter):
        self.topic_getter = topic_getter

    def send(self, title: str, message: str, priority=5, tags=None, **kwargs) -> bool:
        topic = str(self.topic_getter() or '').strip()
        if not topic:
            return False
        url = topic if topic.startswith(('http://', 'https://')) else f"https://ntfy.sh/{topic}"
        headers = {
            'Title': ntfy_header_value(title, 'Scalp Bot'),
            'Priority': str(priority),
        }
        if tags:
            headers['Tags'] = ntfy_header_value(
                ','.join(tags) if isinstance(tags, (list, tuple)) else str(tags),
                '',
                max_chars=80,
            )
        try:
            resp = requests.post(
                url,
                data=notification_body_for_ntfy(message).encode('utf-8'),
                headers=headers,
                timeout=10,
            )
            if 200 <= resp.status_code < 300:
                logger.info("ntfy envoye")
                return True
            logger.warning(f"ntfy erreur: {resp.status_code} {resp.text[:100]}")
            return False
        except Exception as e:
            logger.error(f"ntfy error: {e}")
            return False


class NotificationManager:
    def __init__(self):
        self.channels = {}

    def register(self, name: str, channel: NotificationChannel):
        self.channels[name] = channel

    def send(self, title: str, message: str, priority=5, tags=None, channels=None, **kwargs):
        results = {}
        for name in (channels or list(self.channels.keys())):
            channel = self.channels.get(name)
            if not channel:
                continue
            results[name] = channel.send(title, message, priority=priority, tags=tags, **kwargs)
        return results


NOTIFICATIONS = NotificationManager()
NOTIFICATIONS.register(
    'telegram_scalp',
    TelegramChannel(
        lambda: os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', ''),
        lambda: os.environ.get('TELEGRAM_CHAT_ID', ''),
        label='Telegram scalpbot',
    ),
)
NOTIFICATIONS.register('ntfy', NtfyChannel(lambda: CONFIG.get('NTFY_TOPIC', '')))


def send_notification(title: str, message: str, priority=5, tags=None, telegram=True, ntfy=True, reply_markup=None):
    channels = []
    if telegram:
        channels.append('telegram_scalp')
    if ntfy:
        channels.append('ntfy')
    if tags is None:
        tags = notification_tags_from_text(f"{title}\n{message}")
    return NOTIFICATIONS.send(title, message, priority=priority, tags=tags, channels=channels, reply_markup=reply_markup)


def sanitize_scalp_notification(msg: str) -> str:
    """Normalise le titre directionnel des alertes scalp et ajoute la pastille couleur.
    N'alimente plus ntfy (voir send_light_alert) donc les emojis sont conserves."""
    text = str(msg or '')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text_joined = '\n'.join(lines)
    direction_match = re.search(r'\b(LONG|SHORT)\b', text_joined, re.IGNORECASE)
    symbol_match = re.search(r'\b[A-Z0-9]+/USDT\b', text_joined, re.IGNORECASE)
    if lines and direction_match and 'SCALP' in text_joined.upper():
        direction = direction_match.group(1).upper()
        pastille = '\U0001f7e2' if direction == 'LONG' else '\U0001f534'
        symbol = f" {symbol_match.group(0).upper()}" if symbol_match else ''
        suffix = ' - PYRAMIDING' if 'PYRAMIDING' in text_joined.upper() else ''
        lines[0] = f"{pastille} <b>SCALP {direction}{suffix}</b>{symbol}"
    return '\n'.join(lines)


def send_telegram(msg, ntfy=False):
    msg = sanitize_scalp_notification(msg)
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        tags=[],
        telegram=True,
        ntfy=ntfy,
    )
    return bool(result.get('telegram_scalp'))


def send_telegram_with_buttons(msg, callback_key):
    msg = sanitize_scalp_notification(msg)
    keyboard = {"inline_keyboard": [[
        {"text": "Activer pyramiding", "callback_data": f"pyra_on:{callback_key}"},
        {"text": "Ignorer",            "callback_data": f"pyra_off:{callback_key}"}
    ], [
        {"text": "Scalp OFF",          "callback_data": "scalp_off"}
    ]]}
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        tags=[],
        telegram=True,
        ntfy=False,
        reply_markup=keyboard,
    )
    if not result.get('telegram_scalp'):
        logger.warning("position creee sans notification Telegram")
    return bool(result.get('telegram_scalp'))


def send_light_alert(direction: str) -> bool:
    """Envoie uniquement LONG ou SHORT au listener ntfy des ampoules."""
    command = str(direction or '').strip().upper()
    if command not in ('LONG', 'SHORT'):
        logger.warning(f"[LIGHTS] Commande ignoree: {command!r}")
        return False

    result = send_notification(
        title=f"SCALP {command}",
        message=command,
        priority=5,
        tags=[],
        telegram=False,
        ntfy=True,
    )
    sent = bool(result.get('ntfy'))
    if sent:
        logger.info(f"[LIGHTS] Alerte {command} envoyee")
    else:
        logger.warning(f"[LIGHTS] Echec alerte {command}")
    return sent


def evaluate_context_scalp_secondary(symbol, ctx_1m, price, event_id):
    """Entree SCALP secondaire sur zones ST Context 30m + 1m."""
    if ctx_1m not in ('buy', 'sell'):
        return False

    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]

        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] Signal context ignore: {symbol}")
            persist_state()
            return False

        signal_direction = 'LONG' if ctx_1m == 'buy' else 'SHORT'
        exp_st = 'buy' if signal_direction == 'LONG' else 'sell'
        exp_bias = 'bull' if signal_direction == 'LONG' else 'bear'

        st_1h = m.get('st_ai_1h')
        st_30m = m.get('st_ai_30m')
        bias_30m = m.get('bias_30m')
        ctx_30m = m.get('st_context_30m')
        ctx_lt_1m = m.get('st_context_lt_1m')
        williams_1h = get_williams_filter(symbol, '1h', signal_direction, 3 * 3600)

        st_1h_fresh = bool(st_1h) and is_fresh(m.get('st_ai_1h_ts'), 3 * 3600)
        st_30m_fresh = bool(st_30m) and is_fresh(m.get('st_ai_30m_ts'), 90 * 60)
        bias_30m_fresh = bool(bias_30m) and is_fresh(m.get('bias_30m_ts'), 2 * 3600)
        ctx_30m_fresh = bool(ctx_30m) and is_fresh(m.get('st_context_30m_ts'), 90 * 60)
        ctx_1m_fresh = bool(ctx_1m) and is_fresh(m.get('st_context_1m_ts'), 5 * 60)
        ctx_lt_1m_fresh = bool(ctx_lt_1m) and is_fresh(m.get('st_context_lt_1m_ts'), 5 * 60)

        st_1h_ok = st_1h_fresh and st_1h == exp_st
        st_30m_ok = st_30m_fresh and st_30m == exp_st
        bias_30m_ok = bias_30m_fresh and bias_30m == exp_bias
        ctx_30m_ok = ctx_30m_fresh and ctx_30m == exp_st
        ctx_1m_ok = ctx_1m_fresh and ctx_1m == exp_st
        lt1m_same_block = ctx_lt_1m_fresh and ctx_lt_1m == exp_st
        momentum_30m_ok = st_30m_ok or bias_30m_ok
        secondary_ok = st_1h_ok and momentum_30m_ok and ctx_30m_ok and ctx_1m_ok and not lt1m_same_block

        logger.info(
            f"[SCALP CONTEXT SECONDARY CHECK] {symbol} dir={signal_direction} "
            f"ctx1m={ctx_1m}/{exp_st} fresh={ctx_1m_fresh} ok={ctx_1m_ok} "
            f"ctx30m={ctx_30m}/{exp_st} fresh={ctx_30m_fresh} ok={ctx_30m_ok} "
            f"st1h={st_1h}/{exp_st} fresh={st_1h_fresh} ok={st_1h_ok} "
            f"st30m={st_30m}/{exp_st} fresh={st_30m_fresh} ok={st_30m_ok} "
            f"bias30m={bias_30m}/{exp_bias} fresh={bias_30m_fresh} ok={bias_30m_ok} "
            f"momentum30m_ok={momentum_30m_ok} "
            f"will1h={williams_1h['trend']} fresh={williams_1h['fresh']} ok={williams_1h['ok']} "
            f"lt1m={ctx_lt_1m}/{exp_st} fresh={ctx_lt_1m_fresh} same_block={lt1m_same_block} "
            f"secondary={secondary_ok}"
        )

        pos_key = f"{symbol}_SCALP"
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != signal_direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None

        scalp_entry = False
        if secondary_ok and pos is None and should_send(
            symbol,
            f"scalp_context_secondary_{exp_st}",
            event_id=event_id,
            cooldown=1800,
        ):
            SCALP_POSITIONS[pos_key] = {
                'direction': signal_direction,
                'entry_count': 1,
                'signal_type': 'secondaire',
            }
            PYRA_ENABLED.pop(pos_key, None)
            scalp_entry = True

        persist_state()

    if scalp_entry:
        tg_sent = send_telegram_with_buttons(
            f"<b>SCALP {signal_direction} - ENTREE SECONDAIRE</b> {symbol}\n"
            f"--------------------\n"
            f"Direction: {signal_direction}\n"
            f"Price: ${format_price(price)}\n"
            f"Exchange: OKX\n"
            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
            f"[OK] Zone ST Context 1m: {(ctx_1m or 'NEUTRE').upper()}\n"
            f"[OK] Zone ST Context 30m: {(ctx_30m or 'NEUTRE').upper()}\n"
            f"[OK] ST AI 1H: {(st_1h or 'N/A').upper()}\n"
            f"[OK] ST AI 30m: {(st_30m or 'N/A').upper()} ({'OK' if st_30m_ok else 'option non retenue'})\n"
            f"[OK] Bias 30m: {(bias_30m or 'N/A').upper()} ({'OK' if bias_30m_ok else 'option non retenue'})\n"
            f"{format_williams_filter_line('1H', williams_1h)}\n"
            f"[ANTI-CHOP] LT 1m meme sens: {lt1m_same_block}",
            f"{symbol}_SCALP",
        )
        if not tg_sent:
            logger.warning(f"[SCALP] Entree secondaire {symbol} creee mais notification Telegram echouee")
        send_light_alert(signal_direction)
        logger.info(f"[SCALP] Entree secondaire: {symbol} {signal_direction}")
        return True
    return False


def evaluate_context30_strategy(symbol, price=0, event_id=None, source='webhook'):
    """Strategie CONTEXT30 desactivee."""
    return False
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]

        if not SCALP_ENABLED:
            logger.info(f"[CONTEXT30 OFF] Signal ignore: {symbol}")
            persist_state()
            return False

        ctx_30m = m.get('st_context_30m')
        ctx_1m = m.get('st_context_1m')
        if ctx_30m not in ('buy', 'sell') or ctx_1m not in ('buy', 'sell'):
            return False

        signal_direction = 'LONG' if ctx_1m == 'buy' else 'SHORT'
        exp_ctx = 'buy' if signal_direction == 'LONG' else 'sell'
        ctx_30m_fresh = is_fresh(m.get('st_context_30m_ts'), 90 * 60)
        ctx_1m_fresh = is_fresh(m.get('st_context_1m_ts'), 5 * 60)
        williams_1h = get_williams_filter(symbol, '1h', signal_direction, 3 * 3600)

        ctx_30m_ok = ctx_30m_fresh and ctx_30m == exp_ctx
        ctx_1m_ok = ctx_1m_fresh and ctx_1m == exp_ctx
        entry_ok = ctx_30m_ok and ctx_1m_ok

        logger.info(
            f"[CONTEXT30 CHECK] {symbol} dir={signal_direction} source={source} "
            f"ctx30m={ctx_30m}/{exp_ctx} fresh={ctx_30m_fresh} ok={ctx_30m_ok} "
            f"ctx1m={ctx_1m}/{exp_ctx} fresh={ctx_1m_fresh} ok={ctx_1m_ok} "
            f"will1h={williams_1h['trend']} fresh={williams_1h['fresh']} ok={williams_1h['ok']} "
            f"entry={entry_ok}"
        )
        if not entry_ok:
            return False

        pos_key = f"{symbol}_CONTEXT30"
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != signal_direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None

        is_entry = False
        event_key = event_id or f"context30_{symbol}_{int(time.time())}_{exp_ctx}"
        if pos is None and should_send(symbol, f"context30_entry_{exp_ctx}", event_id=event_key, cooldown=1800):
            SCALP_POSITIONS[pos_key] = {
                'direction': signal_direction,
                'entry_count': 1,
                'signal_type': 'context30',
            }
            PYRA_ENABLED.pop(pos_key, None)
            is_entry = True

        persist_state()

    if is_entry:
        tg_sent = send_telegram_with_buttons(
            f"<b>CONTEXT30 {signal_direction} - ENTREE</b> {symbol}\n"
            f"--------------------\n"
            f"Direction: {signal_direction}\n"
            f"Price: ${format_price(price)}\n"
            f"Exchange: OKX\n"
            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
            f"[OK] Zone ST Context 30m: {(ctx_30m or 'N/A').upper()}\n"
            f"{format_williams_filter_line('1H', williams_1h)}\n"
            f"[OK] Zone ST Context 1m: {(ctx_1m or 'N/A').upper()}",
            f"{symbol}_CONTEXT30",
        )
        if not tg_sent:
            logger.warning(f"[CONTEXT30] Entree {symbol} creee mais notification Telegram echouee")
        send_light_alert(signal_direction)
        logger.info(f"[CONTEXT30] Entree: {symbol} {signal_direction}")
        return True
    return False


def evaluate_range_scalp_signal(symbol, range_10m, price, event_id):
    """Evalue SCALP sur un signal Range Filter 10m confirme."""
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        m['range_filter_10m'] = range_10m
        m['range_filter_10m_ts'] = time.time()

        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] Signal ignore: {symbol}")
            persist_state()
            return

        signal_direction = 'LONG' if range_10m == 'buy' else 'SHORT'
        exp_st = 'buy' if signal_direction == 'LONG' else 'sell'
        exp_bias = 'bull' if signal_direction == 'LONG' else 'bear'
        exp_ctx = exp_st
        opp_ctx = 'sell' if signal_direction == 'LONG' else 'buy'

        st_1h = m.get('st_ai_1h')
        st_30m = m.get('st_ai_30m')
        bias_30m = m.get('bias_30m')
        ctx_10m = m.get('st_context_10m')
        ctx_1m = m.get('st_context_1m')
        ctx_lt_1m = m.get('st_context_lt_1m')
        williams_30m = get_williams_filter(symbol, '30m', signal_direction, 2 * 3600)

        st_1h_fresh = bool(st_1h) and is_fresh(m.get('st_ai_1h_ts'), 3 * 3600)
        st_30m_fresh = bool(st_30m) and is_fresh(m.get('st_ai_30m_ts'), 90 * 60)
        bias_30m_fresh = bool(bias_30m) and is_fresh(m.get('bias_30m_ts'), 2 * 3600)
        ctx_10m_fresh = bool(ctx_10m) and is_fresh(m.get('st_context_10m_ts'), 30 * 60)
        ctx_1m_fresh = bool(ctx_1m) and is_fresh(m.get('st_context_1m_ts'), 5 * 60)
        ctx_lt_1m_fresh = bool(ctx_lt_1m) and is_fresh(m.get('st_context_lt_1m_ts'), 5 * 60)

        st_1h_ok = st_1h_fresh and st_1h == exp_st
        st_30m_ok = st_30m_fresh and st_30m == exp_st
        bias_30m_ok = bias_30m_fresh and bias_30m == exp_bias
        ctx_10m_ok = ctx_10m_fresh and ctx_10m == exp_ctx
        ctx_1m_ok = ctx_1m_fresh and ctx_1m == exp_ctx
        lt1m_same_block = ctx_lt_1m_fresh and ctx_lt_1m == exp_ctx
        antichop_block = lt1m_same_block
        primary_ok = st_1h_ok and st_30m_ok and bias_30m_ok and ctx_1m_ok and not antichop_block
        secondary_ok = False
        context10m_ok = st_30m_ok and williams_30m['ok'] and ctx_10m_ok and not antichop_block
        scalp_all_ok = primary_ok or secondary_ok or context10m_ok
        signal_type = 'context10m' if context10m_ok else 'secondaire' if secondary_ok else 'principal' if primary_ok else 'blocked'

        logger.info(
            f"[SCALP RANGE CHECK] {symbol} dir={signal_direction} "
            f"range10m={range_10m}/{exp_st} "
            f"st1h={st_1h}/{exp_st} fresh={st_1h_fresh} ok={st_1h_ok} "
            f"st30m={st_30m}/{exp_st} fresh={st_30m_fresh} ok={st_30m_ok} "
            f"bias30m={bias_30m}/{exp_bias} fresh={bias_30m_fresh} ok={bias_30m_ok} "
            f"will30m={williams_30m['trend']} fresh={williams_30m['fresh']} ok={williams_30m['ok']} "
            f"ctx10m={ctx_10m}/{exp_ctx} fresh={ctx_10m_fresh} ok={ctx_10m_ok} "
            f"ctx1m={ctx_1m}/{exp_ctx} fresh={ctx_1m_fresh} ok={ctx_1m_ok} "
            f"lt1m={ctx_lt_1m}/{exp_ctx} fresh={ctx_lt_1m_fresh} same_block={lt1m_same_block} "
            f"primary={primary_ok} secondary={secondary_ok} context10m={context10m_ok} signal_type={signal_type} "
            f"will30m_quality={williams_30m['ok']}"
        )

        scalp_entry = False
        scalp_pyra = False
        pos_key = f"{symbol}_SCALP"

        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != signal_direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None

        if scalp_all_ok and pos is None and should_send(symbol, f"scalp_range_entry_{signal_type}_{exp_ctx}", event_id=event_id, cooldown=1800):
            SCALP_POSITIONS[pos_key] = {
                'direction': signal_direction,
                'entry_count': 1,
                'signal_type': signal_type,
            }
            PYRA_ENABLED.pop(pos_key, None)
            pos = SCALP_POSITIONS[pos_key]
            scalp_entry = True
        elif (pos and pos.get('direction') == signal_direction and scalp_all_ok
                and PYRA_ENABLED.get(pos_key, False)
                and should_send(symbol, f"scalp_range_pyra_{exp_ctx}", event_id=event_id, cooldown=CONFIG['PYRA_COOLDOWN'])):
            pos['entry_count'] += 1
            scalp_pyra = True
        elif not scalp_all_ok:
                logger.info(
                    f"[SCALP BLOCKED] {symbol} dir={signal_direction} "
                    f"primary={primary_ok} secondary={secondary_ok} context10m={context10m_ok} "
                    f"lt1m_same_block={lt1m_same_block} "
                    f"pos={pos['direction'] if pos else None}"
                )

        persist_state()

    if scalp_entry or scalp_pyra:
        count_txt = f" - PYRAMIDING #{pos['entry_count']}" if scalp_pyra else ""
        entry_label = {
            'principal': 'ENTREE PRINCIPALE',
            'secondaire': 'ENTREE SECONDAIRE',
            'context10m': 'ENTREE CONTEXT10M',
        }.get(pos.get('signal_type'), 'ENTREE')
        quality_txt = (
            ("<b>[QUALITE] ST AI 30m aligne</b>\n" if st_30m_ok else "")
            + ("<b>[QUALITE] Williams 30m aligne</b>\n" if williams_30m['ok'] else "")
        )
        if quality_txt:
            quality_txt += "\n"
        tg_sent = send_telegram_with_buttons(
            f"<b>SCALP {signal_direction} - {entry_label}{count_txt}</b> {symbol}\n"
            f"--------------------\n"
            f"{quality_txt}"
            f"Direction: {signal_direction}\n"
            f"Price: ${format_price(price)}\n"
            f"Exchange: OKX\n"
            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
            f"[OK] Flip Range Filter 10m: {(range_10m or 'N/A').upper()}\n"
            f"[OK] ST AI 1H: {(st_1h or 'N/A').upper()}\n"
            f"[OK] ST AI 30m: {(st_30m or 'N/A').upper()}\n"
            f"[OK] Bias 30m: {(bias_30m or 'N/A').upper()} (EMA8/SMA21)\n"
            f"{format_williams_filter_line('30m', williams_30m)}\n"
            f"[OK] Zone ST Context 10m: {(ctx_10m or 'NEUTRE').upper()}\n"
            f"[OK] Zone ST Context 1m: {(ctx_1m or 'NEUTRE').upper()}\n"
            f"[ANTI-CHOP] LT 1m meme sens: {lt1m_same_block}",
            pos_key
        )
        if not tg_sent:
            logger.warning(f"[SCALP] Entree {symbol} creee mais notification Telegram echouee")
        send_light_alert(signal_direction)
        logger.info(f"[SCALP] {'Pyramiding' if scalp_pyra else 'Entree'}: {symbol} {signal_direction}")

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
    tf_aliases = {'1': '1m', '1min': '1m', '1minute': '1m', '3': '3m', '3min': '3m', '3minute': '3m', '5': '5m', '5min': '5m', '5minute': '5m', '10': '10m', '10min': '10m', '10minute': '10m', '15': '15m', '20': '20m', '20min': '20m', '20minute': '20m', '30': '30m', '30min': '30m', '30minute': '30m', '60': '1h', '120': '2h', '2hr': '2h', '2hour': '2h', '180': '3h', '3hr': '3h', '3hour': '3h', '240': '4h', '4hr': '4h', '4hour': '4h'}
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
            m['st_ai_1h_ts'] = time.time()
            m['last_st_1h'] = prev_1h
            state_changed = True
            flipped_1h   = (prev_1h is not None and parsed is not None and parsed != prev_1h)
        elif tf == '2h':
            prev_2h = m.get('st_ai_2h')
            m['st_ai_2h'] = parsed
            m['st_ai_2h_ts'] = time.time()
            m['last_st_2h'] = prev_2h
            state_changed = True
        elif tf == '30m':
            prev_30m = m.get('st_ai_30m')
            m['st_ai_30m'] = parsed
            m['st_ai_30m_ts'] = time.time()
            m['last_st_30m'] = prev_30m
            state_changed = True
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
            m['bias_1h_ts'] = time.time()
            state_changed = True
        elif bias_val in ('bull', 'bear', 'neutral') and tf == '2h':
            m['bias_2h'] = bias_val if bias_val != 'neutral' else None
            m['bias_2h_ts'] = time.time()
            state_changed = True
        elif bias_val in ('bull', 'bear', 'neutral') and tf == '20m':
            m['bias_20m'] = bias_val if bias_val != 'neutral' else None
            state_changed = True
        elif bias_val in ('bull', 'bear', 'neutral') and tf == '30m':
            m['bias_30m'] = bias_val if bias_val != 'neutral' else None
            m['bias_30m_ts'] = time.time()
            state_changed = True

    elif alert_type in ('range_filter', 'rangefilter'):
        parsed = parse_direction_value(val)
        if parsed is None:
            logger.warning(f"[WEBHOOK] Range Filter invalide: {symbol} tf={tf} value={val!r}")
            return jsonify({'status': 'ignored', 'reason': 'invalid_range_filter'}), 200
        if tf == '10m':
            evaluate_range_scalp_signal(symbol, parsed, price, event_id)
            return jsonify({'status': 'ok'}), 200
            
    elif alert_type == 'st_context_lt' and tf in ('1m', '3m', '5m', '1h'):
        try:
            lt_val = float(val)
            lt_parsed = 'buy' if lt_val < -1.96 else 'sell' if lt_val > 1.96 else None
        except (TypeError, ValueError):
            logger.warning(f"[WEBHOOK] ST Context LT invalide: {symbol} tf={tf} value={val!r}")
            return jsonify({'status': 'ignored', 'reason': 'invalid_st_context_lt'}), 200
        if tf == '1m':
            m['st_context_lt_1m'] = lt_parsed
            m['st_context_lt_1m_ts'] = time.time()
        elif tf == '3m':
            m['st_context_lt_3m'] = lt_parsed
            m['st_context_lt_3m_ts'] = time.time()
        elif tf == '5m':
            m['st_context_lt_5m'] = lt_parsed
            m['st_context_lt_5m_ts'] = time.time()
        elif tf == '1h':
            m['st_context_lt_1h'] = lt_parsed
            m['st_context_lt_1h_ts'] = time.time()
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
        elif tf == '3m':
            m['st_context_3m'] = ctx_parsed
            m['st_context_3m_ts'] = time.time()
            state_changed = True
        elif tf == '5m':
            m['st_context_5m'] = ctx_parsed
            m['st_context_5m_ts'] = time.time()
            state_changed = True
        elif tf == '10m':
            m['st_context_10m'] = ctx_parsed
            m['st_context_10m_ts'] = time.time()
            state_changed = True
        elif tf == '15m':
            m['st_context_15m'] = ctx_parsed
            m['st_context_15m_ts'] = time.time()
            state_changed = True
        elif tf == '30m':
            m['st_context_30m'] = ctx_parsed
            m['st_context_30m_ts'] = time.time()
            state_changed = True
        elif tf == '2h':
            m['st_context_2h'] = ctx_parsed
            m['st_context_2h_ts'] = time.time()
            state_changed = True
        elif tf == '1h':
            m['st_context_1h'] = ctx_parsed
            m['st_context_1h_ts'] = time.time()
            state_changed = True

        if tf in ('1m', '30m'):
            current_ctx_1m = m.get('st_context_1m')
            if state_changed:
                persist_state()
                state_changed = False
            evaluate_context_scalp_secondary(
                symbol,
                current_ctx_1m,
                price,
                event_id=f"context_secondary_{symbol}_{tf}_{event_id}",
            )

    # Ancienne strategie CONTEXT1H desactivee.
    # ST Context 1H sert maintenant uniquement de tag haute qualite dans SCALP.
    if False and alert_type in ('st_context', 'st_context_lt', 'bias') and tf in ('1m', '5m', '1h'):
        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] Signal ignore: {symbol}")
            if state_changed:
                persist_state()
            return jsonify({'status': 'ok', 'enabled': False}), 200

        ctx_1h = m.get('st_context_1h')
        bias_1h = m.get('bias_1h')
        ctx_1m = m.get('st_context_1m')
        ctx_5m = m.get('st_context_5m')
        ctx_lt_1h = m.get('st_context_lt_1h')

        ctx_1m_fresh_c1h = bool(ctx_1m) and is_fresh(m.get('st_context_1m_ts'), 5 * 60)
        ctx_5m_fresh_c1h = bool(ctx_5m) and is_fresh(m.get('st_context_5m_ts'), 15 * 60)
        ctx_1h_fresh_c1h = bool(ctx_1h) and is_fresh(m.get('st_context_1h_ts'), 3 * 3600)
        lt_1h_fresh_c1h = bool(ctx_lt_1h) and is_fresh(m.get('st_context_lt_1h_ts'), 3 * 3600)

        if not (ctx_1m_fresh_c1h and ctx_1h_fresh_c1h):
            logger.debug(
                f"[CONTEXT1H WAITING] {symbol} "
                f"ctx1m={ctx_1m} fresh={ctx_1m_fresh_c1h} "
                f"ctx1h={ctx_1h} fresh={ctx_1h_fresh_c1h}"
            )

        if ctx_1m_fresh_c1h and ctx_1h_fresh_c1h:
            signal_direction_c1h = 'LONG' if ctx_1m == 'buy' else 'SHORT'
            exp_ctx_c1h = 'buy' if signal_direction_c1h == 'LONG' else 'sell'
            opp_ctx_c1h = 'sell' if signal_direction_c1h == 'LONG' else 'buy'
            exp_bias_c1h = 'bull' if signal_direction_c1h == 'LONG' else 'bear'

            ctx_1h_ok_c1h = ctx_1h == exp_ctx_c1h
            bias_1h_ok_c1h = bias_1h == exp_bias_c1h
            ctx_1m_ok_c1h = ctx_1m == exp_ctx_c1h
            ctx_5m_opp_block_c1h = ctx_5m_fresh_c1h and ctx_5m == opp_ctx_c1h
            lt_1h_same_block_c1h = lt_1h_fresh_c1h and ctx_lt_1h == exp_ctx_c1h
            antichop_c1h = ctx_5m_opp_block_c1h or lt_1h_same_block_c1h
            all_ok_c1h = ctx_1h_ok_c1h and bias_1h_ok_c1h and ctx_1m_ok_c1h and not antichop_c1h

            logger.info(
                f"[CONTEXT1H CHECK] {symbol} dir={signal_direction_c1h} "
                f"ctx1h={ctx_1h}/{exp_ctx_c1h} fresh={ctx_1h_fresh_c1h} "
                f"bias1h={bias_1h}/{exp_bias_c1h} "
                f"ctx1m={ctx_1m}/{exp_ctx_c1h} fresh={ctx_1m_fresh_c1h} "
                f"ctx5m={ctx_5m} fresh={ctx_5m_fresh_c1h} opp_block={ctx_5m_opp_block_c1h} "
                f"lt1h={ctx_lt_1h} fresh={lt_1h_fresh_c1h} same_block={lt_1h_same_block_c1h}"
            )

            pos_key_c1h = f"{symbol}_CONTEXT1H"
            with STATE_LOCK:
                pos_c1h = SCALP_POSITIONS.get(pos_key_c1h)
                if pos_c1h and pos_c1h.get('direction') != signal_direction_c1h:
                    SCALP_POSITIONS.pop(pos_key_c1h, None)
                    PYRA_ENABLED.pop(pos_key_c1h, None)
                    pos_c1h = None

                is_entry_c1h = bool(all_ok_c1h and pos_c1h is None)
                if is_entry_c1h and should_send(symbol, f"context1h_entry_{exp_ctx_c1h}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
                    SCALP_POSITIONS[pos_key_c1h] = {
                        'direction': signal_direction_c1h,
                        'entry_count': 1,
                        'signal_type': 'context1h_bias1h_ctx1m',
                    }
                    PYRA_ENABLED.pop(pos_key_c1h, None)
                    pos_c1h = SCALP_POSITIONS[pos_key_c1h]
                else:
                    is_entry_c1h = False

            if is_entry_c1h and pos_c1h:
                emoji = "\U0001f7e2" if signal_direction_c1h == "LONG" else "\U0001f534"
                send_telegram(
                    f"{emoji} <b>[CONTEXT 1H - ENTREE]</b> {symbol}\n"
                    f"--------------------\n"
                    f"Direction: {signal_direction_c1h}\n"
                    f"Price: ${format_price(price)}\n"
                    f"Exchange: OKX\n"
                    f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                    f"[OK] Zone ST Context 1H: {(ctx_1h or 'N/A').upper()}\n"
                    f"[OK] Bias 1H: {(bias_1h or 'N/A').upper()}\n"
                    f"[OK] Zone ST Context 1m: {(ctx_1m or 'N/A').upper()}\n"
                    f"[ANTI-CHOP] Zone ST Context 5m: {(ctx_5m or 'NEUTRE').upper()}\n"
                    f"[ANTI-CHOP] LT 1H: {(ctx_lt_1h or 'NEUTRE').upper()}"
                )
                logger.info(f"[CONTEXT1H] Entree: {symbol} {signal_direction_c1h}")
                state_changed = True
            elif not all_ok_c1h:
                logger.info(
                    f"[CONTEXT1H BLOCKED] {symbol} dir={signal_direction_c1h} "
                    f"ctx1h_ok={ctx_1h_ok_c1h} bias1h_ok={bias_1h_ok_c1h} "
                    f"ctx1m_ok={ctx_1m_ok_c1h} ctx5m_opp_block={ctx_5m_opp_block_c1h} "
                    f"lt1h_same_block={lt_1h_same_block_c1h}"
                )

    # ==================================================================
    # Logique SCALP secondaire
    # Entree secondaire : ST AI 2H + ST Context 2H + ST Context 5m
    # Qualite : ST AI 30m aligne
    # ==================================================================

    if False and (
        (alert_type == 'st_context' and tf in ('2h', '5m'))
        or (alert_type == 'supertrend' and tf == '2h')
    ):
        st_2h = m.get('st_ai_2h')
        st_30m = m.get('st_ai_30m')
        ctx_2h = m.get('st_context_2h')
        ctx_5m = m.get('st_context_5m')
        ctx_2h_fresh = bool(ctx_2h) and is_fresh(m.get('st_context_2h_ts'), 6 * 3600)
        ctx_5m_fresh = bool(ctx_5m) and is_fresh(m.get('st_context_5m_ts'), 15 * 60)
        st_2h_fresh = bool(st_2h) and is_fresh(m.get('st_ai_2h_ts'), 6 * 3600)
        st_30m_fresh = bool(st_30m) and is_fresh(m.get('st_ai_30m_ts'), 90 * 60)

        should_evaluate = bool(st_2h_fresh and ctx_2h_fresh and ctx_5m_fresh)
        if not should_evaluate:
            if state_changed:
                persist_state()
            return jsonify({'status': 'ok'}), 200

        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] Signal ignore: {symbol}")
            if state_changed:
                persist_state()
            return jsonify({'status': 'ok', 'enabled': False}), 200

        signal_direction = 'LONG' if ctx_5m == 'buy' else 'SHORT'
        exp_st_2h = 'buy' if signal_direction == 'LONG' else 'sell'
        exp_st_30m = exp_st_2h
        exp_ctx = 'buy' if signal_direction == 'LONG' else 'sell'

        st_2h_ok = st_2h_fresh and st_2h == exp_st_2h
        st_30m_ok = st_30m_fresh and st_30m == exp_st_30m
        ctx_2h_ok = ctx_2h_fresh and ctx_2h == exp_ctx
        ctx_5m_ok = ctx_5m_fresh and ctx_5m == exp_ctx
        quality_30m = st_30m_ok
        all_ok = st_2h_ok and ctx_2h_ok and ctx_5m_ok

        pos_key = f"{symbol}_SCALP"
        is_pyra = False
        with STATE_LOCK:
            pos = SCALP_POSITIONS.get(pos_key)
            if pos and pos['direction'] != signal_direction:
                SCALP_POSITIONS.pop(pos_key, None)
                PYRA_ENABLED.pop(pos_key, None)
                pos = None

            candidate = bool(all_ok and pos is None)
            if candidate and should_send(symbol, f"scalp_entry_secondaire_{exp_ctx}", event_id=event_id, cooldown=1800):
                SCALP_POSITIONS[pos_key] = {
                    'direction': signal_direction,
                    'entry_count': 1,
                    'signal_type': 'secondaire',
                }
                PYRA_ENABLED.pop(pos_key, None)
                pos = SCALP_POSITIONS[pos_key]
                is_entry = True
            else:
                is_entry = False
                # Pyramiding : position deja ouverte + nouvelle zone ST Context 5m dans le meme sens
                if (pos and pos['direction'] == signal_direction and st_2h_ok and ctx_2h_ok and ctx_5m_ok
                        and PYRA_ENABLED.get(pos_key, False)
                        and should_send(symbol, f"scalp_pyra_{exp_ctx}", event_id=event_id, cooldown=CONFIG['PYRA_COOLDOWN'])):
                    pos['entry_count'] += 1
                    is_pyra = True
                if not all_ok and not is_pyra:
                    logger.info(
                        f"[SCALP BLOCKED] {symbol} dir={signal_direction} "
                        f"st2h={st_2h}/{exp_st_2h} fresh={st_2h_fresh} ok={st_2h_ok} "
                        f"st30m={st_30m}/{exp_st_30m} fresh={st_30m_fresh} ok={st_30m_ok} "
                        f"ctx2h={ctx_2h}/{exp_ctx} fresh={ctx_2h_fresh} ok={ctx_2h_ok} "
                        f"ctx5m={ctx_5m}/{exp_ctx} fresh={ctx_5m_fresh} ok={ctx_5m_ok} "
                        f"quality30m={quality_30m} pos={pos['direction'] if pos else None}"
                    )

        if is_entry and pos:
            title = f"<b>SCALP {signal_direction} - ENTREE SECONDAIRE</b> {symbol}"
            quality_txt = (
                "<b>⭐ SCALP HAUTE QUALITE</b> (ST AI 30m aligne)\n\n"
                if quality_30m else ""
            )
            tg_sent = send_telegram_with_buttons(
                f"{title}\n"
                f"--------------------\n"
                f"{quality_txt}"
                f"Direction: {signal_direction}\n"
                f"Price: ${format_price(price)}\n"
                f"Exchange: OKX\n"
                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"[INFO] ST AI 2H: {(st_2h or 'N/A').upper()}\n"
                f"[QUALITE] ST AI 30m: {(st_30m or 'N/A').upper()}\n"
                f"[OK] Zone ST Context 2H: {(ctx_2h or 'N/A').upper()}\n"
                f"[OK] Zone ST Context 5m: {(ctx_5m or 'N/A').upper()}",
                pos_key
            )
            if not tg_sent:
                logger.warning(f"[SCALP] Entree {symbol} creee mais notification Telegram echouee")
            send_light_alert(signal_direction)
            logger.info(f"[SCALP] Entree: {symbol} {signal_direction}")
            state_changed = True

        elif is_pyra and pos:
            send_telegram(
                f"<b>SCALP {signal_direction} - PYRAMIDING #{pos['entry_count']}</b> {symbol}\n"
                f"--------------------\n"
                f"Direction: {signal_direction}\n"
                f"Price: ${format_price(price)}\n"
                f"Exchange: OKX\n"
                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"[OK] Nouvelle zone ST Context 5m: {(ctx_5m or 'N/A').upper()}\n"
                f"[INFO] ST AI 2H: {(st_2h or 'N/A').upper()}\n"
                f"[QUALITE] ST AI 30m: {(st_30m or 'N/A').upper()}\n"
                f"[INFO] Zone ST Context 2H: {(ctx_2h or 'NEUTRE').upper()}"
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
# DÉMARRAGE
# ============================================================================

def scalp_required_tv_signals():
    return [
        {
            'label': 'ST AI 1H',
            'field': 'st_ai_1h_ts',
            'max_age': 3 * 3600,
            'warmup': 4 * 3600,
        },
        {
            'label': 'ST AI 30m',
            'field': 'st_ai_30m_ts',
            'max_age': 90 * 60,
            'warmup': 2 * 3600,
        },
        {
            'label': 'ST Context 1m',
            'field': 'st_context_1m_ts',
            'max_age': 5 * 60,
            'warmup': 10 * 60,
        },
        {
            'label': 'ST Context LT 1m',
            'field': 'st_context_lt_1m_ts',
            'max_age': 5 * 60,
            'warmup': 10 * 60,
        },
        {
            'label': 'ST Context 30m',
            'field': 'st_context_30m_ts',
            'max_age': 90 * 60,
            'warmup': 2 * 3600,
        },
        {
            'label': 'ST Context 10m',
            'field': 'st_context_10m_ts',
            'max_age': 30 * 60,
            'warmup': 45 * 60,
        },
    ]


def scalp_watchdog_max_age(symbol, req):
    """Tolere des updates ST Context 1m moins frequents sur CRV/CVX.

    La strategie garde ses propres checks de fraicheur a 5 minutes pour entrer
    en trade. Cette tolerance ne sert qu'a eviter le bruit du watchdog.
    """
    if (
        symbol in {'CRV/USDT', 'CVX/USDT'}
        and req.get('field') in {'st_context_1m_ts', 'st_context_lt_1m_ts'}
    ):
        return 30 * 60
    return req['max_age']


def scalp_tv_signal_watchdog():
    """Surveille les webhooks TradingView critiques du scalpbot, asset par asset."""
    bot_start_time = time.time()
    time.sleep(10 * 60)
    logger.info("[TV SIGNAL WATCHDOG] Scalp demarre")
    while True:
        time.sleep(10 * 60)
        if not SCALP_ENABLED:
            continue
        now = time.time()
        uptime = now - bot_start_time
        issues = []
        with STATE_LOCK:
            symbols = list(CONFIG['SYMBOLS'].keys())
            state_copy = {s: dict(MOMENTUM_STATE.get(s, {})) for s in symbols}

        for req in scalp_required_tv_signals():
            if uptime < req['warmup']:
                continue
            missing = []
            stale = []
            for symbol in symbols:
                ts = state_copy.get(symbol, {}).get(req['field'])
                max_age = scalp_watchdog_max_age(symbol, req)
                if ts is None:
                    missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > max_age:
                    stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 60))
            if missing or stale:
                details = []
                if missing:
                    details.append("jamais recu: " + ", ".join(missing))
                if stale:
                    details.append("perime: " + ", ".join(f"{sym} {age:.0f}m" for sym, age in stale))
                issues.append(f"- {req['label']}: " + " | ".join(details))

        if issues and should_send('GLOBAL', 'scalp_tv_signal_watchdog', cooldown=1800):
            send_telegram(
                "<b>[ALERTE] Signaux TradingView scalp manquants</b>\n"
                "--------------------\n"
                + "\n".join(issues)
                + "\n\nVerifier les alertes TradingView / relay bot principal.",
                ntfy=True,
            )
            logger.warning(f"[TV SIGNAL WATCHDOG] Scalp issues: {issues}")


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

    threading.Thread(target=update_bias_30m, daemon=True).start()
    threading.Thread(target=update_range_filter_10m, daemon=True).start()
    threading.Thread(target=scalp_tv_signal_watchdog, daemon=True).start()

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
        f"SCALP: Range Filter 10m + ST AI 1H + ST AI 30m + Bias 30m + Context 1m\n"
        f"Range Filter 10m: calcule par le bot via OKX\n"
        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}"
        ,
        ntfy=False,
    )

if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    t = threading.Thread(target=startup, daemon=True)
    t.start()
