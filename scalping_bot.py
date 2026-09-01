#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot V3
# Principale : ZALT 30m + Bias 10m (EMA13/SMA30) + flip ZALT 1m
# Secondaire : ZALT 30m + ST Context 3m + flip ZALT 1m
# Service Railway séparé — alertes uniquement, pas d'exécution exchange

import json
import time
import requests
import logging
import threading
import os
import re
import redis as redis_lib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=4)

CONFIG = {
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', ''),
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
    'NTFY_TOPIC': os.environ.get('NTFY_TOPIC', ''),
    'MIN_COOLDOWN': 900,
    'SYMBOLS': {
        'BTC/USDT': {'exchange': 'okx'},
        'CRV/USDT': {'exchange': 'okx'},
        'CVX/USDT': {'exchange': 'okx'},
        'ETH/USDT': {'exchange': 'okx'},
        'LINK/USDT': {'exchange': 'okx'},
        'XRP/USDT': {'exchange': 'okx'},
    },
}

STATE_LOCK = threading.RLock()
MOMENTUM_STATE = {}
LAST_SIGNALS = {}
LAST_SIGNAL_EVENTS = {}
SCALP_ENABLED = True
REDIS_CLIENT = None


def fetch_ohlcv_okx(symbol, tf, limit=100):
    try:
        inst_id = symbol.replace('/', '-')
        bar_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1H', '2h': '2H', '4h': '4H', '1d': '1D',
        }
        bar = bar_map.get(tf, '1H')
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logger.debug(f"[OKX] {symbol} {tf} HTTP {resp.status_code}")
            return None
        body = resp.json()
        if body.get('code') not in (None, '0'):
            logger.debug(f"[OKX] {symbol} {tf} code={body.get('code')} msg={body.get('msg')}")
            return None
        data = body.get('data', [])
        if not data:
            return None
        df = pd.DataFrame(data, columns=['ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
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
    try:
        if df is None or len(df) < sma_len:
            return None
        close = df['close']
        ema = close.ewm(span=ema_len, adjust=False).mean().iloc[-1]
        sma = close.rolling(sma_len).mean().iloc[-1]
        c = close.iloc[-1]
        if c > ema and ema > sma:
            return 'bull'
        if c < ema and ema < sma:
            return 'bear'
        return None
    except Exception as e:
        logger.debug(f"[BIAS] calc failed: {e}")
        return None


def keep_confirmed_candles(df, timeframe_minutes):
    if df is None or df.empty:
        return None
    duration_ms = int(timeframe_minutes * 60 * 1000)
    now_ms = int(time.time() * 1000)
    confirmed = df[df['ts'].astype('int64') + duration_ms <= now_ms].copy()
    if confirmed.empty:
        return None
    return confirmed.reset_index(drop=True)


def build_confirmed_10m_candles(df_5m):
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
        ts=('bucket', 'first'),
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum'),
    )


def update_bias_10m():
    logger.info("Scheduler Bias 10m demarre")
    while True:
        try:
            results = {}
            for symbol in list(CONFIG['SYMBOLS'].keys()):
                try:
                    df_5m = fetch_ohlcv_okx(symbol, '5m', limit=160)
                    if df_5m is None:
                        logger.info(f"[BIAS] {symbol} bias10m=None reason=fetch_failed")
                        results[symbol] = {'bias': None, 'price': None}
                        continue
                    df = build_confirmed_10m_candles(df_5m)
                    if df is None or len(df) < 30:
                        count = 0 if df is None else len(df)
                        logger.info(f"[BIAS] {symbol} bias10m=None reason=insufficient_confirmed_candles ({count}/30)")
                        results[symbol] = {'bias': None, 'price': None}
                        continue
                    bias = calc_bias(df, ema_len=13, sma_len=30)
                    price = float(df['close'].iloc[-1]) if len(df) else None
                    results[symbol] = {'bias': bias, 'price': price}
                    logger.info(f"[BIAS] {symbol} bias10m={bias} price={price}")
                except Exception as e:
                    logger.info(f"[BIAS] {symbol} bias10m=None reason=exception:{e}")
                    results[symbol] = {'bias': None, 'price': None}

            with STATE_LOCK:
                for symbol, result in results.items():
                    init_symbol(symbol)
                    MOMENTUM_STATE[symbol]['bias_10m'] = result.get('bias')
                    MOMENTUM_STATE[symbol]['bias_10m_ts'] = time.time()
                persist_state()

            bias_ok_count = sum(1 for r in results.values() if r.get('bias') is not None)
            fetch_ok_count = sum(1 for r in results.values() if r.get('price') is not None)
            logger.info(
                f"[BIAS] Mise a jour Bias 10m terminee "
                f"({bias_ok_count}/{len(CONFIG['SYMBOLS'])} non-neutre, "
                f"{fetch_ok_count}/{len(CONFIG['SYMBOLS'])} fetch OK)"
            )
        except Exception as e:
            logger.error(f"[BIAS] Erreur: {e}")
        time.sleep(300)


def init_redis():
    global REDIS_CLIENT
    url = CONFIG.get('REDIS_URL', '')
    if not url:
        logger.warning("REDIS_URL non defini — demarrage sans Redis")
        return
    try:
        REDIS_CLIENT = redis_lib.from_url(url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info("Redis connecte")
    except Exception as e:
        logger.error(f"Redis connexion: {e}")
        REDIS_CLIENT = None


def persist_state():
    if not REDIS_CLIENT:
        return
    try:
        payload = {
            'momentum': dict(MOMENTUM_STATE),
            'signals': dict(LAST_SIGNALS),
            'events': dict(LAST_SIGNAL_EVENTS),
            'enabled': SCALP_ENABLED,
        }
        REDIS_CLIENT.set('scalp_bot_state', json.dumps(payload))
    except Exception as e:
        logger.error(f"Redis save error: {e}")


def load_state():
    global MOMENTUM_STATE, LAST_SIGNALS, LAST_SIGNAL_EVENTS, SCALP_ENABLED
    if not REDIS_CLIENT:
        return
    try:
        raw = REDIS_CLIENT.get('scalp_bot_state')
        if not raw:
            return
        payload = json.loads(raw)
        MOMENTUM_STATE = payload.get('momentum', {})
        LAST_SIGNALS = payload.get('signals', {})
        LAST_SIGNAL_EVENTS = payload.get('events', {})
        SCALP_ENABLED = bool(payload.get('enabled', True))
        stale = [s for s in list(MOMENTUM_STATE) if s not in CONFIG['SYMBOLS']]
        for s in stale:
            del MOMENTUM_STATE[s]
        logger.info(f"State Redis charge ({len(MOMENTUM_STATE)} assets)")
    except Exception as e:
        logger.error(f"Redis load error: {e}")


def init_symbol(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_10m': None,
            'bias_10m_ts': None,
            'zalt_1m': None,
            'zalt_1m_ts': None,
            'last_zalt_1m_signal_ts': None,
            'zalt_30m': None,
            'zalt_30m_ts': None,
            'st_context_3m': None,
            'st_context_3m_ts': None,
            'st_context_3m_raw': None,
        }


def format_price(price):
    if price is None:
        return '?'
    try:
        p = float(price)
        if p >= 1000:
            return f"{p:,.0f}"
        if p >= 1:
            return f"{p:.4f}"
        if p >= 0.01:
            return f"{p:.5f}"
        return f"{p:.8f}"
    except Exception:
        return str(price)


def parse_zalt_value(val):
    normalized = str(val).strip().lower()
    if normalized in ('1', 'buy', 'long', 'bull', 'bullish'):
        return 'buy'
    if normalized in ('0', '-1', 'sell', 'short', 'bear', 'bearish'):
        return 'sell'
    return None


def is_fresh(ts, max_age_seconds):
    try:
        return ts is not None and (time.time() - float(ts)) <= max_age_seconds
    except (TypeError, ValueError):
        return False


def should_send(symbol, key, cooldown=900, event_id=None):
    now = time.time()
    k = f"{symbol}:{key}"
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
        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        payload = {"chat_id": chat, "text": message, "parse_mode": "HTML"}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info(f"{self.label} envoye")
                return True
            if resp.status_code == 400 and "can't parse entities" in resp.text.lower():
                plain = strip_html(message) or strip_html(title) or "Scalp alert"
                fallback_payload = {"chat_id": chat, "text": plain}
                if reply_markup:
                    fallback_payload['reply_markup'] = reply_markup
                fallback_resp = requests.post(url, json=fallback_payload, timeout=10)
                if fallback_resp.status_code == 200:
                    logger.info(f"{self.label} envoye en texte brut")
                    return True
                logger.error(f"Telegram fallback {fallback_resp.status_code}: {fallback_resp.text[:100]}")
                return False
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
    return NOTIFICATIONS.send(
        title, message, priority=priority, tags=tags, channels=channels, reply_markup=reply_markup
    )


def sanitize_scalp_notification(msg: str) -> str:
    text = str(msg or '')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text_joined = '\n'.join(lines)
    direction_match = re.search(r'\b(LONG|SHORT)\b', text_joined, re.IGNORECASE)
    symbol_match = re.search(r'\b[A-Z0-9]+/USDT\b', text_joined, re.IGNORECASE)
    if lines and direction_match and 'SCALP' in text_joined.upper():
        direction = direction_match.group(1).upper()
        pastille = '🟢' if direction == 'LONG' else '🔴'
        symbol = f" {symbol_match.group(0).upper()}" if symbol_match else ''
        lines[0] = f"{pastille} <b>SCALP {direction}</b>{symbol}"
    return '\n'.join(lines)


def send_telegram(msg, ntfy=True):
    msg = sanitize_scalp_notification(msg)
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        telegram=True,
        ntfy=ntfy,
    )
    return bool(result.get('telegram_scalp'))


def send_telegram_with_buttons(msg):
    msg = sanitize_scalp_notification(msg)
    keyboard = {"inline_keyboard": [[
        {"text": "Scalp ON", "callback_data": "scalp_on"},
        {"text": "Scalp OFF", "callback_data": "scalp_off"},
    ]]}
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        telegram=True,
        ntfy=True,
        reply_markup=keyboard,
    )
    if not result.get('telegram_scalp'):
        logger.warning("alerte creee sans notification Telegram")
    return bool(result.get('telegram_scalp'))


def evaluate_scalp_v3(symbol, trigger_dir=None, price=0, event_id=None, trigger_label="state_refresh"):
    if trigger_dir not in (None, 'buy', 'sell'):
        return False

    notify_payload = None
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            logger.info(f"[SCALP V3 OFF] Signal ignore: {symbol}")
            return False

        directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
        selected = None
        for exp in directions:
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            exp_bias = 'bull' if exp == 'buy' else 'bear'
            zalt30 = m.get('zalt_30m')
            bias10 = m.get('bias_10m')
            zalt1 = m.get('zalt_1m')
            zalt30_ok = is_fresh(m.get('zalt_30m_ts'), 90 * 60) and zalt30 == exp
            bias10_ok = bool(bias10) and is_fresh(m.get('bias_10m_ts'), 45 * 60) and bias10 == exp_bias
            zalt1_ok = is_fresh(m.get('zalt_1m_ts'), 5 * 60) and zalt1 == exp
            flip_ok = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
            entry_ok = zalt30_ok and bias10_ok and zalt1_ok and flip_ok
            logger.info(
                f"[SCALP V3 CHECK] {symbol} trigger={trigger_label} dir={direction} "
                f"zalt30={zalt30}/{exp} ok={zalt30_ok} "
                f"bias10={bias10}/{exp_bias} ok={bias10_ok} "
                f"zalt1={zalt1}/{exp} ok={zalt1_ok} flip={flip_ok} entry={entry_ok}"
            )
            if entry_ok:
                selected = (exp, direction, zalt30, bias10, zalt1)
                break

        if selected is None:
            return False
        exp, direction, zalt30, bias10, zalt1 = selected
        if not should_send(symbol, f"scalp_v3_entry_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
            return False
        persist_state()
        notify_payload = (direction, zalt30, bias10, zalt1)

    direction, zalt30, bias10, zalt1 = notify_payload
    msg = (
        f"<b>SCALP {direction} - V3</b> {symbol}\n"
        f"--------------------\n"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"[OK] ZALT 30m: {(zalt30 or 'N/A').upper()}\n"
        f"[OK] Bias 10m: {(bias10 or 'N/A').upper()}\n"
        f"[OK] Flip ZALT 1m: {(zalt1 or 'N/A').upper()}"
    )
    if not send_telegram_with_buttons(msg):
        logger.warning(f"[SCALP V3] Entree {symbol} creee mais notification Telegram echouee")
    logger.info(f"[SCALP V3] Entree: {symbol} {direction}")
    return True


def evaluate_scalp_v3_secondary(symbol, trigger_dir=None, price=0, event_id=None, trigger_label="state_refresh"):
    if trigger_dir not in (None, 'buy', 'sell'):
        return False

    notify_payload = None
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            return False

        directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
        selected = None
        for exp in directions:
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            zalt30 = m.get('zalt_30m')
            ctx3 = m.get('st_context_3m')
            zalt1 = m.get('zalt_1m')
            zalt30_ok = is_fresh(m.get('zalt_30m_ts'), 90 * 60) and zalt30 == exp
            ctx3_ok = is_fresh(m.get('st_context_3m_ts'), 10 * 60) and ctx3 == exp
            zalt1_ok = is_fresh(m.get('zalt_1m_ts'), 5 * 60) and zalt1 == exp
            flip_ok = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
            entry_ok = zalt30_ok and ctx3_ok and zalt1_ok and flip_ok
            logger.info(
                f"[SCALP V3 SECONDARY CHECK] {symbol} trigger={trigger_label} dir={direction} "
                f"zalt30={zalt30}/{exp} ok={zalt30_ok} "
                f"ctx3={ctx3}/{exp} ok={ctx3_ok} "
                f"zalt1={zalt1}/{exp} ok={zalt1_ok} flip={flip_ok} entry={entry_ok}"
            )
            if entry_ok:
                selected = (exp, direction, zalt30, ctx3, zalt1)
                break

        if selected is None:
            return False
        exp, direction, zalt30, ctx3, zalt1 = selected
        if not should_send(symbol, f"scalp_v3_secondary_entry_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
            return False
        persist_state()
        notify_payload = (direction, zalt30, ctx3, zalt1)

    direction, zalt30, ctx3, zalt1 = notify_payload
    msg = (
        f"<b>SCALP {direction} - V3 SECONDAIRE</b> {symbol}\n"
        f"--------------------\n"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"[OK] ZALT 30m: {(zalt30 or 'N/A').upper()}\n"
        f"[OK] Zone ST Context 3m: {(ctx3 or 'N/A').upper()}\n"
        f"[OK] Flip ZALT 1m: {(zalt1 or 'N/A').upper()}"
    )
    if not send_telegram_with_buttons(msg):
        logger.warning(f"[SCALP V3 SECONDARY] Entree {symbol} creee mais notification Telegram echouee")
    logger.info(f"[SCALP V3 SECONDARY] Entree: {symbol} {direction}")
    return True


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        logger.warning("[WEBHOOK] Requete sans JSON")
        return jsonify({'status': 'error', 'reason': 'no_json'}), 400
    WEBHOOK_EXECUTOR.submit(run_webhook_job, data)
    return jsonify({'status': 'ok', 'queued': True}), 200


def run_webhook_job(data):
    try:
        with app.app_context():
            process_webhook(data)
    except Exception:
        logger.exception("[WEBHOOK] Erreur non geree dans le job async")


def process_webhook(data):
    if not data:
        logger.warning("[WEBHOOK] Donnees vides")
        return

    raw_symbol = str(data.get('symbol', '')).strip().upper()
    tf = str(data.get('tf', '')).strip().lower()
    alert_type = str(data.get('type', '')).strip().lower()
    val = data.get('value')
    price = data.get('price', 0)
    event_id = data.get('event_id') or data.get('time') or str(time.time())

    tf_aliases = {
        '1': '1m', '1min': '1m', '1minute': '1m',
        '3': '3m', '3min': '3m', '3minute': '3m',
        '30': '30m', '30min': '30m', '30minute': '30m',
    }
    tf = tf_aliases.get(tf, tf)
    alert_type_aliases = {
        'zerolagtrendsignal': 'zalt',
        'zerolagtrendsignals': 'zalt',
        'zero_lag_trend_signal': 'zalt',
        'zero_lag_trend_signals': 'zalt',
        'zls': 'zalt',
    }
    alert_type = alert_type_aliases.get(alert_type.replace(' ', '').replace('-', '_'), alert_type)

    if '/' not in raw_symbol:
        for q in ['USDT', 'USDC']:
            if raw_symbol.endswith(q):
                raw_symbol = raw_symbol[:-len(q)] + '/' + q
                break
    if not raw_symbol.endswith('/USDT'):
        raw_symbol = raw_symbol.replace('/USDC', '/USDT')
    symbol = raw_symbol

    if symbol not in CONFIG['SYMBOLS']:
        return

    zalt_signal = str(data.get('signal') or data.get('event') or '').strip().lower()
    parsed_zalt = parse_zalt_value(val) if alert_type == 'zalt' else None

    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        logger.info(f"Webhook: {symbol} | tf={tf} | type={alert_type} | val={val} | signal={zalt_signal or '-'}")

        if alert_type == 'zalt':
            if parsed_zalt is None:
                logger.warning(f"[WEBHOOK] ZALT invalide: {symbol} tf={tf} value={val!r}")
                return
            if tf in ('1m', '30m'):
                m[f'zalt_{tf}'] = parsed_zalt
                m[f'zalt_{tf}_ts'] = time.time()
                if tf == '1m' and zalt_signal in ('trend_flip', 'flip'):
                    m['last_zalt_1m_signal_ts'] = time.time()
                persist_state()
            else:
                logger.info(f"[ZALT] {symbol} tf={tf} ignore: timeframe non utilise par SCALP V3")
                return

        elif alert_type == 'st_context' and tf == '3m':
            try:
                ctx_val = float(val)
                ctx_parsed = 'buy' if ctx_val < -1.96 else 'sell' if ctx_val > 1.96 else None
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] ST Context invalide: {symbol} tf={tf} value={val!r}")
                return
            m['st_context_3m'] = ctx_parsed
            m['st_context_3m_ts'] = time.time()
            m['st_context_3m_raw'] = ctx_val
            persist_state()
        else:
            return

    trigger_dir = None
    if alert_type == 'zalt' and tf == '1m' and zalt_signal in ('trend_flip', 'flip'):
        trigger_dir = parsed_zalt

    if alert_type == 'zalt' and tf in ('1m', '30m'):
        evaluate_scalp_v3(
            symbol,
            trigger_dir=trigger_dir,
            price=price,
            event_id=f"scalp_v3_{symbol}_{tf}_{alert_type}_{event_id}",
            trigger_label=f"{alert_type}_{tf}",
        )
        evaluate_scalp_v3_secondary(
            symbol,
            trigger_dir=trigger_dir,
            price=price,
            event_id=f"scalp_v3_secondary_{symbol}_{tf}_{alert_type}_{event_id}",
            trigger_label=f"{alert_type}_{tf}",
        )

    if alert_type == 'st_context' and tf == '3m':
        evaluate_scalp_v3_secondary(
            symbol,
            price=price,
            event_id=f"scalp_v3_secondary_ctx3_{symbol}_{event_id}",
            trigger_label=f"{alert_type}_{tf}",
        )


@app.route('/telegram_callback', methods=['POST'])
def telegram_callback():
    tg_secret = os.environ.get('SCALP_TELEGRAM_SECRET', '')
    if tg_secret and request.headers.get('X-Telegram-Bot-Api-Secret-Token', '') != tg_secret:
        return jsonify({'ok': False}), 403
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': True}), 200
    try:
        cb = data.get('callback_query', {})
        cb_id = cb.get('id')
        cb_data = cb.get('data', '')
        chat_id = cb.get('message', {}).get('chat', {}).get('id')
        msg_id = cb.get('message', {}).get('message_id')
        user = cb.get('from', {}).get('first_name', 'User')
        tok = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
        if tok and cb_id:
            requests.post(
                f"https://api.telegram.org/bot{tok}/answerCallbackQuery",
                json={"callback_query_id": cb_id},
                timeout=5,
            )
        global SCALP_ENABLED
        if cb_data == 'scalp_off':
            with STATE_LOCK:
                SCALP_ENABLED = False
                persist_state()
            logger.info(f"[SCALP] Desactive par Telegram ({user})")
            if tok and chat_id and msg_id:
                requests.post(
                    f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                    json={
                        "chat_id": chat_id,
                        "message_id": msg_id,
                        "reply_markup": {"inline_keyboard": [[
                            {"text": "Scalp OFF", "callback_data": "noop"},
                            {"text": "Scalp ON", "callback_data": "scalp_on"},
                        ]]},
                    },
                    timeout=5,
                )
        elif cb_data == 'scalp_on':
            with STATE_LOCK:
                SCALP_ENABLED = True
                persist_state()
            logger.info(f"[SCALP] Active par Telegram ({user})")
            if tok and chat_id and msg_id:
                requests.post(
                    f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                    json={
                        "chat_id": chat_id,
                        "message_id": msg_id,
                        "reply_markup": {"inline_keyboard": [[
                            {"text": "Scalp ON", "callback_data": "noop"},
                            {"text": "Scalp OFF", "callback_data": "scalp_off"},
                        ]]},
                    },
                    timeout=5,
                )
    except Exception as e:
        logger.error(f"[CALLBACK] Erreur: {e}")
    return jsonify({'ok': True}), 200


@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'status': 'ok',
        'bot': 'Scalping Bot V3',
        'enabled': SCALP_ENABLED,
        'assets': len(CONFIG['SYMBOLS']),
    })


@app.route('/scalp_status', methods=['GET'])
def scalp_status():
    return jsonify({
        'status': 'ok',
        'enabled': SCALP_ENABLED,
        'assets': len(CONFIG['SYMBOLS']),
    })


def normalize_symbol_for_debug(raw_symbol):
    symbol = (raw_symbol or '').strip().upper()
    if not symbol:
        return ''
    if '/' not in symbol:
        for quote in ('USDT', 'USDC'):
            if symbol.endswith(quote):
                symbol = symbol[:-len(quote)] + '/' + quote
                break
        else:
            symbol = f"{symbol}/USDT"
    if symbol.endswith('/USDC'):
        symbol = symbol.replace('/USDC', '/USDT')
    return symbol


def signal_age_seconds(ts):
    if not ts:
        return None
    try:
        return round(time.time() - float(ts), 1)
    except (TypeError, ValueError):
        return None


def signal_debug_payload(state, field, max_age):
    ts = state.get(f'{field}_ts')
    age = signal_age_seconds(ts)
    return {
        'value': state.get(field),
        'raw': state.get(f'{field}_raw'),
        'ts': ts,
        'age_sec': age,
        'fresh': bool(state.get(field)) and age is not None and age <= max_age,
    }


@app.route('/debug_symbol', methods=['GET'])
def debug_symbol():
    secret = os.environ.get('ADMIN_SECRET', '')
    if not secret or request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    symbol = normalize_symbol_for_debug(request.args.get('symbol', ''))
    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({
            'status': 'error',
            'reason': 'not_in_watchlist',
            'symbol': symbol,
            'available_symbols': sorted(CONFIG['SYMBOLS'].keys()),
        }), 404
    with STATE_LOCK:
        init_symbol(symbol)
        m = dict(MOMENTUM_STATE.get(symbol, {}))
        zalt1 = m.get('zalt_1m')
        direction = 'LONG' if zalt1 == 'buy' else 'SHORT' if zalt1 == 'sell' else None
        exp_zalt = 'buy' if direction == 'LONG' else 'sell' if direction == 'SHORT' else None
        exp_bias = 'bull' if direction == 'LONG' else 'bear' if direction == 'SHORT' else None
        zalt30 = signal_debug_payload(m, 'zalt_30m', 90 * 60)
        zalt1_sig = signal_debug_payload(m, 'zalt_1m', 5 * 60)
        bias10 = signal_debug_payload(m, 'bias_10m', 45 * 60)
        ctx3m = signal_debug_payload(m, 'st_context_3m', 10 * 60)
        flip_fresh = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
        if direction:
            zalt30_ok = zalt30['fresh'] and zalt30['value'] == exp_zalt
            bias10_ok = bias10['fresh'] and bias10['value'] == exp_bias
            ctx3m_ok = ctx3m['fresh'] and ctx3m['value'] == exp_zalt
            primary_ok = zalt30_ok and bias10_ok and flip_fresh
            secondary_ok = zalt30_ok and ctx3m_ok and flip_fresh
        else:
            zalt30_ok = bias10_ok = ctx3m_ok = primary_ok = secondary_ok = False
        return jsonify({
            'status': 'ok',
            'symbol': symbol,
            'enabled': SCALP_ENABLED,
            'now_shanghai': datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'scalp_v3': {
                'direction_from_zalt_1m': direction,
                'expected_zalt': exp_zalt,
                'expected_bias': exp_bias,
                'flip_1m_fresh': flip_fresh,
                'principale_ok': primary_ok,
                'secondaire_ok': secondary_ok,
                'zalt30_ok': zalt30_ok,
                'bias10_ok': bias10_ok,
                'ctx3m_ok': ctx3m_ok,
            },
            'signals': {
                'zalt_30m': zalt30,
                'zalt_1m': zalt1_sig,
                'last_zalt_1m_signal_ts': m.get('last_zalt_1m_signal_ts'),
                'bias_10m': bias10,
                'st_context_3m': ctx3m,
            },
        })


@app.route('/test_ntfy', methods=['POST'])
def test_ntfy():
    secret = os.environ.get('ADMIN_SECRET', '')
    if not secret or request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    result = send_notification(
        title='SCALPBOT TEST NTFY',
        message=f"Test ntfy scalpbot - {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')} Shanghai",
        priority=5,
        tags=['bell'],
        telegram=False,
        ntfy=True,
    )
    ok = bool(result.get('ntfy'))
    return jsonify({'status': 'ok' if ok else 'error', 'result': result}), (200 if ok else 502)


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
    if not secret or request.headers.get('X-Admin-Secret') != secret:
        return jsonify({'error': 'unauthorized'}), 401
    with STATE_LOCK:
        LAST_SIGNALS.clear()
        LAST_SIGNAL_EVENTS.clear()
        persist_state()
    main_url = os.environ.get('MAIN_BOT_URL', '').rstrip('/')
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
                data = resp.json()
                logger.info(f"[RESET] sync_scalp: sent={len(data.get('sent', []))} errors={len(data.get('errors', []))}")
            except Exception as e:
                logger.warning(f"[RESET] sync_scalp echoue: {e}")
        threading.Thread(target=_sync_after_reset, daemon=True).start()
    return jsonify({'status': 'reset'}), 200


def scalp_required_tv_signals():
    return [
        {'label': 'ZALT 30m', 'field': 'zalt_30m_ts', 'max_age': 90 * 60, 'warmup': 2 * 60 * 60},
        {'label': 'ZALT 1m', 'field': 'zalt_1m_ts', 'max_age': 5 * 60, 'warmup': 10 * 60},
        {'label': 'ST Context 3m', 'field': 'st_context_3m_ts', 'max_age': 10 * 60, 'warmup': 20 * 60},
    ]


def scalp_tv_signal_watchdog():
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
                if ts is None:
                    missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > req['max_age']:
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
    tok = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
    base_url = os.environ.get('SCALP_PUBLIC_URL', '').rstrip('/')
    if base_url and not base_url.startswith(('https://', 'http://')):
        base_url = f'https://{base_url}'
    if tok and base_url:
        try:
            wh_url = f"{base_url}/telegram_callback"
            wh_payload = {'url': wh_url}
            tg_secret = os.environ.get('SCALP_TELEGRAM_SECRET', '')
            if tg_secret:
                wh_payload['secret_token'] = tg_secret
            resp_wh = requests.post(
                f"https://api.telegram.org/bot{tok}/setWebhook",
                json=wh_payload,
                timeout=10,
            )
            if resp_wh.status_code == 200 and resp_wh.json().get('ok'):
                logger.info(f"Telegram webhook configure: {wh_url}")
            else:
                logger.warning(f"Telegram webhook erreur: {resp_wh.text[:100]}")
        except Exception as e:
            logger.warning(f"Webhook setup: {e}")

    threading.Thread(target=update_bias_10m, daemon=True).start()
    threading.Thread(target=scalp_tv_signal_watchdog, daemon=True).start()

    main_url = os.environ.get('MAIN_BOT_URL', '').rstrip('/')
    if main_url and not main_url.startswith(('https://', 'http://')):
        main_url = f'https://{main_url}'
    admin_secret = os.environ.get('ADMIN_SECRET', '')
    if main_url and admin_secret:
        def _sync():
            time.sleep(5)
            try:
                resp = requests.post(
                    f'{main_url}/sync_scalp',
                    headers={'X-Admin-Secret': admin_secret},
                    timeout=15,
                )
                if not 200 <= resp.status_code < 300:
                    raise RuntimeError(f"sync_scalp HTTP {resp.status_code}: {resp.text[:200]}")
                data = resp.json()
                logger.info(f"[STARTUP] sync_scalp: sent={len(data.get('sent', []))} errors={len(data.get('errors', []))}")
            except Exception as e:
                logger.warning(f"[STARTUP] sync_scalp echoue: {e}")
        threading.Thread(target=_sync, daemon=True).start()

    send_telegram(
        "<b>Scalping Bot demarre</b>\n"
        "--------------------\n"
        f"Assets: {len(CONFIG['SYMBOLS'])}\n"
        "Strategie active: SCALP V3\n"
        "Principale: ZALT 30m + Bias 10m + flip ZALT 1m\n"
        "Secondaire: ZALT 30m + ST Context 3m + flip ZALT 1m\n"
        f"{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}",
        ntfy=False,
    )


if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    threading.Thread(target=startup, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)