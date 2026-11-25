#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot : SCALP A et B actives sur six actifs.
# SCALP A = ZALT 2H + Bias 1H + RCI 1H; B conserve Bias 2H + CTX 15m.
# Strategies actives avec routage Telegram separe.

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=4)

CONFIG = {
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', ''),
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
    'NTFY_TOPIC': os.environ.get('NTFY_TOPIC', ''),
    'MIN_COOLDOWN': 1800,
    'ENABLE_SCALP': os.environ.get('ENABLE_SCALP', os.environ.get('ENABLE_SCALP_BTC', '1')) == '1',
    'ENABLE_SCALP_BTC_15M': os.environ.get('ENABLE_SCALP_BTC_15M', '0') == '1',
    'ENABLE_SCALP_CONTINUATION': os.environ.get('ENABLE_SCALP_CONTINUATION', '1') == '1',
    'ENABLE_SCALP_CONTINUATION_B': os.environ.get('ENABLE_SCALP_CONTINUATION_B', '0') == '1',
    'SYMBOLS': {
        'AAVE/USDT': {'exchange': 'okx'},
        'ADA/USDT': {'exchange': 'okx'},
        'APT/USDT': {'exchange': 'okx'},
        'ARB/USDT': {'exchange': 'okx'},
        'AVAX/USDT': {'exchange': 'okx'},
        'BCH/USDT': {'exchange': 'okx'},
        'BNB/USDT': {'exchange': 'okx'},
        'BONK/USDT': {'exchange': 'okx'},
        'BTC/USDT': {'exchange': 'okx'},
        'COMP/USDT': {'exchange': 'okx'},
        'CRV/USDT': {'exchange': 'okx'},
        'CVX/USDT': {'exchange': 'okx'},
        'DASH/USDT': {'exchange': 'okx'},
        'DOGE/USDT': {'exchange': 'okx'},
        'DOT/USDT': {'exchange': 'okx'},
        'ENA/USDT': {'exchange': 'okx'},
        'ETH/USDT': {'exchange': 'okx'},
        'FARTCOIN/USDT': {'exchange': 'okx'},
        'FET/USDT': {'exchange': 'okx'},
        'FIL/USDT': {'exchange': 'okx'},
        'HBAR/USDT': {'exchange': 'okx'},
        'HYPE/USDT': {'exchange': 'okx'},
        'INJ/USDT': {'exchange': 'okx'},
        'LINK/USDT': {'exchange': 'okx'},
        'LTC/USDT': {'exchange': 'okx'},
        'NEAR/USDT': {'exchange': 'okx'},
        'ONDO/USDT': {'exchange': 'okx'},
        'PENGU/USDT': {'exchange': 'okx'},
        'PEPE/USDT': {'exchange': 'okx'},
        'RENDER/USDT': {'exchange': 'okx'},
        'SOL/USDT': {'exchange': 'okx'},
        'SUI/USDT': {'exchange': 'okx'},
        'TAO/USDT': {'exchange': 'okx'},
        'UNI/USDT': {'exchange': 'okx'},
        'USELESS/USDT': {'exchange': 'okx'},
        'XLM/USDT': {'exchange': 'okx'},
        'XPL/USDT': {'exchange': 'okx'},
        'XRP/USDT': {'exchange': 'okx'},
        'ZEC/USDT': {'exchange': 'okx'},
        'ZEN/USDT': {'exchange': 'okx'},
    },
}

PULSE_SCALP_SYMBOLS = {
    'ADA/USDT', 'APT/USDT', 'ARB/USDT', 'BCH/USDT', 'BONK/USDT',
    'COMP/USDT', 'CVX/USDT', 'DASH/USDT', 'ENA/USDT',
    'FARTCOIN/USDT', 'FET/USDT', 'FIL/USDT', 'INJ/USDT', 'NEAR/USDT',
    'ONDO/USDT', 'PENGU/USDT', 'PEPE/USDT', 'RENDER/USDT', 'TAO/USDT',
    'UNI/USDT', 'USELESS/USDT', 'XLM/USDT', 'XPL/USDT', 'ZEN/USDT',
}

SCALP_SYMBOLS = {
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'LINK/USDT', 'ZEC/USDT',
}

STATE_LOCK = threading.RLock()
MOMENTUM_STATE = {}
LAST_SIGNALS = {}
LAST_SIGNAL_EVENTS = {}
SCALP_ENABLED = True
WATCHDOG_EXCLUDED_SYMBOLS = {'CVX/USDT'}
REDIS_CLIENT = None


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
            'st_context_30m': None, 'st_context_30m_ts': None, 'st_context_30m_raw': None,
            'st_context_5m': None, 'st_context_5m_ts': None, 'st_context_5m_raw': None,
            'st_context_15m': None, 'st_context_15m_ts': None, 'st_context_15m_raw': None,
            'st_context_1m': None, 'st_context_1m_ts': None, 'st_context_1m_raw': None,
            'st_context_2h': None, 'st_context_2h_ts': None, 'st_context_2h_raw': None,
            'bias_4h': None, 'bias_4h_ts': None,
            'bias_1d': None, 'bias_1d_ts': None,
            'bias_2h': None, 'bias_2h_ts': None,
            'bias_1h': None, 'bias_1h_ts': None,
            'bias_30m': None, 'bias_30m_ts': None,
            'bias_15m': None, 'bias_15m_ts': None,
            'zalt_15m': None, 'zalt_15m_ts': None,
            'zalt_4h': None, 'zalt_4h_ts': None,
            'bias_1h_btc': None, 'bias_1h_btc_ts': None,
            'zalt_1h_btc': None, 'zalt_1h_btc_ts': None,
            'rci_15m_10': None, 'rci_15m_ts': None,
            'st_context_3m': None, 'st_context_3m_ts': None,
            'zalt_12h': None, 'zalt_12h_ts': None,
            'zalt_2h': None, 'zalt_2h_ts': None,
            'zalt_5m': None, 'zalt_5m_ts': None,
            'last_zalt_5m_flip_dir': None, 'last_zalt_5m_flip_ts': None,
            'zalt_30m': None, 'zalt_30m_ts': None,
            'last_zalt_30m_flip_dir': None, 'last_zalt_30m_flip_ts': None,
            'rci_5m_10': None, 'rci_5m_ts': None,
            'rci_10m_10': None, 'rci_10m_30': None, 'rci_10m_50': None,
            'rci_10m_dir': None, 'rci_10m_ts': None,
            'rci_2h_10': None, 'rci_2h_30': None, 'rci_2h_50': None,
            'rci_2h_dir': None, 'rci_2h_chop': None, 'rci_2h_ts': None,
            'rci_4h_10': None, 'rci_4h_30': None, 'rci_4h_50': None,
            'rci_4h_dir': None, 'rci_4h_chop': None, 'rci_4h_ts': None,
            'rci_30m_10': None, 'rci_30m_30': None, 'rci_30m_50': None,
            'rci_30m_dir': None, 'rci_30m_chop': None, 'rci_30m_ts': None,
            'rci_30m_10_prev': None,
            'rci_1h_10': None, 'rci_1h_30': None, 'rci_1h_50': None,
            'rci_1h_dir': None, 'rci_1h_chop': None, 'rci_1h_ts': None,
            'scalp_prep_dir': None, 'scalp_b_prep_dir': None, 'scalp_quality_dir': None,
            'scalp_a_bias1h_prep_dir': None,
            'continuation_prep_dir': None, 'continuation_prep_route': None, 'continuation_prep_ts': None,
            'continuation_quality_dir': None,
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


def parse_dir_value(val):
    normalized = str(val).strip().lower()
    if normalized in ('1', 'buy', 'long', 'bull', 'bullish'):
        return 'buy'
    if normalized in ('0', '-1', 'sell', 'short', 'bear', 'bearish'):
        return 'sell'
    return None


def parse_st_context_value(val):
    try:
        ctx_val = float(val)
        if ctx_val < -1.96:
            return 'buy', ctx_val
        if ctx_val > 1.96:
            return 'sell', ctx_val
        return None, ctx_val
    except (TypeError, ValueError):
        parsed = parse_dir_value(val)
        return parsed, val


def is_fresh(ts, max_age_seconds):
    try:
        return ts is not None and (time.time() - float(ts)) <= max_age_seconds
    except (TypeError, ValueError):
        return False


def should_send(symbol, key, cooldown=1800, event_id=None):
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
NOTIFICATIONS.register(
    'telegram_priority_scalp',
    TelegramChannel(
        lambda: os.environ.get('PRIORITY_SCALP_BOT_TOKEN', ''),
        lambda: os.environ.get('PRIORITY_SCALP_CHAT_ID', '-1003706862644'),
        label='Telegram scalpbot priority',
    ),
)
NOTIFICATIONS.register(
    'telegram_secondary_scalp',
    TelegramChannel(
        lambda: os.environ.get('SCALP_SECONDARY_BOT_TOKEN', ''),
        lambda: os.environ.get('SCALP_SECONDARY_CHAT_ID', ''),
        label='Telegram ScalpSecondaire',
    ),
)
NOTIFICATIONS.register('ntfy', NtfyChannel(lambda: CONFIG.get('NTFY_TOPIC', '')))


def send_notification(title: str, message: str, priority=5, tags=None, telegram=True, ntfy=True,
                      reply_markup=None, telegram_channel='telegram_scalp'):
    channels = []
    if telegram:
        channels.append(telegram_channel)
    # Les alertes montre sont reservees a SWING dans le bot principal.
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


def telegram_channel_for_symbol(symbol=None, priority=False):
    if symbol in PULSE_SCALP_SYMBOLS:
        return 'telegram_secondary_scalp'
    if symbol in CONFIG['SYMBOLS'] or symbol is None:
        return 'telegram_priority_scalp' if priority else 'telegram_scalp'
    logger.warning(f"symbole sans groupe Telegram scalp: {symbol}; fallback principal")
    return 'telegram_priority_scalp' if priority else 'telegram_scalp'


def send_telegram(msg, ntfy=False, priority=False, symbol=None):
    msg = sanitize_scalp_notification(msg)
    telegram_channel = telegram_channel_for_symbol(symbol, priority)
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        telegram=True,
        ntfy=ntfy,
        telegram_channel=telegram_channel,
    )
    if not result.get(telegram_channel) and telegram_channel != 'telegram_scalp':
        logger.warning(f"alerte scalp non envoyee sur {telegram_channel}, fallback Telegram scalpbot")
        fallback = send_notification(
            notification_title_from_message(msg),
            msg,
            priority=5,
            telegram=True,
            ntfy=False,
            telegram_channel='telegram_scalp',
        )
        return bool(fallback.get('telegram_scalp') or result.get('ntfy'))
    return bool(result.get(telegram_channel))


def scalp_control_keyboard():
    if SCALP_ENABLED:
        return {"inline_keyboard": [[
            {"text": "Mettre SCALP en pause", "callback_data": "scalp_off"},
        ]]}
    return {"inline_keyboard": [[
        {"text": "Activer SCALP", "callback_data": "scalp_on"},
    ]]}


def send_telegram_with_buttons(msg, ntfy=False, priority=False, symbol=None):
    msg = sanitize_scalp_notification(msg)
    keyboard = scalp_control_keyboard()
    telegram_channel = telegram_channel_for_symbol(symbol, priority)
    reply_markup = None if telegram_channel == 'telegram_secondary_scalp' else keyboard
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        telegram=True,
        ntfy=ntfy,
        reply_markup=reply_markup,
        telegram_channel=telegram_channel,
    )
    if not result.get(telegram_channel) and telegram_channel != 'telegram_scalp':
        logger.warning(f"alerte scalp non envoyee sur {telegram_channel}, fallback Telegram scalpbot")
        fallback = send_notification(
            notification_title_from_message(msg),
            msg,
            priority=5,
            telegram=True,
            ntfy=False,
            reply_markup=keyboard,
            telegram_channel='telegram_scalp',
        )
        return bool(fallback.get('telegram_scalp') or result.get('ntfy'))
    if not result.get(telegram_channel):
        logger.warning("alerte creee sans notification Telegram")
    return bool(result.get(telegram_channel))


def evaluate_scalp_continuation(symbol, price=0, event_id=None, rci15m_previous=None, trigger=False):
    """PREP sur confluence 4H/2H/15m; entree au franchissement RCI 15m."""
    if symbol not in SCALP_SYMBOLS or not CONFIG.get('ENABLE_SCALP_CONTINUATION', True) or not CONFIG.get('ENABLE_SCALP', True) or not SCALP_ENABLED:
        return False
    notifications = []
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        bias4h = m.get('bias_4h') if is_fresh(m.get('bias_4h_ts'), 12 * 3600) else None
        bias2h = m.get('bias_2h') if is_fresh(m.get('bias_2h_ts'), 6 * 3600) else None
        zalt4h = m.get('zalt_4h') if is_fresh(m.get('zalt_4h_ts'), 12 * 3600) else None
        ctx15m = m.get('st_context_15m') if is_fresh(m.get('st_context_15m_ts'), 60 * 60) else None
        try:
            rci2h = float(m.get('rci_2h_10')) if is_fresh(m.get('rci_2h_ts'), 6 * 3600) else None
        except (TypeError, ValueError):
            rci2h = None
        try:
            rci15m = float(m.get('rci_15m_10')) if is_fresh(m.get('rci_15m_ts'), 30 * 60) else None
        except (TypeError, ValueError):
            rci15m = None
        route = direction = None
        for exp in ('buy', 'sell'):
            rci2h_ok = rci2h is not None and (rci2h <= -75 if exp == 'buy' else rci2h >= 75)
            if ctx15m != exp or not rci2h_ok:
                continue
            if bias4h == exp:
                route, direction = 'A', exp
                break
            if CONFIG.get('ENABLE_SCALP_CONTINUATION_B', False) and zalt4h == exp and bias2h == exp and bias4h in ('buy', 'sell') and bias4h != zalt4h:
                route, direction = 'B', exp
                break
        previous_dir = m.get('continuation_prep_dir')
        previous_route = m.get('continuation_prep_route')
        if direction != previous_dir or route != previous_route:
            m['continuation_prep_dir'] = direction
            m['continuation_prep_route'] = route
            m['continuation_prep_ts'] = time.time() if direction else None
            persist_state()
            if direction:
                notifications.append(('prep', route, direction))
        quality_dir = direction if route == 'A' and zalt4h == direction else None
        if quality_dir != m.get('continuation_quality_dir'):
            m['continuation_quality_dir'] = quality_dir
            persist_state()
            if quality_dir and previous_dir == direction and previous_route == route:
                notifications.append(('quality', route, direction))
        if direction and trigger and previous_dir == direction and previous_route == route and rci15m is not None and rci15m_previous is not None:
            crossed = (rci15m_previous > -75 and rci15m <= -75) if direction == 'buy' else (rci15m_previous < 75 and rci15m >= 75)
            if crossed and should_send(symbol, f"continuation_{route}_{direction}", cooldown=1800, event_id=event_id):
                notifications.append(('entry', route, direction))
    for kind, route, direction in notifications:
        side = 'LONG' if direction == 'buy' else 'SHORT'
        quality = route == 'A' and zalt4h == direction
        title = f"{'PREP' if kind == 'prep' else 'ENTREE' if kind == 'entry' else 'QUALITE'} CONTINUATION {route}"
        send_telegram_with_buttons(
            f"<b>[{title}{' - QUALITE' if quality and kind != 'quality' else ''}]</b> {symbol}\n"
            f"Direction: {side}\nPrice: ${format_price(price)}\n"
            + (f"[OK] Bias 4H 21/55: {side}\n" if route == 'A' else f"[OK] ZALT 4H 55/1.2 + Bias 2H 17/40: {side}\n")
            + f"[OK] RCI court 2H extreme: {rci2h:.1f}\n[OK] ST Context 15m aligne\n"
            + (f"[QUALITE] ZALT 4H aligne\n" if quality else "")
            + (f"[TRIGGER] RCI court 15m franchit l'extreme: {rci15m:.1f}" if kind == 'entry' else "[ATTENTE] RCI court 15m franchit l'extreme favorable"),
            ntfy=False, symbol=symbol,
        )
    return bool(notifications)


def evaluate_scalp_btc_15m(symbol, price=0, event_id=None):
    """Six actifs Scalp: confirmations 1H, RCI 15m, nouvelle zone Context 3m."""
    if symbol not in SCALP_SYMBOLS:
        return False
    if not CONFIG.get('ENABLE_SCALP_BTC_15M', False) or not CONFIG.get('ENABLE_SCALP', True) or not SCALP_ENABLED:
        return False
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        direction = m.get('st_context_3m')
        if direction not in ('buy', 'sell'):
            return False
        try:
            rci5 = float(m.get('rci_15m_10'))
        except (TypeError, ValueError):
            return False
        aligned = (
            is_fresh(m.get('bias_1h_btc_ts'), 2 * 3600) and m.get('bias_1h_btc') == direction
            and is_fresh(m.get('zalt_1h_btc_ts'), 2 * 3600) and m.get('zalt_1h_btc') == direction
            and is_fresh(m.get('rci_15m_ts'), 30 * 60)
            and ((direction == 'buy' and rci5 <= -75) or (direction == 'sell' and rci5 >= 75))
            and is_fresh(m.get('st_context_3m_ts'), 10 * 60)
        )
        if not aligned or not should_send(symbol, f"scalp_btc_15m_{direction}", cooldown=1800, event_id=event_id):
            return False
    side = 'LONG' if direction == 'buy' else 'SHORT'
    return send_telegram_with_buttons(
        f"<b>[SCALP 3m - ENTREE]</b> {symbol}\n"
        f"Direction: {side}\nPrice: ${format_price(price)}\n"
        f"[OK] Bias 1H 21/55 et ZALT 1H 45/1.3: {direction.upper()}\n"
        f"[OK] RCI court 15m extreme favorable: {rci5:.1f}\n"
        "[TRIGGER] Nouvelle zone ST Context 3m alignee",
        ntfy=False, symbol=symbol,
    )


def evaluate_scalp_core(symbol, price=0, event_id=None, trigger_label="state_refresh"):
    """SCALP A/B: confirmations distinctes; entree CTX 5m + RCI 5m extreme."""
    if symbol not in SCALP_SYMBOLS or not CONFIG.get('ENABLE_SCALP', True):
        return False

    notifications = []
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            return False

        for exp in ('buy', 'sell'):
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            bias2h = m.get('bias_2h')
            bias2h_ok = is_fresh(m.get('bias_2h_ts'), 6 * 3600) and bias2h == exp
            bias1h = m.get('bias_1h_btc')
            bias1h_ok = is_fresh(m.get('bias_1h_btc_ts'), 2 * 3600) and bias1h == exp
            zalt2h = m.get('zalt_2h')
            zalt2h_ok = is_fresh(m.get('zalt_2h_ts'), 6 * 3600) and zalt2h == exp
            try:
                rci1h = float(m.get('rci_1h_10'))
            except (TypeError, ValueError):
                rci1h = None
            rci1h_ok = bool(
                is_fresh(m.get('rci_1h_ts'), 3 * 3600) and rci1h is not None
                and ((exp == 'buy' and rci1h <= -75) or (exp == 'sell' and rci1h >= 75))
            )
            ctx5m = m.get('st_context_5m')
            ctx5m_ok = is_fresh(m.get('st_context_5m_ts'), 20 * 60) and ctx5m == exp
            ctx15m = m.get('st_context_15m')
            ctx15m_ok = is_fresh(m.get('st_context_15m_ts'), 60 * 60) and ctx15m == exp
            base_ok = bool(bias2h_ok and rci1h_ok)
            armed_a = bool(bias1h_ok and rci1h_ok and zalt2h_ok)
            armed_b = bool(base_ok and not zalt2h_ok and ctx15m_ok)
            armed = armed_a or armed_b
            route = 'A' if armed_a else 'B'
            quality = bool(armed_a and ctx15m_ok)
            try:
                rci5m = float(m.get('rci_5m_10'))
            except (TypeError, ValueError):
                rci5m = None
            rci5m_ok = bool(
                is_fresh(m.get('rci_5m_ts'), 20 * 60) and rci5m is not None
                and ((exp == 'buy' and rci5m <= -75) or (exp == 'sell' and rci5m >= 75))
            )

            prep_dir = m.get('scalp_a_bias1h_prep_dir')
            prep_b_dir = m.get('scalp_b_prep_dir')
            quality_dir = m.get('scalp_quality_dir')
            send_prep = (armed_a and prep_dir != exp) or (armed_b and prep_b_dir != exp)
            send_quality = quality and quality_dir != exp and (
                (armed_a and prep_dir == exp) or (armed_b and prep_b_dir == exp)
            )
            if armed_a and send_prep:
                m['scalp_a_bias1h_prep_dir'] = exp
            elif not armed_a and prep_dir == exp:
                m['scalp_a_bias1h_prep_dir'] = None
            if armed_b and send_prep:
                m['scalp_b_prep_dir'] = exp
            elif not armed_b and prep_b_dir == exp:
                m['scalp_b_prep_dir'] = None
            if quality:
                m['scalp_quality_dir'] = exp
            elif quality_dir == exp:
                m['scalp_quality_dir'] = None

            entry_ok = armed and ctx5m_ok and rci5m_ok and trigger_label != 'sync_state' and (
                (armed_a and prep_dir == exp) or (armed_b and prep_b_dir == exp)
            )
            logger.info(
                f"[SCALP CHECK] {symbol} {direction} src={trigger_label} "
                f"bias1h={bias1h}/{bias1h_ok} bias2h={bias2h}/{bias2h_ok} zalt2h={zalt2h}/{zalt2h_ok} rci1h={rci1h}/{rci1h_ok} "
                f"ctx5m={ctx5m}/{ctx5m_ok} ctx15m={ctx15m}/{ctx15m_ok} "
                f"rci5m={rci5m}/{rci5m_ok} route={route} armed={armed} entry={entry_ok}"
            )
            if send_prep:
                notifications.append(('prep', route, direction, bias1h, bias2h, zalt2h, rci1h, ctx5m, ctx15m, quality))
            elif send_quality:
                notifications.append(('quality', route, direction, bias1h, bias2h, zalt2h, rci1h, ctx5m, ctx15m, True))
            if entry_ok and should_send(symbol, f"scalp_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
                notifications.append(('entry', route, direction, bias1h, bias2h, zalt2h, rci1h, ctx5m, ctx15m, quality))
        persist_state()

    for kind, route, direction, bias1h, bias2h, zalt2h, rci1h, ctx5m, ctx15m, quality in notifications:
        emoji = "🟢" if direction == "LONG" else "🔴"
        title = f'PREP SCALP {route}' if kind == 'prep' else f'QUALITE SCALP {route}' if kind == 'quality' else f'SCALP {route} - ENTREE'
        expected = 'buy' if direction == 'LONG' else 'sell'
        quality_line = f"\n[QUALITE] ST Context 15m: {ctx15m.upper()}" if route == 'A' and ctx15m == expected else ""
        trigger_line = "\n[TRIGGER] RCI court 5m extreme favorable" if kind == 'entry' else "\n[ATTENTE] ST Context 5m aligne + RCI court 5m extreme favorable"
        send_telegram_with_buttons(
            f"{emoji} <b>{title} {direction}</b> {symbol}\n"
            "--------------------\n"
            f"Price: ${format_price(price)}\n"
            + (f"[OK] Bias 1H 21/55: {bias1h.upper()}\n[OK] ZALT 2H 55/1.2: {zalt2h.upper()}\n" if route == 'A' else f"[OK] Bias 2H 17/40: {bias2h.upper()}\n[OK] ST Context 15m: {ctx15m.upper()} (remplace ZALT 2H)\n")
            +
            f"[OK] RCI court 1H extreme favorable: {rci1h:.1f}\n"
            f"{'[OK] ST Context 5m: ' + ctx5m.upper() if kind == 'entry' else '[ATTENTE] ST Context 5m'}"
            f"{quality_line}{trigger_line}",
            ntfy=False,
            symbol=symbol,
        )
    return bool(notifications)


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
    rci30m_extreme_cross = None
    new_btc_ctx3m_zone = False
    rci15m_previous = None

    tf_aliases = {
        '1': '1m', '1min': '1m', '1minute': '1m',
        '60': '1h', '60m': '1h', '1hour': '1h',
        '5': '5m', '5min': '5m', '5minute': '5m',
        '3': '3m', '3min': '3m', '3minute': '3m',
        '10': '10m', '10min': '10m', '10minute': '10m',
        '15': '15m', '15min': '15m', '15minute': '15m',
        '30': '30m', '30min': '30m', '30minute': '30m',
    }
    tf = tf_aliases.get(tf, tf)
    alert_type_aliases = {
        'zerolagtrendsignal': 'zalt',
        'zerolagtrendsignals': 'zalt',
        'zero_lag_trend_signal': 'zalt',
        'zero_lag_trend_signals': 'zalt',
        'zls': 'zalt',
        'stcontext': 'st_context',
        'stcontextlt': 'st_context_lt',
        'st_context_long_term': 'st_context_lt',
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

    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        logger.info(f"Webhook: {symbol} | tf={tf} | type={alert_type} | val={val}")

        if alert_type == 'st_context' and tf == '3m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            new_btc_ctx3m_zone = symbol in SCALP_SYMBOLS and ctx_parsed in ('buy', 'sell') and ctx_parsed != m.get('st_context_3m') and not data.get('sync', False)
            m['st_context_3m'] = ctx_parsed
            m['st_context_3m_ts'] = time.time()
            persist_state()

        elif alert_type == 'st_context' and tf == '1m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_1m'] = ctx_parsed
            m['st_context_1m_ts'] = time.time()
            m['st_context_1m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '5m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_5m'] = ctx_parsed
            m['st_context_5m_ts'] = time.time()
            m['st_context_5m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '15m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_15m'] = ctx_parsed
            m['st_context_15m_ts'] = time.time()
            m['st_context_15m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '30m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_30m'] = ctx_parsed
            m['st_context_30m_ts'] = time.time()
            m['st_context_30m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '2h':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_2h'] = ctx_parsed
            m['st_context_2h_ts'] = time.time()
            m['st_context_2h_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'rci' and tf == '30m':
            value = val if val in ('buy', 'sell') else 'chop'
            direction = value if value in ('buy', 'sell') else None
            is_chop = bool(data.get('chop', value == 'chop'))
            previous_rci10 = m.get('rci_30m_10')
            try:
                previous_rci10 = float(previous_rci10) if previous_rci10 is not None else None
                current_rci10 = float(data.get('rci10'))
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] RCI 30m invalide: {symbol} rci10={data.get('rci10')!r}")
                return
            if previous_rci10 is not None and not data.get('sync', False):
                if previous_rci10 > -75 and current_rci10 <= -75:
                    rci30m_extreme_cross = 'buy'
                elif previous_rci10 < 75 and current_rci10 >= 75:
                    rci30m_extreme_cross = 'sell'
            m['rci_30m_10_prev'] = previous_rci10
            m['rci_30m_10'] = current_rci10
            m['rci_30m_30'] = data.get('rci30')
            m['rci_30m_50'] = data.get('rci50')
            m['rci_30m_dir'] = direction
            m['rci_30m_chop'] = is_chop
            m['rci_30m_ts'] = time.time()
            persist_state()

        elif alert_type == 'rci' and tf == '10m':
            rci10 = data.get('rci10')
            try:
                rci10_value = float(rci10)
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] RCI 10m invalide: {symbol} rci10={rci10!r}")
                return
            m['rci_10m_10'] = rci10_value
            m['rci_10m_30'] = data.get('rci30')
            m['rci_10m_50'] = data.get('rci50')
            m['rci_10m_dir'] = 'buy' if rci10_value <= -75 else 'sell' if rci10_value >= 75 else None
            m['rci_10m_ts'] = time.time()
            persist_state()

        elif alert_type == 'rci' and tf == '2h':
            rci10 = data.get('rci10')
            try:
                rci10_value = float(rci10)
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] RCI 2H invalide: {symbol} rci10={rci10!r}")
                return
            m['rci_2h_10'] = rci10_value
            m['rci_2h_30'] = data.get('rci30')
            m['rci_2h_50'] = data.get('rci50')
            m['rci_2h_dir'] = 'buy' if rci10_value <= -75 else 'sell' if rci10_value >= 75 else None
            m['rci_2h_chop'] = bool(data.get('chop', False))
            m['rci_2h_ts'] = time.time()
            persist_state()

        elif alert_type == 'rci' and tf in ('1h', '5m', '15m'):
            rci10 = data.get('rci10')
            try:
                rci10_value = float(rci10)
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] RCI {tf} invalide: {symbol} rci10={rci10!r}")
                return
            if tf == '15m':
                try:
                    rci15m_previous = float(m.get('rci_15m_10'))
                except (TypeError, ValueError):
                    rci15m_previous = None
            m[f'rci_{tf}_10'] = rci10_value
            m[f'rci_{tf}_30'] = data.get('rci30')
            m[f'rci_{tf}_50'] = data.get('rci50')
            m[f'rci_{tf}_dir'] = 'buy' if rci10_value <= -75 else 'sell' if rci10_value >= 75 else None
            m[f'rci_{tf}_chop'] = bool(data.get('chop', False))
            m[f'rci_{tf}_ts'] = time.time()
            persist_state()

        elif alert_type == 'rci' and tf == '4h':
            rci10 = data.get('rci10')
            try:
                rci10_value = float(rci10)
            except (TypeError, ValueError):
                logger.warning(f"[WEBHOOK] RCI 4H invalide: {symbol} rci10={rci10!r}")
                return
            m['rci_4h_10'] = rci10_value
            m['rci_4h_30'] = data.get('rci30')
            m['rci_4h_50'] = data.get('rci50')
            m['rci_4h_dir'] = 'buy' if rci10_value <= -75 else 'sell' if rci10_value >= 75 else None
            m['rci_4h_chop'] = bool(data.get('chop', False))
            m['rci_4h_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '4h':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_4h'] = bias_val
            m['bias_4h_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '1d':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_1d'] = bias_val
            m['bias_1d_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '1h':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_1h'] = bias_val
            m['bias_1h_ts'] = time.time()
            persist_state()
            if symbol in SCALP_SYMBOLS and str(data.get('event_id', '')).startswith('okx_scalp_bias_1h_'):
                m['bias_1h_btc'] = bias_val
                m['bias_1h_btc_ts'] = time.time()
                persist_state()

        elif alert_type == 'bias' and tf == '30m':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_30m'] = bias_val
            m['bias_30m_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '15m':
            m['bias_15m'] = val if val in ('buy', 'sell') else None
            m['bias_15m_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '2h':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_2h'] = bias_val
            m['bias_2h_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '15m':
            m['zalt_15m'] = val if val in ('buy', 'sell') else None
            m['zalt_15m_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '1h':
            m['zalt_1h_btc'] = val if val in ('buy', 'sell') else None
            m['zalt_1h_btc_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '4h':
            m['zalt_4h'] = val if val in ('buy', 'sell') else None
            m['zalt_4h_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '12h':
            zalt_val = val if val in ('buy', 'sell') else None
            m['zalt_12h'] = zalt_val
            m['zalt_12h_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '2h':
            zalt_val = val if val in ('buy', 'sell') else None
            m['zalt_2h'] = zalt_val
            m['zalt_2h_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '5m':
            zalt_val = val if val in ('buy', 'sell') else None
            m['zalt_5m'] = zalt_val
            m['zalt_5m_ts'] = time.time()
            if data.get('signal') == 'trend_flip' and zalt_val in ('buy', 'sell'):
                m['last_zalt_5m_flip_dir'] = zalt_val
                m['last_zalt_5m_flip_ts'] = time.time()
            persist_state()

        elif alert_type == 'zalt' and tf == '30m':
            zalt_val = val if val in ('buy', 'sell') else None
            m['zalt_30m'] = zalt_val
            m['zalt_30m_ts'] = time.time()
            if data.get('signal') == 'trend_flip' and zalt_val in ('buy', 'sell'):
                m['last_zalt_30m_flip_dir'] = zalt_val
                m['last_zalt_30m_flip_ts'] = time.time()
            persist_state()

        else:
            return

    if symbol in SCALP_SYMBOLS and (
        (alert_type == 'bias' and tf in ('2h', '4h'))
        or (alert_type == 'zalt' and tf == '4h')
        or (alert_type == 'rci' and tf in ('2h', '15m'))
        or (alert_type == 'st_context' and tf == '15m')
    ):
        evaluate_scalp_continuation(
            symbol, price=price, event_id=f"continuation_{symbol}_{event_id}",
            rci15m_previous=rci15m_previous,
            trigger=alert_type == 'rci' and tf == '15m' and not data.get('sync', False),
        )

    if new_btc_ctx3m_zone:
        evaluate_scalp_btc_15m(symbol, price=price, event_id=f"scalp_3m_{symbol}_{event_id}")

    if symbol in SCALP_SYMBOLS and (
        (alert_type == 'st_context' and tf in ('5m', '15m'))
        or (alert_type == 'bias' and tf in ('1h', '2h'))
        or (alert_type == 'rci' and tf in ('1h', '5m'))
        or (alert_type == 'zalt' and tf == '2h')
    ):
        evaluate_scalp_core(
            symbol,
            price=price,
            event_id=f"scalp_{symbol}_{tf}_{alert_type}_{event_id}",
            trigger_label=(
                'sync_state' if data.get('sync', False)
                else 'zalt_5m_flip' if alert_type == 'zalt' and tf == '5m' and data.get('signal') == 'trend_flip'
                else f"{alert_type}_{tf}"
            ),
        )

@app.route('/telegram_callback', methods=['POST'])
def telegram_callback():
    global SCALP_ENABLED
    tg_secret = os.environ.get('SCALP_TELEGRAM_SECRET', '')
    if tg_secret and request.headers.get('X-Telegram-Bot-Api-Secret-Token', '') != tg_secret:
        return jsonify({'ok': False}), 403
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': True}), 200
    try:
        message = (
            data.get('message')
            or data.get('channel_post')
            or data.get('edited_message')
            or data.get('edited_channel_post')
            or {}
        )
        message_text = str(message.get('text', '')).strip().lower()
        command = message_text.split()[0].split('@')[0] if message_text else ''
        message_chat_id = message.get('chat', {}).get('id')
        tok = os.environ.get('SCALP_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '')
        if command in ('/scalp', '/scalp_status') and tok and message_chat_id:
            status = 'ACTIVE' if SCALP_ENABLED else 'EN PAUSE'
            response = requests.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                json={
                    'chat_id': message_chat_id,
                    'text': f"<b>Controle SCALP</b>\nEtat actuel : <b>{status}</b>",
                    'parse_mode': 'HTML',
                    'reply_markup': scalp_control_keyboard(),
                },
                timeout=5,
            )
            if not 200 <= response.status_code < 300:
                logger.warning(f"[SCALP CONTROL] Telegram HTTP {response.status_code}: {response.text[:200]}")
            else:
                logger.info(f"[SCALP CONTROL] Panneau envoye au chat {message_chat_id}")
            return jsonify({'ok': True}), 200

        cb = data.get('callback_query', {})
        cb_id = cb.get('id')
        cb_data = cb.get('data', '')
        chat_id = cb.get('message', {}).get('chat', {}).get('id')
        msg_id = cb.get('message', {}).get('message_id')
        user = cb.get('from', {}).get('first_name', 'User')
        if tok and cb_id:
            requests.post(
                f"https://api.telegram.org/bot{tok}/answerCallbackQuery",
                json={
                    "callback_query_id": cb_id,
                    "text": "SCALP mis a jour",
                },
                timeout=5,
            )
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
                        "reply_markup": scalp_control_keyboard(),
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
                        "reply_markup": scalp_control_keyboard(),
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
        bias2h = signal_debug_payload(m, 'bias_2h', 6 * 3600)
        zalt2h = signal_debug_payload(m, 'zalt_2h', 6 * 3600)
        ctx15m = signal_debug_payload(m, 'st_context_15m', 60 * 60)
        ctx5m = signal_debug_payload(m, 'st_context_5m', 20 * 60)
        rci1h = {'fresh': is_fresh(m.get('rci_1h_ts'), 3 * 3600)}
        checks = {}
        for exp in ('buy', 'sell'):
            rci = m.get('rci_1h_10')
            rci_ok = bool(rci1h['fresh'] and rci is not None and (
                (exp == 'buy' and float(rci) <= -75) or (exp == 'sell' and float(rci) >= 75)
            ))
            base_ok = bias2h['fresh'] and bias2h['value'] == exp and rci_ok
            a_ok = base_ok and zalt2h['fresh'] and zalt2h['value'] == exp
            b_ok = base_ok and not a_ok and ctx15m['fresh'] and ctx15m['value'] == exp
            checks[exp] = {
                'eligible': symbol in SCALP_SYMBOLS,
                'prep_a_ok': bool(a_ok),
                'prep_b_ok': bool(b_ok),
                'entry_trigger': 'ST Context 5m aligne + RCI court 5m extreme favorable',
                'ctx5m_ok': ctx5m['fresh'] and ctx5m['value'] == exp,
            }
        return jsonify({
            'status': 'ok',
            'symbol': symbol,
            'enabled': SCALP_ENABLED,
            'now_shanghai': datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'scalp': {
                'long': checks['buy'],
                'short': checks['sell'],
            },
            'signals': {
                'st_context_15m': ctx15m,
                'st_context_5m': ctx5m,
                'bias_2h': bias2h,
                'zalt_2h': zalt2h,
                'rci_1h_10': rci,
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
    requirements = [
        {'label': 'RCI 5m (SCALP)', 'field': 'rci_5m_ts', 'max_age': 20 * 60, 'warmup': 45 * 60, 'symbols': sorted(SCALP_SYMBOLS)},
        {'label': 'ST Context 5m (SCALP)', 'field': 'st_context_5m_ts', 'max_age': 60 * 60, 'warmup': 60 * 60, 'symbols': sorted(SCALP_SYMBOLS)},
        {'label': 'ST Context 15m (SCALP B / qualite A)', 'field': 'st_context_15m_ts', 'max_age': 60 * 60, 'warmup': 75 * 60, 'symbols': sorted(SCALP_SYMBOLS)},
    ]
    return requirements


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
            symbols = [
                symbol for symbol in SCALP_SYMBOLS
                if symbol not in WATCHDOG_EXCLUDED_SYMBOLS
            ]
            state_copy = {s: dict(MOMENTUM_STATE.get(s, {})) for s in symbols}
        for req in scalp_required_tv_signals():
            if uptime < req['warmup']:
                continue
            missing = []
            stale = []
            req_symbols = req.get('symbols', symbols)
            for symbol in req_symbols:
                ts = state_copy.get(symbol, {}).get(req['field'])
                max_age = req['max_age']
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
        if issues and should_send('GLOBAL', 'scalp_tv_signal_watchdog', cooldown=3600):
            send_telegram(
                "<b>[ALERTE] Signaux TradingView scalp manquants</b>\n"
                "--------------------\n"
                + "\n".join(issues)
                + "\n\nVerifier les alertes TradingView / relay bot principal.",
                ntfy=False,
            )
            logger.warning(f"[TV SIGNAL WATCHDOG] Scalp issues: {issues}")

        # Signaux obligatoires calcules en interne puis relayes par le bot principal.
        if uptime >= 45 * 60:
            internal_requirements = [
                ('Bias 2H (SCALP)', 'bias_2h_ts', 6 * 3600, 'scalp_bias2h_watchdog', sorted(SCALP_SYMBOLS)),
                ('RCI 1H (SCALP)', 'rci_1h_ts', 3 * 3600, 'scalp_rci1h_watchdog', sorted(SCALP_SYMBOLS)),
                ('ZALT 2H (SCALP)', 'zalt_2h_ts', 6 * 3600, 'scalp_zalt2h_watchdog', sorted(SCALP_SYMBOLS)),
                ('RCI 5m (SCALP)', 'rci_5m_ts', 20 * 60, 'scalp_rci5m_watchdog', sorted(SCALP_SYMBOLS)),
            ]
            for label, field, max_age, alert_key, required_symbols in internal_requirements:
                missing, stale = [], []
                for symbol in required_symbols:
                    ts = state_copy.get(symbol, {}).get(field)
                    if ts is None:
                        missing.append(symbol.replace('/USDT', ''))
                    elif now - float(ts) > max_age:
                        stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 60))
                if (missing or stale) and should_send('GLOBAL', alert_key, cooldown=3600):
                    details = []
                    if missing:
                        details.append("jamais recu: " + ", ".join(missing))
                    if stale:
                        details.append("perime: " + ", ".join(f"{sym} {age:.0f}m" for sym, age in stale))
                    send_telegram(
                        f"<b>[ALERTE] Relais {label} (mainbot) interrompu — SCALP bloque/degrade</b>\n"
                        "--------------------\n"
                        + " | ".join(details)
                        + "\n\nVerifier le cycle indicateurs / relay du bot principal.",
                        ntfy=False,
                    )
                    logger.warning(f"[{label.upper()} WATCHDOG] missing={missing} stale={stale}")


def startup():
    global SCALP_ENABLED
    init_redis()
    load_state()
    # Conserver l'etat ON/OFF restaure depuis Redis.
    with STATE_LOCK:
        persist_state()
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

    send_telegram_with_buttons(
        "<b>Scalping Bot demarre</b>\n"
        "--------------------\n"
        f"Assets: {len(CONFIG['SYMBOLS'])}\n"
        "SCALP A (6 actifs): PREP ZALT 2H 55/1.2 + Bias 1H 21/55 + RCI 1H extreme\n"
        "SCALP B: PREP Bias 2H + CTX 15m + RCI 1H extreme (sans ZALT 2H aligne)\n"
        "A/B: entree CTX 5m + RCI 5m extreme 21/1.0\n"
        f"{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}",
        ntfy=False,
    )

    if os.environ.get('SCALP_SECONDARY_BOT_TOKEN') and os.environ.get('SCALP_SECONDARY_CHAT_ID'):
        send_notification(
            'ScalpSecondaire connecte',
            "<b>ScalpSecondaire connecte</b>\n"
            "--------------------\n"
            f"Watchlist PULSE: {len(PULSE_SCALP_SYMBOLS)} assets\n"
            "SCALP A/B actives sur la liste automatique.",
            telegram=True,
            ntfy=False,
            telegram_channel='telegram_secondary_scalp',
        )


if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    threading.Thread(target=startup, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
