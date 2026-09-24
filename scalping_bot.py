#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot — strategie unique : Bias 4H + ST Context 10m.
# ST Context 30m est non bloquant : oppose = avertissement, aligne = JACKPOT.
# RCI 30m est affiche comme confirmation manuelle uniquement.

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
    'SYMBOLS': {
        'AAVE/USDT': {'exchange': 'okx'},
        'ADA/USDT': {'exchange': 'okx'},
        'APT/USDT': {'exchange': 'okx'},
        'ARB/USDT': {'exchange': 'okx'},
        'AVAX/USDT': {'exchange': 'okx'},
        'BNB/USDT': {'exchange': 'okx'},
        'BONK/USDT': {'exchange': 'okx'},
        'BTC/USDT': {'exchange': 'okx'},
        'COMP/USDT': {'exchange': 'okx'},
        'CRV/USDT': {'exchange': 'okx'},
        'CVX/USDT': {'exchange': 'okx'},
        'DASH/USDT': {'exchange': 'okx'},
        'DOGE/USDT': {'exchange': 'okx'},
        'ENA/USDT': {'exchange': 'okx'},
        'ETH/USDT': {'exchange': 'okx'},
        'ETHFI/USDT': {'exchange': 'okx'},
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
        'XPL/USDT': {'exchange': 'okx'},
        'XRP/USDT': {'exchange': 'okx'},
        'ZEC/USDT': {'exchange': 'okx'},
        'ZEN/USDT': {'exchange': 'okx'},
    },
}

SCALP_PRIMARY_SYMBOLS = {
    'AAVE/USDT', 'ADA/USDT', 'AVAX/USDT', 'BTC/USDT', 'CRV/USDT',
    'DOGE/USDT', 'ENA/USDT', 'ETH/USDT', 'HYPE/USDT', 'LINK/USDT',
    'LTC/USDT', 'NEAR/USDT', 'SOL/USDT', 'SUI/USDT', 'TAO/USDT',
    'UNI/USDT', 'XRP/USDT', 'ZEC/USDT',
}

PULSE_SCALP_SYMBOLS = {
    'APT/USDT', 'ARB/USDT', 'BNB/USDT', 'BONK/USDT', 'COMP/USDT',
    'CVX/USDT', 'DASH/USDT', 'ETHFI/USDT', 'FARTCOIN/USDT', 'FET/USDT',
    'FIL/USDT', 'HBAR/USDT', 'INJ/USDT', 'ONDO/USDT', 'PENGU/USDT',
    'PEPE/USDT', 'RENDER/USDT', 'USELESS/USDT', 'XPL/USDT', 'ZEN/USDT',
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
            'st_context_10m': None, 'st_context_10m_ts': None, 'st_context_10m_raw': None,
            'st_context_1m': None, 'st_context_1m_ts': None, 'st_context_1m_raw': None,
            'bias_4h': None, 'bias_4h_ts': None,
            'bias_1h': None, 'bias_1h_ts': None,
            'bias_30m': None, 'bias_30m_ts': None,
            'rci_10m_10': None, 'rci_10m_30': None, 'rci_10m_50': None,
            'rci_10m_dir': None, 'rci_10m_ts': None,
            'rci_30m_10': None, 'rci_30m_30': None, 'rci_30m_50': None,
            'rci_30m_dir': None, 'rci_30m_chop': None, 'rci_30m_ts': None,
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


def telegram_channel_for_symbol(symbol=None, priority=False):
    if symbol in PULSE_SCALP_SYMBOLS:
        return 'telegram_secondary_scalp'
    if symbol in SCALP_PRIMARY_SYMBOLS or symbol is None:
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


def send_telegram_with_buttons(msg, ntfy=False, priority=False, symbol=None):
    msg = sanitize_scalp_notification(msg)
    keyboard = {"inline_keyboard": [[
        {"text": "Scalp ON", "callback_data": "scalp_on"},
        {"text": "Scalp OFF", "callback_data": "scalp_off"},
    ]]}
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


def evaluate_scalp(symbol, price=0, event_id=None, trigger_label="state_refresh"):
    """Scalp unique : Bias 4H + ST Context 10m.
    CTX 30m est non bloquant; aligne, il transforme l'alerte en JACKPOT.
    RCI 30m reste une confirmation manuelle."""
    notify = None
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] ignore {symbol}")
            return False

        for exp in ('buy', 'sell'):
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            bias4h = m.get('bias_4h')
            bias4h_ok = is_fresh(m.get('bias_4h_ts'), 10 * 3600) and bias4h == exp
            ctx10 = m.get('st_context_10m')
            ctx10_ok = is_fresh(m.get('st_context_10m_ts'), 45 * 60) and ctx10 == exp

            ctx30 = m.get('st_context_30m')
            ctx30_fresh = is_fresh(m.get('st_context_30m_ts'), 90 * 60)
            rci30 = m.get('rci_30m_dir')
            rci30_fresh = is_fresh(m.get('rci_30m_ts'), 90 * 60)

            ctx30_aligned = bool(ctx30_fresh and ctx30 == exp)
            ctx30_opposite = bool(ctx30_fresh and ctx30 == ('sell' if exp == 'buy' else 'buy'))
            entry_ok = bias4h_ok and ctx10_ok
            logger.info(
                f"[SCALP CHECK] {symbol} {direction} src={trigger_label} "
                f"entry={entry_ok} bias4h={bias4h} ok={bias4h_ok} "
                f"ctx10={ctx10} ok={ctx10_ok} ctx30={ctx30} fresh={ctx30_fresh} "
                f"aligned={ctx30_aligned} opposite={ctx30_opposite} "
                f"rci30={rci30} fresh={rci30_fresh}"
            )
            signal_kind = 'jackpot' if ctx30_aligned else 'entry'
            if entry_ok and should_send(symbol, f"scalp_{signal_kind}_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
                notify = (
                    direction, symbol, price, bias4h, ctx10,
                    ctx30, ctx30_fresh, ctx30_aligned, ctx30_opposite, rci30, rci30_fresh,
                    m.get('rci_30m_10'), m.get('rci_30m_30'), m.get('rci_30m_50'),
                )
                break

    if not notify:
        return False

    direction, symbol, price, bias4h, ctx10, ctx30, ctx30_fresh, jackpot, ctx30_opposite, rci30, rci30_fresh, rci30_10, rci30_30, rci30_50 = notify
    emoji = "🟢" if direction == "LONG" else "🔴"
    ctx30_txt = ctx30.upper() if ctx30_fresh and ctx30 else "NEUTRE/NON FRAIS"
    rci30_txt = rci30.upper() if rci30_fresh and rci30 else "NEUTRE/NON FRAIS"
    if jackpot:
        ctx30_line = f"[JACKPOT] ST Context 30m aligne: {ctx30_txt}"
    elif ctx30_opposite:
        ctx30_line = f"[ALERTE NON BLOQUANTE] ST Context 30m oppose: {ctx30_txt}"
    else:
        ctx30_line = f"[INFO NON BLOQUANTE] ST Context 30m: {ctx30_txt}"
    send_telegram_with_buttons(
        f"{emoji} <b>{'SCALP JACKPOT' if jackpot else 'SCALP'} {direction}</b> {symbol}\n"
        f"--------------------\n"
        f"Price: ${format_price(price)}\n"
        f"[OK] Bias 4H: {bias4h.upper()}\n"
        f"[OK] ST Context 10m: {ctx10.upper()}\n"
        f"{ctx30_line}\n"
        f"[MANUEL] Regarder le RCI 30m: {rci30_txt} "
        f"(10={rci30_10}, 30={rci30_30}, 50={rci30_50}) pour confirmation\n"
        f"Trigger: Bias 4H + ST Context 10m",
        symbol=symbol,
    )
    return True


def evaluate_scalp_1h(symbol, price=0, event_id=None, trigger_label="state_refresh"):
    """Scalp 1H, watchlist SCALP : Bias 1H + confirmation Bias 30m/CTX 10m,
    RCI 10m extreme et CTX 1m. CTX 10m oppose bloque toujours."""
    if symbol not in SCALP_PRIMARY_SYMBOLS:
        return False

    notify = None
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            return False

        for exp in ('buy', 'sell'):
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            bias1h = m.get('bias_1h')
            bias1h_ok = is_fresh(m.get('bias_1h_ts'), 3 * 3600) and bias1h == exp

            bias30 = m.get('bias_30m')
            bias30_fresh = is_fresh(m.get('bias_30m_ts'), 90 * 60)
            bias30_aligned = bool(bias30_fresh and bias30 == exp)

            ctx10 = m.get('st_context_10m')
            ctx10_fresh = is_fresh(m.get('st_context_10m_ts'), 45 * 60)
            opposite = 'sell' if exp == 'buy' else 'buy'
            ctx10_aligned = bool(ctx10_fresh and ctx10 == exp)
            ctx10_opposite = bool(ctx10_fresh and ctx10 == opposite)
            confirmation_ok = bool(not ctx10_opposite and (bias30_aligned or ctx10_aligned))

            ctx1 = m.get('st_context_1m')
            ctx1_ok = is_fresh(m.get('st_context_1m_ts'), 12 * 60) and ctx1 == exp

            rci10 = m.get('rci_10m_10')
            rci10_fresh = is_fresh(m.get('rci_10m_ts'), 45 * 60)
            try:
                rci10_value = float(rci10)
            except (TypeError, ValueError):
                rci10_value = None
            rci10_ok = bool(
                rci10_fresh
                and rci10_value is not None
                and ((exp == 'buy' and rci10_value <= -80) or (exp == 'sell' and rci10_value >= 80))
            )

            entry_ok = bias1h_ok and confirmation_ok and rci10_ok and ctx1_ok
            logger.info(
                f"[SCALP1H CHECK] {symbol} {direction} src={trigger_label} entry={entry_ok} "
                f"bias1h={bias1h} ok={bias1h_ok} bias30={bias30} aligned={bias30_aligned} "
                f"ctx10={ctx10} aligned={ctx10_aligned} opposite={ctx10_opposite} "
                f"confirmation={confirmation_ok} rci10m={rci10_value} ok={rci10_ok} ctx1={ctx1} ok={ctx1_ok}"
            )
            if entry_ok and should_send(
                symbol,
                f"scalp_1h_entry_{exp}",
                event_id=event_id,
                cooldown=CONFIG['MIN_COOLDOWN'],
            ):
                notify = (
                    direction, symbol, price, bias1h, bias30, bias30_aligned,
                    ctx10, ctx10_aligned, ctx1, rci10_value,
                )
                break

    if not notify:
        return False

    direction, symbol, price, bias1h, bias30, bias30_aligned, ctx10, ctx10_aligned, ctx1, rci10_value = notify
    emoji = "🟢" if direction == "LONG" else "🔴"
    zone_label = "SURVENTE <= -80" if direction == "LONG" else "SURACHAT >= +80"
    send_telegram_with_buttons(
        f"{emoji} <b>SCALP 1H {direction}</b> {symbol}\n"
        f"--------------------\n"
        f"Price: ${format_price(price)}\n"
        f"[OK] Bias 1H: {bias1h.upper()}\n"
        f"{'[OK] Bias 30m aligne: ' + bias30.upper() if bias30_aligned else '[CONFIRMATION] Bias 30m non aligne; ST Context 10m aligne: ' + ctx10.upper()}\n"
        f"[OK] RCI court 10m: {rci10_value:.1f} ({zone_label})\n"
        f"[OK] ST Context 1m: {ctx1.upper()}\n"
        f"Trigger: Bias 1H + (Bias 30m ou CTX 10m aligne) + RCI 10m extreme +/-80 + CTX 1m",
        ntfy=False,
        priority=True,
        symbol=symbol,
    )
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
        '60': '1h', '60m': '1h', '1hour': '1h',
        '5': '5m', '5min': '5m', '5minute': '5m',
        '10': '10m', '10min': '10m', '10minute': '10m',
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

        if alert_type == 'st_context' and tf == '10m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_10m'] = ctx_parsed
            m['st_context_10m_ts'] = time.time()
            m['st_context_10m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '1m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_1m'] = ctx_parsed
            m['st_context_1m_ts'] = time.time()
            m['st_context_1m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'st_context' and tf == '30m':
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m['st_context_30m'] = ctx_parsed
            m['st_context_30m_ts'] = time.time()
            m['st_context_30m_raw'] = ctx_raw
            persist_state()

        elif alert_type == 'rci' and tf == '30m':
            value = val if val in ('buy', 'sell') else 'chop'
            direction = value if value in ('buy', 'sell') else None
            is_chop = bool(data.get('chop', value == 'chop'))
            m['rci_30m_10'] = data.get('rci10')
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
            m['rci_10m_dir'] = 'buy' if rci10_value <= -80 else 'sell' if rci10_value >= 80 else None
            m['rci_10m_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '4h':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_4h'] = bias_val
            m['bias_4h_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '1h':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_1h'] = bias_val
            m['bias_1h_ts'] = time.time()
            persist_state()

        elif alert_type == 'bias' and tf == '30m':
            bias_val = val if val in ('buy', 'sell') else None
            m['bias_30m'] = bias_val
            m['bias_30m_ts'] = time.time()
            persist_state()

        else:
            return

    if (
        (alert_type == 'st_context' and tf in ('10m', '30m'))
        or (alert_type == 'bias' and tf == '4h')
        or (alert_type == 'rci' and tf == '30m')
    ):
        evaluate_scalp(
            symbol,
            price=price,
            event_id=f"scalp_{symbol}_{tf}_{alert_type}_{event_id}",
            trigger_label=f"{alert_type}_{tf}",
        )

    if symbol in SCALP_PRIMARY_SYMBOLS and (
        (alert_type == 'st_context' and tf == '1m')
        or (alert_type == 'st_context' and tf == '10m')
        or (alert_type == 'bias' and tf == '1h')
        or (alert_type == 'bias' and tf == '30m')
        or (alert_type == 'rci' and tf == '10m')
    ):
        evaluate_scalp_1h(
            symbol,
            price=price,
            event_id=f"scalp1h_{symbol}_{tf}_{alert_type}_{event_id}",
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
        ctx1m = signal_debug_payload(m, 'st_context_1m', 12 * 60)
        ctx10m = signal_debug_payload(m, 'st_context_10m', 45 * 60)
        ctx30m = signal_debug_payload(m, 'st_context_30m', 90 * 60)
        zalt10m = signal_debug_payload(m, 'zalt_10m', 45 * 60)
        zalt30m = signal_debug_payload(m, 'zalt_30m', 90 * 60)
        bias30m_fresh = is_fresh(m.get('bias_30m_ts'), 90 * 60)
        bias2h_fresh = is_fresh(m.get('bias_2h_ts'), 5 * 3600)
        rci30_fresh = is_fresh(m.get('rci_30m_ts'), 90 * 60)
        rci2h_fresh = is_fresh(m.get('rci_2h_ts'), 6 * 3600)
        checks = {}
        checks_info_30m = {}
        checks_secondary = {}
        for exp in ('buy', 'sell'):
            ctx1_ok = ctx1m['fresh'] and ctx1m['value'] == exp
            bias30_ok = bool(bias30m_fresh and m.get('bias_30m') == exp)
            checks[exp] = {
                'bias30m_ok': bias30_ok,
                'ctx1m_ok': ctx1_ok,
                'rci10m_manual': True,
                'ctx30m_manual_non_blocking': ctx30m,
                'rci30m_confirmation': {
                    '10': m.get('rci_30m_10'), '30': m.get('rci_30m_30'), '50': m.get('rci_30m_50'),
                    'dir': m.get('rci_30m_dir'), 'chop': m.get('rci_30m_chop'), 'ts': m.get('rci_30m_ts'),
                    'fresh': rci30_fresh,
                },
                'entry_ok': bias30_ok and ctx1_ok,
            }

            ctx30_ok = ctx30m['fresh'] and ctx30m['value'] == exp
            rci2h_ok = bool(rci2h_fresh and m.get('rci_2h_dir') == exp)
            checks_info_30m[exp] = {
                'ctx30m_ok': ctx30_ok,
                'rci2h_ok': rci2h_ok,
                'ctx1m_ok': ctx1_ok,
                'info_ok': ctx30_ok and rci2h_ok and ctx1_ok,
            }

            opp = 'sell' if exp == 'buy' else 'buy'
            bias2h_ok = bool(bias2h_fresh and m.get('bias_2h') == exp)
            ctx10_ok = bool(ctx10m['fresh'] and ctx10m['value'] == exp)
            ctx10_chop_veto = bool(ctx10m['fresh'] and ctx10m['value'] == opp)
            rci30_short = m.get('rci_30m_10')
            if exp == 'buy':
                rci30_ok = bool(rci30_fresh and rci30_short is not None and float(rci30_short) <= -75)
            else:
                rci30_ok = bool(rci30_fresh and rci30_short is not None and float(rci30_short) >= 75)
            checks_secondary[exp] = {
                'bias2h_ok': bias2h_ok,
                'ctx10m_ok': ctx10_ok,
                'anti_chop_veto': ctx10_chop_veto,
                'rci30m_ok': rci30_ok,
                'entry_ok': bias2h_ok and ctx10_ok and rci30_ok and not ctx10_chop_veto,
            }
        return jsonify({
            'status': 'ok',
            'symbol': symbol,
            'enabled': SCALP_ENABLED,
            'now_shanghai': datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'scalp_simple': {
                'long': checks['buy'],
                'short': checks['sell'],
            },
            'scalp_secondary': {
                'long': checks_secondary['buy'],
                'short': checks_secondary['sell'],
            },
            'scalp_info_30m': {
                'long': checks_info_30m['buy'],
                'short': checks_info_30m['sell'],
            },
            'signals': {
                'zalt_30m': zalt30m,
                'last_zalt_30m_signal_ts': m.get('last_zalt_30m_signal_ts'),
                'zalt_10m': zalt10m,
                'last_zalt_10m_signal_ts': m.get('last_zalt_10m_signal_ts'),
                'st_context_1m': ctx1m,
                'st_context_10m': ctx10m,
                'st_context_30m': ctx30m,
                'bias_30m': {'value': m.get('bias_30m'), 'ts': m.get('bias_30m_ts'), 'fresh': bias30m_fresh},
                'bias_2h': {'value': m.get('bias_2h'), 'ts': m.get('bias_2h_ts')},
                'rci_30m': {
                    '10': m.get('rci_30m_10'), '30': m.get('rci_30m_30'), '50': m.get('rci_30m_50'),
                    'dir': m.get('rci_30m_dir'), 'chop': m.get('rci_30m_chop'), 'ts': m.get('rci_30m_ts'),
                },
                'rci_2h': {
                    '10': m.get('rci_2h_10'), '30': m.get('rci_2h_30'), '50': m.get('rci_2h_50'),
                    'dir': m.get('rci_2h_dir'), 'chop': m.get('rci_2h_chop'), 'ts': m.get('rci_2h_ts'),
                    'fresh': rci2h_fresh,
                },
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
        {'label': 'ST Context 10m', 'field': 'st_context_10m_ts', 'max_age': 45 * 60, 'warmup': 90 * 60},
        {'label': 'ST Context 30m', 'field': 'st_context_30m_ts', 'max_age': 90 * 60, 'warmup': 2 * 3600},
        {'label': 'ST Context 1m (Scalp 1H)', 'field': 'st_context_1m_ts', 'max_age': 12 * 60, 'warmup': 20 * 60, 'scope': 'primary'},
        {'label': 'RCI 10m (Scalp 1H)', 'field': 'rci_10m_ts', 'max_age': 45 * 60, 'warmup': 90 * 60, 'scope': 'primary'},
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
            symbols = [
                symbol for symbol in CONFIG['SYMBOLS']
                if symbol not in WATCHDOG_EXCLUDED_SYMBOLS
            ]
            state_copy = {s: dict(MOMENTUM_STATE.get(s, {})) for s in symbols}
        for req in scalp_required_tv_signals():
            if uptime < req['warmup']:
                continue
            missing = []
            stale = []
            req_symbols = [s for s in symbols if req.get('scope') != 'primary' or s in SCALP_PRIMARY_SYMBOLS]
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

        # Bias 4H: source interne relayee par le bot principal.
        if uptime >= 45 * 60:
            bias4h_missing, bias4h_stale = [], []
            for symbol in symbols:
                ts = state_copy.get(symbol, {}).get('bias_4h_ts')
                if ts is None:
                    bias4h_missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > 10 * 3600:
                    bias4h_stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 60))
            if (bias4h_missing or bias4h_stale) and should_send('GLOBAL', 'scalp_bias4h_watchdog', cooldown=3600):
                details = []
                if bias4h_missing:
                    details.append("jamais recu: " + ", ".join(bias4h_missing))
                if bias4h_stale:
                    details.append("perime: " + ", ".join(f"{sym} {age:.0f}m" for sym, age in bias4h_stale))
                send_telegram(
                    "<b>[ALERTE] Relais Bias 4H (OKX) interrompu — scalp bloque</b>\n"
                    "--------------------\n"
                    + " | ".join(details)
                    + "\n\nVerifier le cycle indicateurs / relay du bot principal (pas une alerte TradingView).",
                    ntfy=False,
                )
                logger.warning(f"[BIAS 4H WATCHDOG] missing={bias4h_missing} stale={bias4h_stale}")

        # Bias 1H: requis uniquement par la strategie Scalp 1H sur la watchlist primaire.
        if uptime >= 45 * 60:
            bias1h_missing, bias1h_stale = [], []
            for symbol in symbols:
                if symbol not in SCALP_PRIMARY_SYMBOLS:
                    continue
                ts = state_copy.get(symbol, {}).get('bias_1h_ts')
                if ts is None:
                    bias1h_missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > 3 * 3600:
                    bias1h_stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 60))
            if (bias1h_missing or bias1h_stale) and should_send('GLOBAL', 'scalp_bias1h_watchdog', cooldown=3600):
                details = []
                if bias1h_missing:
                    details.append("jamais recu: " + ", ".join(bias1h_missing))
                if bias1h_stale:
                    details.append("perime: " + ", ".join(f"{sym} {age:.0f}m" for sym, age in bias1h_stale))
                send_telegram(
                    "<b>[ALERTE] Relais Bias 1H (OKX) interrompu — Scalp 1H bloque</b>\n"
                    "--------------------\n"
                    + " | ".join(details)
                    + "\n\nVerifier le cycle indicateurs / relay du bot principal.",
                    ntfy=False,
                )
                logger.warning(f"[BIAS 1H WATCHDOG] missing={bias1h_missing} stale={bias1h_stale}")

        # Bias 30m: confirmation interne de la strategie Scalp 1H.
        if uptime >= 45 * 60:
            bias30_missing, bias30_stale = [], []
            for symbol in symbols:
                if symbol not in SCALP_PRIMARY_SYMBOLS:
                    continue
                ts = state_copy.get(symbol, {}).get('bias_30m_ts')
                if ts is None:
                    bias30_missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > 90 * 60:
                    bias30_stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 60))
            if (bias30_missing or bias30_stale) and should_send('GLOBAL', 'scalp_bias30m_watchdog', cooldown=3600):
                details = []
                if bias30_missing:
                    details.append("jamais recu: " + ", ".join(bias30_missing))
                if bias30_stale:
                    details.append("perime: " + ", ".join(f"{sym} {age:.0f}m" for sym, age in bias30_stale))
                send_telegram(
                    "<b>[ALERTE] Relais Bias 30m (OKX) interrompu — Scalp 1H degrade</b>\n"
                    "--------------------\n"
                    + " | ".join(details)
                    + "\n\nLe CTX 10m aligne peut toujours confirmer l'entree.",
                    ntfy=False,
                )
                logger.warning(f"[BIAS 30m WATCHDOG] missing={bias30_missing} stale={bias30_stale}")


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
        "SCALP: Bias 4H + ST Context 10m\n"
        "CTX 30m oppose: avertissement non bloquant\n"
        "CTX 30m aligne: alerte JACKPOT\n"
        "RCI 30m: confirmation manuelle\n"
        "SCALP 1H: Bias 1H + (Bias 30m ou CTX 10m aligne) + RCI 10m +/-80 + CTX 1m\n"
        f"{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}",
        ntfy=False,
    )

    if os.environ.get('PRIORITY_SCALP_BOT_TOKEN') and os.environ.get('PRIORITY_SCALP_CHAT_ID'):
        send_notification(
            'Scalp2H connecte',
            "<b>Scalp2H connecte</b>\n"
            "--------------------\n"
            f"Strategie Scalp 1H active sur {len(SCALP_PRIMARY_SYMBOLS)} assets.\n"
            "Bias 1H + (Bias 30m ou CTX 10m aligne) + RCI 10m +/-80 + CTX 1m.",
            telegram=True,
            ntfy=False,
            telegram_channel='telegram_priority_scalp',
        )

    if os.environ.get('SCALP_SECONDARY_BOT_TOKEN') and os.environ.get('SCALP_SECONDARY_CHAT_ID'):
        send_notification(
            'ScalpSecondaire connecte',
            "<b>ScalpSecondaire connecte</b>\n"
            "--------------------\n"
            f"Watchlist PULSE: {len(PULSE_SCALP_SYMBOLS)} assets\n"
            "Les alertes scalp de cette watchlist arrivent maintenant ici.",
            telegram=True,
            ntfy=False,
            telegram_channel='telegram_secondary_scalp',
        )


if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    threading.Thread(target=startup, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
