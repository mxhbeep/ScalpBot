#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scalping Bot V3
# Principale : ZALT 30m + ZALT 10m + ST Context 1m + flip ZALT 1m | anti-chop ST Context 3m
# Secondaire : RPZ 30m + ZALT 10m + ST Context 1m + flip ZALT 1m | anti-chop ST Context 3m
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
        'APT/USDT': {'exchange': 'okx'},
        'BTC/USDT': {'exchange': 'okx'},
        'CRV/USDT': {'exchange': 'okx'},
        'CVX/USDT': {'exchange': 'okx'},
        'DOGE/USDT': {'exchange': 'okx'},
        'ETH/USDT': {'exchange': 'okx'},
        'FARTCOIN/USDT': {'exchange': 'okx'},
        'HYPE/USDT': {'exchange': 'okx'},
        'LINK/USDT': {'exchange': 'okx'},
        'PENGU/USDT': {'exchange': 'okx'},
        'PEPE/USDT': {'exchange': 'okx'},
        'USELESS/USDT': {'exchange': 'okx'},
        'XPL/USDT': {'exchange': 'okx'},
        'XRP/USDT': {'exchange': 'okx'},
        'ZEC/USDT': {'exchange': 'okx'},
    },
}

STATE_LOCK = threading.RLock()
MOMENTUM_STATE = {}
LAST_SIGNALS = {}
LAST_SIGNAL_EVENTS = {}
SCALP_ENABLED = True
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
            'zalt_1m': None, 'zalt_1m_ts': None, 'last_zalt_1m_signal_ts': None,
            'zalt_30m': None, 'zalt_30m_ts': None,
            'zalt_1h': None, 'zalt_1h_ts': None,
            'rpz_1h': None, 'rpz_1h_ts': None,
            'st_context_3m': None, 'st_context_3m_ts': None, 'st_context_3m_raw': None,
            'st_context_30m': None, 'st_context_30m_ts': None, 'st_context_30m_raw': None,
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


def _trend_ok(m, exp):
    zalt1h_ok = is_fresh(m.get('zalt_1h_ts'), 3 * 3600) and m.get('zalt_1h') == exp
    rpz1h_ok = is_fresh(m.get('rpz_1h_ts'), 3 * 3600) and m.get('rpz_1h') == exp
    zalt30_ok = is_fresh(m.get('zalt_30m_ts'), 90 * 60) and m.get('zalt_30m') == exp
    return zalt1h_ok or (rpz1h_ok and zalt30_ok), zalt1h_ok, rpz1h_ok, zalt30_ok


def evaluate_scalp_v3(symbol, trigger_dir=None, price=0, event_id=None, trigger_label="state_refresh"):
    """SCALP V3.1 principale: (ZALT 1H ou RPZ 1H+ZALT 30m) + ST Context 30m + ST Context 3m + flip ZALT 1m."""
    if trigger_dir not in (None, 'buy', 'sell'):
        return False
    notify_payload = None
    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        if not SCALP_ENABLED:
            logger.info(f"[SCALP OFF] ignore {symbol}")
            return False
        directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
        selected = None
        for exp in directions:
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            trend_ok, zalt1h_ok, rpz1h_ok, zalt30_ok = _trend_ok(m, exp)
            ctx30_ok = is_fresh(m.get('st_context_30m_ts'), 90 * 60) and m.get('st_context_30m') == exp
            ctx3_ok = is_fresh(m.get('st_context_3m_ts'), 10 * 60) and m.get('st_context_3m') == exp
            zalt1_ok = is_fresh(m.get('zalt_1m_ts'), 5 * 60) and m.get('zalt_1m') == exp
            flip_ok = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
            primary_ok = trend_ok and ctx30_ok and ctx3_ok and zalt1_ok and flip_ok
            logger.info(
                f"[SCALP PRIM] {symbol} {direction} src={trigger_label} "
                f"trend={trend_ok} z1h={zalt1h_ok} rpz1h={rpz1h_ok} z30={zalt30_ok} "
                f"ctx30={ctx30_ok} ctx3={ctx3_ok} z1m={zalt1_ok} flip={flip_ok} ok={primary_ok}"
            )
            if primary_ok:
                selected = (exp, direction, zalt1h_ok, rpz1h_ok, zalt30_ok)
                break
        if not selected:
            return False
        exp, direction, zalt1h_ok, rpz1h_ok, zalt30_ok = selected
        if not should_send(symbol, f"scalp_v3_primary_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
            return False
        trend_txt = "ZALT 1H" if zalt1h_ok else "RPZ 1H + ZALT 30m"
        notify_payload = (direction, symbol, price, trend_txt)
    if notify_payload:
        direction, symbol, price, trend_txt = notify_payload
        emoji = "🟢" if direction == "LONG" else "🔴"
        send_telegram_with_buttons(
            f"{emoji} SCALP {direction} {symbol}\n"
            f"--------------------\n"
            f"Price: ${format_price(price)}\n"
            f"Principale: CTX 30m + CTX 3m + flip ZALT 1m\n"
            f"Tendance: {trend_txt}"
        )
        return True
    return False


def evaluate_scalp_v3_secondary(symbol, trigger_dir=None, price=0, event_id=None, trigger_label="state_refresh"):
    """SCALP V3.1 secondaire: (ZALT 1H+ZALT 30m+CTX 3m) ou (RPZ 1H+ZALT 30m+CTX 3m) + flip ZALT 1m."""
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
        path = None
        for exp in directions:
            direction = 'LONG' if exp == 'buy' else 'SHORT'
            zalt1h_ok = is_fresh(m.get('zalt_1h_ts'), 3 * 3600) and m.get('zalt_1h') == exp
            rpz1h_ok = is_fresh(m.get('rpz_1h_ts'), 3 * 3600) and m.get('rpz_1h') == exp
            zalt30_ok = is_fresh(m.get('zalt_30m_ts'), 90 * 60) and m.get('zalt_30m') == exp
            ctx3_ok = is_fresh(m.get('st_context_3m_ts'), 10 * 60) and m.get('st_context_3m') == exp
            zalt1_ok = is_fresh(m.get('zalt_1m_ts'), 5 * 60) and m.get('zalt_1m') == exp
            flip_ok = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
            path_a = zalt1h_ok and zalt30_ok and ctx3_ok and zalt1_ok and flip_ok
            path_b = rpz1h_ok and zalt30_ok and ctx3_ok and zalt1_ok and flip_ok
            logger.info(
                f"[SCALP SEC] {symbol} {direction} src={trigger_label} "
                f"A={path_a} B={path_b} z1h={zalt1h_ok} rpz1h={rpz1h_ok} "
                f"z30={zalt30_ok} ctx3={ctx3_ok} flip={flip_ok}"
            )
            if path_a or path_b:
                selected = (exp, direction)
                path = "ZALT 1H + ZALT 30m + CTX 3m" if path_a else "RPZ 1H + ZALT 30m + CTX 3m"
                break
        if not selected:
            return False
        exp, direction = selected
        if not should_send(symbol, f"scalp_v3_secondary_{exp}", event_id=event_id, cooldown=CONFIG['MIN_COOLDOWN']):
            return False
        notify_payload = (direction, symbol, price, path)
    if notify_payload:
        direction, symbol, price, path = notify_payload
        emoji = "🟢" if direction == "LONG" else "🔴"
        send_telegram_with_buttons(
            f"{emoji} SCALP {direction} SEC {symbol}\n"
            f"--------------------\n"
            f"Price: ${format_price(price)}\n"
            f"Secondaire: {path} + flip ZALT 1m"
        )
        return True
    return False
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
        'reversal_probability_zone': 'rpz',
        'reversal_probability': 'rpz',
        'rpz_zone': 'rpz',
        'stcontext': 'st_context',
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
    parsed_dir = parse_dir_value(val) if alert_type in ('zalt', 'rpz') else None

    with STATE_LOCK:
        init_symbol(symbol)
        m = MOMENTUM_STATE[symbol]
        logger.info(f"Webhook: {symbol} | tf={tf} | type={alert_type} | val={val} | signal={zalt_signal or '-'}")

        if alert_type == 'zalt':
            if parsed_dir is None:
                logger.warning(f"[WEBHOOK] ZALT invalide: {symbol} tf={tf} value={val!r}")
                return
            if tf in ('1m', '30m', '1h'):
                m[f'zalt_{tf}'] = parsed_dir
                m[f'zalt_{tf}_ts'] = time.time()
                if tf == '1m' and zalt_signal in ('trend_flip', 'flip'):
                    m['last_zalt_1m_signal_ts'] = time.time()
                persist_state()
            else:
                logger.info(f"[ZALT] {symbol} tf={tf} ignore: timeframe non utilise par SCALP V3")
                return

        elif alert_type == 'rpz' and tf == '1h':
            if parsed_dir is None:
                logger.warning(f"[WEBHOOK] RPZ invalide: {symbol} tf={tf} value={val!r}")
                return
            m['rpz_1h'] = parsed_dir
            m['rpz_1h_ts'] = time.time()
            persist_state()

        elif alert_type == 'st_context' and tf in ('3m', '30m'):
            ctx_parsed, ctx_raw = parse_st_context_value(val)
            m[f'st_context_{tf}'] = ctx_parsed
            m[f'st_context_{tf}_ts'] = time.time()
            m[f'st_context_{tf}_raw'] = ctx_raw
            persist_state()
        else:
            return

    trigger_dir = None
    if alert_type == 'zalt' and tf == '1m' and zalt_signal in ('trend_flip', 'flip'):
        trigger_dir = parsed_dir

    if alert_type in ('zalt', 'rpz', 'st_context'):
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
        exp = 'buy' if direction == 'LONG' else 'sell' if direction == 'SHORT' else None
        zalt1h = signal_debug_payload(m, 'zalt_1h', 3 * 3600)
        rpz1h = signal_debug_payload(m, 'rpz_1h', 3 * 3600)
        zalt30 = signal_debug_payload(m, 'zalt_30m', 90 * 60)
        zalt1_sig = signal_debug_payload(m, 'zalt_1m', 5 * 60)
        ctx30m = signal_debug_payload(m, 'st_context_30m', 90 * 60)
        ctx3m = signal_debug_payload(m, 'st_context_3m', 10 * 60)
        flip_fresh = is_fresh(m.get('last_zalt_1m_signal_ts'), 5 * 60)
        if direction:
            zalt1h_ok = zalt1h['fresh'] and zalt1h['value'] == exp
            rpz1h_ok = rpz1h['fresh'] and rpz1h['value'] == exp
            zalt30_ok = zalt30['fresh'] and zalt30['value'] == exp
            zalt1_ok = zalt1_sig['fresh'] and zalt1_sig['value'] == exp
            ctx30_ok = ctx30m['fresh'] and ctx30m['value'] == exp
            ctx3_ok = ctx3m['fresh'] and ctx3m['value'] == exp
            trend_ok = zalt1h_ok or (rpz1h_ok and zalt30_ok)
            primary_ok = trend_ok and ctx30_ok and ctx3_ok and zalt1_ok and flip_fresh
            secondary_path_a = zalt1h_ok and zalt30_ok and ctx3_ok and zalt1_ok and flip_fresh
            secondary_path_b = rpz1h_ok and zalt30_ok and ctx3_ok and zalt1_ok and flip_fresh
            secondary_ok = secondary_path_a or secondary_path_b
        else:
            zalt1h_ok = rpz1h_ok = zalt30_ok = zalt1_ok = ctx30_ok = ctx3_ok = trend_ok = primary_ok = False
            secondary_path_a = secondary_path_b = secondary_ok = False
        return jsonify({
            'status': 'ok',
            'symbol': symbol,
            'enabled': SCALP_ENABLED,
            'now_shanghai': datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'scalp_v3_1': {
                'direction_from_zalt_1m': direction,
                'expected': exp,
                'flip_1m_fresh': flip_fresh,
                'trend_ok': trend_ok,
                'principale_ok': primary_ok,
                'secondaire_ok': secondary_ok,
                'secondaire_path_a_zalt1h': secondary_path_a,
                'secondaire_path_b_rpz1h': secondary_path_b,
                'zalt1h_ok': zalt1h_ok,
                'rpz1h_ok': rpz1h_ok,
                'zalt30_ok': zalt30_ok,
                'zalt1m_ok': zalt1_ok,
                'ctx30m_ok': ctx30_ok,
                'ctx3m_ok': ctx3_ok,
            },
            'signals': {
                'zalt_1h': zalt1h,
                'rpz_1h': rpz1h,
                'zalt_30m': zalt30,
                'zalt_1m': zalt1_sig,
                'last_zalt_1m_signal_ts': m.get('last_zalt_1m_signal_ts'),
                'st_context_30m': ctx30m,
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
        {'label': 'ZALT 1H', 'field': 'zalt_1h_ts', 'max_age': 3 * 3600, 'warmup': 4 * 3600},
        {'label': 'ZALT 30m', 'field': 'zalt_30m_ts', 'max_age': 90 * 60, 'warmup': 2 * 3600},
        {'label': 'ZALT 1m', 'field': 'zalt_1m_ts', 'max_age': 5 * 60, 'warmup': 10 * 60},
        {'label': 'RPZ 1H', 'field': 'rpz_1h_ts', 'max_age': 3 * 3600, 'warmup': 4 * 3600},
        {'label': 'ST Context 30m', 'field': 'st_context_30m_ts', 'max_age': 90 * 60, 'warmup': 2 * 3600},
        {'label': 'ST Context 3m', 'field': 'st_context_3m_ts', 'max_age': 10 * 60, 'warmup': 20 * 60},
    ]


def scalp_watchdog_max_age(symbol, req):
    """CVX est tolerant a 24h sur tous les signaux du watchdog (asset moins liquide/actif,
    evite le bruit de fausses alertes 'signal manquant')."""
    if symbol == 'CVX/USDT':
        return 24 * 60 * 60
    return req['max_age']


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
        "Strategie active: SCALP V3\n"
        "Principale: tendance (ZALT1H ou RPZ1H+ZALT30) + CTX30 + CTX3 + flip ZALT1m\n"
        "Secondaire: (ZALT1H+ZALT30 ou RPZ1H+ZALT30) + CTX3 + flip ZALT1m\n"
        "Anti-chop: ST Context 3m aligne\n"
        f"{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}",
        ntfy=False,
    )


if os.environ.get('ENABLE_SCALP_BOT', '1') == '1':
    threading.Thread(target=startup, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)