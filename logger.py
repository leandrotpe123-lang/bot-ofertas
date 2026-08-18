"""Camada 7 — Logs centralizados. Importado primeiro por todos os módulos."""
from __future__ import annotations
import logging
from datetime import datetime, timezone

def _mk_log(nome: str, cor: str) -> logging.Logger:
    lg = logging.getLogger(nome)
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            f'\033[{cor}m[%(name)-10s]\033[0m %(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'))
        lg.addHandler(h)
        lg.setLevel(logging.DEBUG)
    return lg

log_ing = _mk_log('INGESTAO',  '1;37')
log_nrm = _mk_log('NORMAL',    '1;33')
log_ded = _mk_log('DEDUP',     '1;35')
log_enr = _mk_log('ENRICH',    '1;34')
log_out = _mk_log('ENVIO',     '1;32')
log_db  = _mk_log('DB',        '1;38;5;208')
log_sys = _mk_log('SISTEMA',   '1;37')
log_hc  = _mk_log('HEALTH',    '1;38;5;118')

# ── Helpers de timeline (🧭 TL) — blindados, nunca lançam ─────────
def _ts_str(date) -> str:
    """Formata datetime tz-aware (UTC do Telegram) como HH:MM:SS local.
    Só p/ log. '-' se None, '?' em qualquer falha."""
    try:
        if date is None:
            return "-"
        return date.astimezone().strftime("%H:%M:%S")
    except Exception:
        return "?"


def _idade_str(date) -> str:
    """Idade (agora − date): '12.3s', '2m04s', '1h03m'. tz-aware.
    Blindado: nunca lança — devolve '?'."""
    try:
        if date is None:
            return "?"
        s = (datetime.now(timezone.utc) - date).total_seconds()
        if s < 0:
            s = 0.0
        if s < 60:
            return f"{s:.1f}s"
        m, seg = divmod(int(s), 60)
        if m < 60:
            return f"{m}m{seg:02d}s"
        h, m = divmod(m, 60)
        return f"{h}h{m:02d}m"
    except Exception:
        return "?"

def _idade_seg(date) -> float:
    """Idade em segundos (agora − date), tz-aware. Blindado: devolve
    -1.0 se não der p/ calcular — chamador trata como 'não descartar'."""
    try:
        if date is None:
            return -1.0
        return (datetime.now(timezone.utc) - date).total_seconds()
    except Exception:
        return -1.0
