"""Camada 7 — Logs centralizados. Importado primeiro por todos os módulos."""
from __future__ import annotations
import logging

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
log_cls = _mk_log('CLASSIF',   '1;36')
log_nrm = _mk_log('NORMAL',    '1;33')
log_ded = _mk_log('DEDUP',     '1;35')
log_enr = _mk_log('ENRICH',    '1;34')
log_out = _mk_log('ENVIO',     '1;32')
log_db  = _mk_log('DB',        '1;38;5;208')
log_sys = _mk_log('SISTEMA',   '1;37')
log_hc  = _mk_log('HEALTH',    '1;38;5;118')

