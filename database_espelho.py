"""
Camada 7 — Banco: espelho do post no canal de cupons.

Responsabilidade ÚNICA: a tabela `espelho_post` — qual mensagem do
canal de cupons espelha qual post do canal principal. É PROJEÇÃO: não
participa de identidade, família, decisão nem convergência. O único
escritor é o trabalhador de pipeline.espelho_cupons (sequencial).
"""
from __future__ import annotations

import time
from typing import Optional

from database_conexao import _db


def db_espelho_get(msg_id_dest: int) -> Optional[int]:
    """msg_id do espelho do post, ou None quando não há espelho."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_espelho FROM espelho_post WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def db_espelho_set(msg_id_dest: int, msg_id_espelho: int) -> None:
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO espelho_post(msg_id_dest,msg_id_espelho,ts)"
                " VALUES(?,?,?)", (msg_id_dest, msg_id_espelho, time.time()))
    except Exception:
        pass


def db_espelho_del(msg_id_dest: int) -> None:
    try:
        with _db() as db:
            db.execute("DELETE FROM espelho_post WHERE msg_id_dest=?",
                       (msg_id_dest,))
    except Exception:
        pass


def db_espelho_mover(antigo: int, novo: int) -> None:
    """O post principal foi substituído (apagar+reenviar): o MESMO
    espelho passa a responder pelo msg_id novo."""
    try:
        with _db() as db:
            db.execute(
                "UPDATE espelho_post SET msg_id_dest=?, ts=? WHERE msg_id_dest=?",
                (novo, time.time(), antigo))
    except Exception:
        pass
