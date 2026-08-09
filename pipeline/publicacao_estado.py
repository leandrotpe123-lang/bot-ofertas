"""Camada 6 — Publicação / Estado e ponte de Origem.

Responsabilidade ÚNICA: a memória de idempotência do processo
(g._IDS_PROC) e a ponte Origem→Oferta, que consulta o vínculo e
valida a VIDA do post apontado.

Toca globals e database. NÃO fala com o Telegram, NÃO decide, NÃO
publica.

CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR:
  _marcar tem prefixo `_`, mas NÃO é privado deste arquivo: é
  importado por publicacao_aplicadores, que o chama ao concluir uma
  publicação nova. O underscore foi PRESERVADO deliberadamente na
  extração, para não renomear nada fora do escopo daquele front — é
  dívida registrada, não descuido. _marcar NÃO é promovido a
  público nem reexportado por pipeline.publicacao.

Extraído de pipeline.publicacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import time

from database import db_get_post
import globals as g
from pipeline import origem
from pipeline.vida_oferta import viva

async def _marcar(msg_id: int):
    async with g._IDS_LOCK:
        g._IDS_PROC.add(msg_id)
        if len(g._IDS_PROC) > 5000:
            for _ in range(len(g._IDS_PROC) - 4000):
                g._IDS_PROC.pop()


async def _foi_processado(msg_id: int) -> bool:
    async with g._IDS_LOCK:
        return msg_id in g._IDS_PROC


def destino_vivo_de_origem(chat: str, msg_id: int):
    """Ponte Origem→Oferta: consulta o vínculo (infra pura, I7) e valida
    aqui a VIDA do post apontado (autoridade: vida_oferta). Devolve o
    dest do post lógico VIVO, ou None (vínculo morto → fluxo normal,
    coerente com F4/renascimento por ciclo novo)."""
    if not chat or not msg_id:
        return None
    dest = origem.consultar(chat, msg_id)
    if not dest:
        return None
    estado = db_get_post(dest)
    if estado and viva(estado.get("janela_fim") or 0.0, time.time()):
        return dest
    return None

