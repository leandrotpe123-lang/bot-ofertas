"""
Camada — Ritmo de entrega (saturação e rajada).

Responsabilidade ÚNICA: responder se o sistema está publicando rápido
demais e reter o estado que sustenta essa resposta.

Duas perguntas, um domínio:
  - quanto atrasar antes de publicar (delay_saturacao)
  - registrar que uma publicação ocorreu (registrar_saturacao / registrar_burst)

Retém dois estados próprios:
  - contagem por plataforma, persistente (tabela saturacao)
  - janela de rajada, em memória (globals._burst)

NÃO faz:
  - decidir publicar/editar/ignorar    (pipeline.decisao)
  - aplicar o veredito no destino      (pipeline.publicacao)
  - falar com o Telegram               (pipeline.saida)
  - derivar identidade ou família      (pipeline.identidade_oferta)

DÍVIDA HERDADA (F1 moveu sem alterar — NÃO corrigir aqui):
  delay_saturacao reclassifica o post pelo texto via _KW_EVENTO, quando
  a natureza já viaja pronta em enr.tipo desde o enriquecimento. Os dois
  conjuntos NÃO são equivalentes; a troca muda quem fura a saturação e
  é decisão de negócio, registrada para frente própria.
"""
from __future__ import annotations

import time

import globals as g
from database import db_count_sat, db_registrar_sat
from pipeline.estado_evento import _KW_EVENTO


__all__ = [
    "delay_saturacao",
    "registrar_saturacao",
    "registrar_burst",
]


# ── Política de ritmo ─────────────────────────────────────────────
_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60


# ── Janela de rajada (estado em memória) ──────────────────────────
async def registrar_burst():
    async with g._BURST_LOCK:
        agora = time.monotonic(); g._burst.append(agora)
        while g._burst and agora - g._burst[0] > _SAT_BURST_JAN:
            g._burst.pop(0)


async def _burst_count() -> int:
    async with g._BURST_LOCK:
        agora = time.monotonic()
        return sum(1 for t in g._burst if agora - t <= _SAT_BURST_JAN)


# ── Contagem por plataforma (estado persistente) ──────────────────
def registrar_saturacao(plat: str, sku: str = "") -> None:
    db_registrar_sat(plat, sku)


# ── Consulta de ritmo ─────────────────────────────────────────────
async def delay_saturacao(plat: str, texto: str) -> float:
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay
