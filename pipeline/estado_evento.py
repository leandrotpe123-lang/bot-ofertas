"""
Camada — Classificação de Estado de Evento.

Responsabilidade única: determinar o estado de ciclo de vida de uma
oferta no momento em que é processada, comparando-a contra o histórico
de deduplicação.

Estados possíveis:
  - NEW       : oferta nunca vista
  - SEEN      : oferta vista recentemente, ainda dentro da janela
  - EXPIRED   : oferta vista há muito tempo, além do TTL
  - RESTOCKED : oferta reativada (texto indica retorno explícito)

NÃO faz:
  - normalização de mensagem (responsabilidade da normalização)
  - deduplicação ou claim de identidade (responsabilidade da deduplicação)
  - detecção de cupom (responsabilidade de utils.cupom)
  - verificação de viabilidade de texto (permanece na normalização)
  - persistência (apenas consulta o banco de deduplicação)
"""
from __future__ import annotations

import re
import time
from enum import Enum

from database import db_get_dedupe
from utils.hashes import _fp_c3


# ── Janelas e TTLs por plataforma ─────────────────────────────────
# Janela: tempo durante o qual a oferta é considerada "já vista".
# TTL de restock: tempo além do qual a oferta é considerada expirada.
_JANELA_C3 = {
    "shopee":  60.0,
    "amazon":  300.0,
    "magalu":  300.0,
    "default": 120.0,
}
_TTL_RESTOCK_C3 = {
    "shopee":  3600.0,
    "amazon":  7200.0,
    "magalu":  14400.0,
    "default": 3600.0,
}


# ── Vocabulário de evento e de reativação ─────────────────────────
# _KW_EVENTO detecta campanhas interativas (quiz, roleta, missão).
# Esta é a definição canônica de _KW_EVENTO no sistema.
_KW_EVENTO = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|'
    r'jogue|desafio)\b',
    re.I,
)

# _RE_RESTOCK_C3 detecta linguagem de retorno explícito de uma oferta.
_RE_RESTOCK_C3 = re.compile(
    r'voltou|restock|reativado|dispon[ií]vel\s+novamente|'
    r'voltou\s+ao\s+estoque|de\s+volta|ativo\s+novamente|normalizou|'
    r'voltando|voltou\s+cupom|relançamento',
    re.I,
)


# ── Contrato de saída ─────────────────────────────────────────────
class EstadoEvento(Enum):
    NEW       = "new"
    SEEN      = "seen"
    EXPIRED   = "expired"
    RESTOCKED = "restocked"


# ── Núcleo da classificação ───────────────────────────────────────
def detectar_estado_evento(
    texto: str,
    id_global: str,
    plat: str,
) -> EstadoEvento:
    """
    Classifica o estado de ciclo de vida de uma oferta.

    Consulta a entrada de deduplicação correspondente ao identificador
    global e à plataforma. Se não houver registro anterior, a oferta é
    NEW. Havendo registro, compara o tempo decorrido contra a janela e
    o TTL da plataforma:

      - dentro da janela              → SEEN
      - fora da janela, texto indica retorno → RESTOCKED
      - além do TTL                   → EXPIRED
      - demais casos                  → SEEN
    """
    eh_restock = bool(_RE_RESTOCK_C3.search(texto))
    entrada    = db_get_dedupe(_fp_c3(id_global, plat))

    if not entrada:
        return EstadoEvento.NEW

    ts_anterior = entrada.get("ts", 0)
    delta       = time.time() - ts_anterior
    janela      = _JANELA_C3.get(plat, _JANELA_C3["default"])
    ttl         = _TTL_RESTOCK_C3.get(plat, _TTL_RESTOCK_C3["default"])

    if delta < janela:
        return EstadoEvento.SEEN
    if eh_restock:
        return EstadoEvento.RESTOCKED
    if delta > ttl:
        return EstadoEvento.EXPIRED
    return EstadoEvento.SEEN
