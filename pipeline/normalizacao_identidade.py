"""
Camada 3 — Normalização / Identidade semântica.

Responsabilidade ÚNICA: derivar os FATOS SEMÂNTICOS da oferta —
produto, campanha e sinal de cashback — a partir das URLs afiliadas
LONGAS e da linha-título do texto limpo.

FRONTEIRA: lê texto para EXTRAIR SINAL, nunca para TRANSFORMAR texto.
Toda transformação de forma é exclusiva de pipeline.normalizacao_texto.

NÃO resolve, NÃO afilia, NÃO encurta, NÃO faz rede, NÃO toca cache
nem banco. Não conhece pipeline.normalizacao nem normalizacao_links.

Extraído de pipeline.normalizacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import re
from typing import List, NamedTuple, Optional, Tuple

from plataformas import registry
from plataformas.contrato import AUSENTE
from utils.urls import _netloc, host_canonico_campanha, chaves_canonicas_campanha

__all__ = [
    "IdentidadeProduto",
    "IdentidadeCampanha",
    "derivar_produto",
    "remover_cupons_da_entidade",
    "derivar_campanha",
]
