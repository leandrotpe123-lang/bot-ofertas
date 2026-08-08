"""Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada."""
#
# Implementação: pipeline.montagem_texto (renderização do texto) e
# pipeline.montagem_imagem (obtenção da imagem). Este arquivo retém o
# CONTRATO de saída e o ORQUESTRADOR, e reexporta montar_texto e
# preparar_imagem_tg para preservar a acessibilidade que já existia.
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from pipeline.normalizacao import MensagemNormalizada
from pipeline.montagem_imagem import _resolver_imagem, preparar_imagem_tg
from pipeline.montagem_texto import montar_texto

# ─────────────────────────────────────────────────────────────────
# Dataclass de saída
# ─────────────────────────────────────────────────────────────────

@dataclass
class MensagemMontada:
    msg_id:        int
    chat:          str
    plat:          str
    sku:           str
    texto:         str
    imagem:        object
    mapa:          Dict[str, str]
    msg_id_origem: int


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────

async def montar(
    norm: MensagemNormalizada,
) -> MensagemMontada:

    texto = montar_texto(norm)

    imagem = await _resolver_imagem(norm)

    return MensagemMontada(
        msg_id=norm.msg_id,
        chat=norm.chat,
        plat=norm.plat,
        sku=norm.sku,
        texto=texto,
        imagem=imagem,
        mapa=norm.mapa,
        msg_id_origem=norm.msg_id,
    )
    
