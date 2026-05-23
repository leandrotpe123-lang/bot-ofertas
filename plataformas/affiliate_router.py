"""Roteador central de afiliação por plataforma."""
from __future__ import annotations
from typing import Optional

import aiohttp

from logger import log_nrm


async def rotear_afiliacao(
    plat: str,
    url: str,
    sessao: aiohttp.ClientSession,
    msg_id: int = 0
) -> Optional[str]:
    """
    Roteia a URL para o motor de afiliação correto.
    Retorna URL afiliada ou None se não suportado.
    """

    if plat == "amazon":
        from plataformas.amazon import _afiliar_amazon
        return await _afiliar_amazon(url, sessao)

    if plat == "shopee":
        from plataformas.shopee import _afiliar_shopee
        return await _afiliar_shopee(url, sessao)

    if plat == "magalu":
        from plataformas.magalu import _afiliar_magalu
        return await _afiliar_magalu(url, sessao, msg_id)

    if plat in ("mercadolivre", "ml", "meli"):
        from plataformas.mercado_livre import _afiliar_mercadolivre
        return await _afiliar_mercadolivre(url, sessao, msg_id)
    # Plataforma desconhecida ou futura
    log_nrm.debug(f"🔮 Plataforma sem motor de afiliação: plat={plat} url={url[:60]}")
    return None
