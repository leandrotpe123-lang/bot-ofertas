"""Camada 5 — Montagem / Obtenção da IMAGEM publicável.

Responsabilidade ÚNICA: obter a imagem publicável a partir da mídia
de origem — download com timeout, validação de tamanho mínimo e
buffer nomeado. Toda falha degrada para None: imagem é opcional e
nunca derruba a publicação.

NÃO renderiza texto, NÃO conhece emoji, papel, cupom nem marcação:
isso é pipeline.montagem_texto. Não importa aquele módulo, nem é
importado por ele.

IMPORTS DIFERIDOS: `client` e `config` são importados DENTRO de
preparar_imagem_tg, exatamente como no módulo de origem. Não promova
esses imports para o topo — a posição é deliberada.

Extraído de pipeline.montagem sem qualquer alteração de comportamento.
"""
from __future__ import annotations

import asyncio
import io

from typing import Optional

import medicao_desempenho
from logger import log_enr
from pipeline.normalizacao import MensagemNormalizada

# ─────────────────────────────────────────────────────────────────
# IMAGENS
# ─────────────────────────────────────────────────────────────────

async def preparar_imagem_tg(media_obj) -> Optional[object]:

    from client import client
    import config

    t0 = medicao_desempenho.agora()

    try:

        buf = io.BytesIO()

        try:

            res = await asyncio.wait_for(
                client.download_media(
                    media_obj,
                    file=buf,
                ),
                timeout=config._TIMEOUT_DOWNLOAD_MIDIA,
            )

        except asyncio.TimeoutError:

            log_enr.warning(
                f"⏱ download_media timeout após "
                f"{config._TIMEOUT_DOWNLOAD_MIDIA}s"
            )
            medicao_desempenho.midia(t0, 0, "timeout", media_obj)

            return None

        if res is None:
            medicao_desempenho.midia(t0, 0, "vazio", media_obj)
            return None

        buf.seek(0)

        if buf.getbuffer().nbytes < 500:
            medicao_desempenho.midia(t0, buf.getbuffer().nbytes, "pequeno", media_obj)
            return None

        buf.name = "imagem.jpg"
        medicao_desempenho.midia(t0, buf.getbuffer().nbytes, "ok", media_obj)

        return buf

    except Exception as e:

        log_enr.warning(f"⚠️ download_media: {e}")
        medicao_desempenho.midia(t0, 0, "erro", media_obj)

        return None


async def _resolver_imagem(
    norm: MensagemNormalizada,
) -> object:

    if norm.tem_midia:

        img = await preparar_imagem_tg(norm.media_obj)

        if img:
            return img

    return None

