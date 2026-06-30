"""
Plataforma Shopee — afiliação.

Domínio único: a integração com o serviço externo de afiliados da
Shopee — chamada GraphQL autenticada por assinatura criptográfica,
política de novas tentativas, expansão de encurtador, cache de links
mediado e repasse direto. É a capacidade de efeito colateral
controlado do contrato (acessa rede).

Fluxo de dependência: depende de `links` (reconhecimento de
encurtador, canonização e identidade da plataforma) e dos recursos
externos do core (cache, resolver, config). Não é importado por
`links` — a direção é única.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Optional

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url

from .links import (
    _ENCURTADORES,
    _IDENTIFICADOR,
    _bate_dominio,
    _canonica_live,
    _url_produto_canonica,
    limpa_url,
)


_SHP_APP_ID = os.environ.get("SHOPEE_APP_ID", "")
_SHP_SECRET = os.environ.get("SHOPEE_SECRET", "")


# ── Endpoint do serviço de afiliados ──────────────────────────────
_ENDPOINT_AFILIADOS = "https://open-api.affiliate.shopee.com.br/graphql"
_TENTATIVAS_AFILIACAO = 3
_TIMEOUT_AFILIACAO = 12

# ── Política de repasse direto (sem afiliação) ────────────────────
# Domínio cujas URLs são publicadas como recebidas, sem passar pelo
# serviço de afiliados. É política de afiliação, não classificação.
_REPASSE_DIRETO = frozenset({"flapremios.com.br"})


async def _chamar_servico_afiliados(
    url_produto: str, sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Chama o serviço externo de geração de link afiliado.

    Aplica autenticação por assinatura criptográfica e política de
    novas tentativas com intervalo progressivo. Devolve o link
    afiliado, ou None quando o serviço não o produz.
    """
    for tentativa in range(1, _TENTATIVAS_AFILIACAO + 1):
        try:
            ts = str(int(time.time()))
            payload = json.dumps(
                {"query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url_produto}" }}) '
                    f'{{ shortLink }} }}'
                )},
                separators=(",", ":"),
            )
            assinatura = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()
            headers = {
                "Authorization": (
                    f"SHA256 Credential={_SHP_APP_ID},"
                    f"Timestamp={ts},Signature={assinatura}"
                ),
                "Content-Type": "application/json",
            }
            async with config._SEM_HTTP:
                async with sessao.post(
                    _ENDPOINT_AFILIADOS,
                    data=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(
                        total=_TIMEOUT_AFILIACAO,
                    ),
                ) as resposta:
                    dados = await resposta.json()
                    link = (
                        dados.get("data", {})
                        .get("generateShortLink", {})
                        .get("shortLink")
                    )
                    if link:
                        log_nrm.info(
                            f"✅ SHP t={tentativa}: {link}"
                        )
                        return link
                    log_nrm.warning(
                        f"⚠️ SHP serviço t={tentativa}: "
                        f"{dados.get('errors') or dados.get('error')}"
                    )
        except Exception as e:
            log_nrm.warning(f"⚠️ SHP t={tentativa}: {e}")
        if tentativa < _TENTATIVAS_AFILIACAO:
            await asyncio.sleep(tentativa * 1.5)
    return None


async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Shopee na sua forma afiliada.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e para chamar o serviço externo de
    afiliados, e consulta o cache de links mediado. Não propaga
    exceções: qualquer falha legítima resulta no sentinela AUSENTE.

    Garante que a URL devolvida pertence à Shopee.
    """
    url = _sanitizar_url(url)
    netloc = _netloc(url)

    # Domínio de repasse direto: devolvido sem afiliação.
    if _bate_dominio(netloc, _REPASSE_DIRETO):
        log_nrm.info(f"↩️ SHP repasse direto: {url[:60]}")
        return url

    # Consulta ao cache mediado.
    cache = consultar_link(url)
    if cache:
        return cache

    # Expansão de encurtador próprio, quando aplicável.
    url_expandida = url
    if netloc in _ENCURTADORES:
        try:
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
        except Exception as e:
            log_nrm.warning(f"⚠️ SHP expansão falhou: {e}")
            return AUSENTE

    # Chamada ao serviço de afiliados, sobre a URL limpa.
    url_limpa = limpa_url(url_expandida)
    link = await _chamar_servico_afiliados(url_limpa, sessao)

    # Mecanismo de recuperação: tenta a URL canônica de produto.
    if not link:
        url_canonica = _url_produto_canonica(url_expandida)
        if url_canonica and url_canonica != url_limpa:
            log_nrm.info(f"🔄 SHP recuperação: {url_canonica[:60]}")
            link = await _chamar_servico_afiliados(url_canonica, sessao)

    # Falha legítima: ausência explícita.
    if not link:
        log_nrm.warning(f"⚠️ SHP sem afiliação: {url[:60]}")
        return AUSENTE

    # Validação da URL afiliada resultante.
    if "shopee" not in _netloc(link):
        log_nrm.warning(f"⚠️ SHP validação falhou: {link}")
        return AUSENTE

    canonica = url_limpa
    if (_netloc(url_limpa) or "").lower() == "live.shopee.com.br":
        canonica = _canonica_live(url_limpa)
    resultado = Afiliacao(publicada=link, canonica=canonica)
    registrar_link(url, resultado, _IDENTIFICADOR)
    return resultado

