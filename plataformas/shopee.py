"""
Plataforma — Shopee.

Módulo autocontido que descreve integralmente a plataforma Shopee
e cumpre o contrato de plataforma. Consolida o conhecimento que
antes se encontrava disperso entre os módulos de classificação,
de estado de evento e de limpeza de parâmetros.

Expõe a instância `PLATAFORMA`, registrada no registry durante a
inicialização do sistema.

A capacidade de afiliação depende de um serviço externo de geração
de link, autenticado por assinatura criptográfica. É, por contrato,
uma capacidade com efeito colateral controlado.

Este módulo depende do contrato, dos utilitários do core e dos
recursos externos. Não depende da pipeline, do registry nem da
orquestração. Não acessa o banco de dados diretamente.

Baseline arquitetural: Documento 1 — Especificação do Contrato.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Optional
from urllib.parse import urlparse

import aiohttp

from config import _SHP_APP_ID, _SHP_SECRET, _SEM_HTTP
from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    ParametrosTemporais,
    Plataforma,
    TipoLink,
)
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url


# ── Identidade da plataforma ──────────────────────────────────────
_IDENTIFICADOR = "shopee"


# ── Domínios e encurtadores ───────────────────────────────────────
_DOMINIOS = frozenset({
    "shopee.com.br", "s.shopee.com.br", "shopee.com",
    "shope.ee", "flapremios.com.br",
})
_ENCURTADORES = frozenset({
    "s.shopee.com.br", "shope.ee", "s.shopee.com",
})

# Domínio de campanha de tratamento por repasse direto, sem afiliação.
_REPASSE_DIRETO = frozenset({"flapremios.com.br"})


# ── Padrões de extração do identificador de produto ───────────────
# Na Shopee, o produto é identificado pelo par (loja, item).
_P_PRODUTO = [
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/item/(\d+)/(\d+)'),
    re.compile(r'/i\.(\d+)\.(\d+)'),
]


# ── Endpoint do serviço de afiliados ──────────────────────────────
_ENDPOINT_AFILIADOS = "https://open-api.affiliate.shopee.com.br/graphql"
_TENTATIVAS_AFILIACAO = 3
_TIMEOUT_AFILIACAO = 12


# ── Parâmetros temporais de deduplicação ──────────────────────────
_PARAMETROS_TEMPORAIS = ParametrosTemporais(
    janela_s=60.0,
    ttl_restock_s=3600.0,
)


import re  # posicionado após as constantes para clareza de leitura


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_par_produto(parsed) -> str:
    """
    Extrai o identificador de produto da Shopee no formato
    'loja.item', ou string vazia se ausente.
    """
    texto = parsed.path + "?" + parsed.query
    for padrao in _P_PRODUTO:
        m = padrao.search(texto)
        if m:
            return f"{m.group(1)}.{m.group(2)}"
    return ""


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence à Shopee (domínio próprio ou
    encurtador próprio). Pura e determinística. Não falha.
    """
    if not url:
        return False
    netloc = _netloc(url)
    if not netloc:
        return False
    return _bate_dominio(netloc, _DOMINIOS)


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """
    Extrai a identidade estruturada de uma URL da Shopee.

    Pura e determinística. Para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida.

    Classificação do tipo de link:
      - domínio de repasse direto    → CAMPANHA
      - encurtador próprio            → ENCURTADO
      - par (loja, item) presente     → PRODUTO
      - demais casos                  → BUSCA
    """
    netloc = _netloc(url)

    # Domínio de campanha de repasse direto.
    if netloc == "flapremios.com.br":
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    # Encurtador: natureza final desconhecida até a expansão.
    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)
    par = _extrair_par_produto(parsed)
    if par:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=par,
            id_global=f"{_IDENTIFICADOR}:{par}",
        )

    return IdentidadeProduto(tipo_link=TipoLink.BUSCA, id_produto=AUSENTE)


# ── Capacidade opcional: limpeza de URL ───────────────────────────
def limpa_url(url: str) -> str:
    """
    Remove parâmetros de rastreamento da URL, preservando os
    parâmetros funcionais de identificação. Pura e determinística.
    Preserva a identidade canônica do produto.
    """
    from urllib.parse import parse_qs, urlencode, urlunparse
    try:
        parsed = urlparse(url)
        params = {
            k: v[0] for k, v in parse_qs(parsed.query).items()
            if k in {"shopid", "itemid", "smtt"}
        }
        return urlunparse(parsed._replace(
            query=urlencode(params) if params else "",
            fragment="",
        ))
    except Exception:
        return url


# ── Capacidade obrigatória: afiliação ─────────────────────────────
def _url_produto_canonica(url: str) -> Optional[str]:
    """
    Constrói a URL canônica de produto a partir de uma URL da
    Shopee, quando o par (loja, item) está presente. Usada como
    mecanismo de recuperação na afiliação.
    """
    try:
        parsed = urlparse(url)
        for padrao in _P_PRODUTO:
            m = padrao.search(parsed.path + "?" + parsed.query)
            if m:
                return (
                    f"https://shopee.com.br/product/"
                    f"{m.group(1)}/{m.group(2)}"
                )
    except Exception:
        pass
    return None


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
            async with _SEM_HTTP:
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
            async with _SEM_HTTP:
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

    registrar_link(url, link, _IDENTIFICADOR)
    return link


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    parametros_temporais=_PARAMETROS_TEMPORAIS,
    pos_processa=None,
    limpa_url=limpa_url,
)
