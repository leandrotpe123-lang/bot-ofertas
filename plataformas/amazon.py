"""
Plataforma — Amazon.

Módulo autocontido que descreve integralmente a plataforma Amazon
e cumpre o contrato de plataforma. Consolida o conhecimento que
antes se encontrava disperso entre os módulos de classificação,
de estado de evento e de limpeza de parâmetros.

Expõe a instância `PLATAFORMA`, que é registrada no registry
durante a inicialização do sistema.

Este módulo depende do contrato, dos utilitários do core e dos
recursos externos. Não depende da pipeline, do registry nem da
orquestração. Não acessa o banco de dados diretamente: o cache
de links é mediado por `utils.cache_links`.

Baseline arquitetural: Documento 1 — Especificação do Contrato.
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

import config
import os

_AMZ_TAG = os.environ.get("AMAZON_TAG", "leo21073-20")
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
_IDENTIFICADOR = "amazon"


# ── Domínios e encurtadores ───────────────────────────────────────
_DOMINIOS = frozenset({
    "amazon.com.br", "amazon.com",
})
_ENCURTADORES = frozenset({
    "amzn.to", "link.amazon", "a.co", "amzn.com", "amzlink.to",
})


# ── Quirk HTTP: hosts que exigem GET na resolução ─────────────────
# Hosts cujos servidores não respondem corretamente a requisições
# HEAD, exigindo GET direto no resolver de redirecionamento. Hoje
# coincide com _ENCURTADORES, mas a relação NÃO é definicional: é
# fato empírico, sujeito a revisão por host individualmente.
_ENCURTADORES_FORCA_GET = frozenset({
    "amzn.to", "a.co", "amzn.com", "amzlink.to",
})

# ── Hosts de campanha ─────────────────────────────────────────────
_HOSTS_CAMPANHA = frozenset({
    "amazon.com.br",
    "primevideo.com",
})

# ── Padrões de extração ───────────────────────────────────────────
_P_ASIN = [
    re.compile(r'/dp/([A-Z0-9]{10})', re.I),
    re.compile(r'/gp/product/([A-Z0-9]{10})', re.I),
    re.compile(r'[?&]asin=([A-Z0-9]{10})', re.I),
]
_P_PROMO = re.compile(r'/promotion/psp/([A-Z0-9]{8,16})', re.I)
_PATHS_SEM_AFILIACAO = re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|'
    r'gp/css(?:/|$)|gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|'
    r'hz/|ap/|gp/registry(?:/|$))',
    re.I,
)


# ── Parâmetros de limpeza de URL ──────────────────────────────────
_PARAMS_MANTER = frozenset({
    "keywords", "node", "k", "i", "rh", "n", "field-keywords",
})


# ── Parâmetros temporais de deduplicação ──────────────────────────
_PARAMETROS_TEMPORAIS = ParametrosTemporais(
    janela_s=300.0,
    ttl_restock_s=7200.0,
)


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_asin(parsed) -> str:
    """Extrai o ASIN de uma URL Amazon, ou string vazia se ausente."""
    texto = parsed.path + "?" + parsed.query
    for padrao in _P_ASIN:
        m = padrao.search(texto)
        if m:
            return m.group(1).upper()
    return ""


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence à Amazon (domínio próprio ou
    encurtador próprio). Pura e determinística. Não falha: uma
    URL malformada simplesmente não é reconhecida.
    """
    if not url:
        return False
    netloc = _netloc(url)
    if not netloc:
        return False
    return _bate_dominio(netloc, _DOMINIOS) or netloc in _ENCURTADORES


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """
    Extrai a identidade estruturada de uma URL da Amazon.

    Pura e determinística. Para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida, ainda que com identificador
    de produto ausente.

    Classificação do tipo de link:
      - encurtador próprio          → ENCURTADO
      - caminho sem afiliação        → INVALIDO
      - ASIN presente                → PRODUTO
      - identificador de promoção    → CAMPANHA
      - caminho de busca             → BUSCA
      - caminho de evento ou loja    → EVENTO
      - demais casos                 → CAMPANHA
    """
    netloc = _netloc(url)

    # Encurtador: natureza final desconhecida até a expansão.
    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)

    # Caminho que pertence à Amazon mas não comporta afiliação.
    if _PATHS_SEM_AFILIACAO.match(parsed.path):
        return IdentidadeProduto(
            tipo_link=TipoLink.INVALIDO, id_produto=AUSENTE,
        )

    # Produto identificado por ASIN.
    asin = _extrair_asin(parsed)
    if asin:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=asin,
            id_global=f"{_IDENTIFICADOR}:{asin}",
        )

    # Campanha de promoção identificada.
    promo = _P_PROMO.search(parsed.path)
    if promo:
        promo_id = promo.group(1).upper()
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA,
            id_produto=AUSENTE,
            id_global=f"{_IDENTIFICADOR}:promo_{promo_id}",
        )

    # Demais naturezas, sem identificador de produto.
    if re.search(r'/s[/?]|/deals|/b[/?]', parsed.path):
        tipo = TipoLink.BUSCA
    elif re.search(r'/events/|/stores/', parsed.path):
        tipo = TipoLink.EVENTO
    else:
        tipo = TipoLink.CAMPANHA

    return IdentidadeProduto(tipo_link=tipo, id_produto=AUSENTE)


# ── Capacidade opcional: limpeza de URL ───────────────────────────
def limpa_url(url: str) -> str:
    """
    Remove parâmetros de rastreamento da URL, preservando os
    parâmetros funcionais. Pura e determinística. Preserva a
    identidade canônica do produto.
    """
    try:
        parsed = urlparse(url)
        params = {
            k: v[0] for k, v in parse_qs(parsed.query).items()
            if k.lower() in _PARAMS_MANTER and len(v[0]) < 60
        }
        return urlunparse(parsed._replace(
            query=urlencode(params), fragment="",
        ))
    except Exception:
        return url


# ── Capacidade obrigatória: afiliação ─────────────────────────────
def _construir_url_afiliada(url: str) -> Optional[str]:
    """
    Constrói a URL afiliada da Amazon a partir de uma URL já
    expandida. Anexa a tag de afiliado. Devolve None quando a
    construção não é possível.
    """
    try:
        parsed = urlparse(url)

        # Caminho sem afiliação: devolve a URL limpa, sem tag.
        if _PATHS_SEM_AFILIACAO.match(parsed.path):
            return urlunparse(parsed._replace(query="", fragment=""))

        # Produto: forma canônica /dp/ASIN com a tag.
        asin = _extrair_asin(parsed)
        if asin:
            return urlunparse(parsed._replace(
                path=f"/dp/{asin}",
                query=f"tag={_AMZ_TAG}",
                fragment="",
            ))

        # Promoção: preserva o caminho, anexa a tag.
        if "/promotion/" in parsed.path:
            return urlunparse(parsed._replace(
                query=f"tag={_AMZ_TAG}", fragment="",
            ))

        # Demais casos: limpa e anexa a tag.
        limpa = limpa_url(url)
        p_limpa = urlparse(limpa)
        query = parse_qs(p_limpa.query)
        query["tag"] = [_AMZ_TAG]
        pares = [(k, v) for k, vs in query.items() for v in vs]
        return urlunparse(p_limpa._replace(
            query=urlencode(pares), fragment="",
        ))
    except Exception:
        return None


async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Amazon na sua forma afiliada.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e consulta o cache de links mediado. Não
    propaga exceções ao core: qualquer falha legítima resulta no
    sentinela AUSENTE.

    Garante que a URL devolvida pertence à Amazon e preserva a
    identidade canônica do produto original.
    """
    url = _sanitizar_url(url)

    # Consulta ao cache mediado, antes de qualquer processamento.
    cache = consultar_link(url)
    if cache:
        return cache

    # Expansão de encurtador próprio, quando aplicável.
    if _netloc(url) in _ENCURTADORES:
        try:
            async with config._SEM_HTTP:
                url = await desencurtar(url, sessao)
        except Exception as e:
            log_nrm.warning(f"⚠️ AMZ expansão falhou: {e}")
            return AUSENTE

    # Caminho que não comporta afiliação: devolve forma limpa.
    identidade = extrai_identidade(url)
    if identidade.tipo_link == TipoLink.INVALIDO:
        afiliada = _construir_url_afiliada(url)
        if afiliada:
            registrar_link(url, afiliada, _IDENTIFICADOR)
            return afiliada
        return AUSENTE

    # Construção da URL afiliada.
    afiliada = _construir_url_afiliada(url)
    if not afiliada or "amazon" not in _netloc(afiliada):
        log_nrm.warning(f"⚠️ AMZ afiliação inválida: {afiliada}")
        return AUSENTE

    registrar_link(url, afiliada, _IDENTIFICADOR)
    log_nrm.info(f"✅ AMZ afiliada: {afiliada[:70]}")
    return afiliada


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    parametros_temporais=_PARAMETROS_TEMPORAIS,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
    hosts_campanha=_HOSTS_CAMPANHA,
)
