"""
Plataforma — Magalu.

Módulo autocontido que descreve integralmente a plataforma Magalu
e cumpre o contrato de plataforma. Consolida o conhecimento antes
disperso entre os módulos de classificação, de estado de evento e
de limpeza de parâmetros.

Expõe a instância PLATAFORMA, registrada no registry durante a
inicialização do sistema.

A capacidade de afiliação produz a URL afiliada longa. O
encurtamento NÃO pertence a este módulo: é comportamento do core,
acionado pela declaração requer_encurtamento.

Depende do contrato, dos utilitários do core e dos recursos
externos. Não depende da pipeline, do registry nem da orquestração.
Não acessa o banco de dados.

Baseline arquitetural: Documento 1 — Especificação do Contrato.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse, quote

import aiohttp

import config
import os

_MGL_PARTNER = os.environ.get("MAGALU_PARTNER_ID", "3440")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "5479317")
_MGL_PID = os.environ.get("MAGALU_PID", "magazinevoce")
from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    Plataforma,
    TipoLink,
)
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url


# ── Identidade da plataforma ──────────────────────────────────────
_IDENTIFICADOR = "magalu"


# ── Domínios e encurtadores ───────────────────────────────────────
_DOMINIOS = frozenset({
    "magazineluiza.com.br", "m.magazineluiza.com.br",
    "sacola.magazineluiza.com.br", "magazinevoce.com.br",
    "maga.lu", "divulgador.magalu.com",
})
_ENCURTADORES = frozenset({
    "maga.lu", "divulgador.magalu.com",
})


# ── Quirk HTTP: hosts que exigem GET na resolução ─────────────────
# Hosts cujos servidores não respondem corretamente a requisições
# HEAD, exigindo GET direto no resolver de redirecionamento. Para
# Magalu, esta declaração materializa empiricamente a distinção
# entre identidade de encurtador e quirk HTTP: `maga.lu` requer
# GET, mas `divulgador.magalu.com` — também encurtador próprio da
# plataforma — responde corretamente a HEAD e portanto NÃO compõe
# este conjunto. A relação com _ENCURTADORES é factual, não
# definicional, e deve ser revisada por host individualmente.
_ENCURTADORES_FORCA_GET = frozenset({
    "maga.lu",
})

# ── Hosts de campanha ─────────────────────────────────────────────
_HOSTS_CAMPANHA = frozenset({
    "magazineluiza.com.br", "magazinevoce.com.br",
})

# ── Padrões de extração do identificador de produto ───────────────
# Rotas OFICIAIS de produto da Magalu. Cada padrão é ancorado numa
# rota explícita: a ancoragem é requisito de SEGURANÇA, não de
# estilo. O fragmento [a-z0-9]{5,} casaria segmentos de slug (ex.:
# "mochila"), de modo que qualquer extração posicional não ancorada
# produziria falsos positivos — e um falso positivo funde produtos
# distintos numa mesma entidade: falha silenciosa e destrutiva.
# Ambas as rotas partilham o MESMO namespace de identificador; a
# rota do divulgador é apenas outra gramática de URL para o mesmo
# produto. Para ampliar quando surgir um formato novo, acrescente o
# padrão aqui — é o ÚNICO lugar de decisão.
_P_PRODUTO = [
    re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{5,})(?:/|$|[?#])', re.I),
    re.compile(r'/divulgador/oferta/([a-z0-9]{5,})(?:/|$|[?#])', re.I),
]

# ── Parâmetros de rastreamento a remover na limpeza ───────────────
_PARAMS_REMOVER = frozenset({
    "partnerid", "promoterid", "afforcedeeplink", "deeplinkvalue",
    "partner_id", "promoter_id", "utm_source", "utm_medium",
    "utm_campaign", "pid", "c", "af_force_deeplink",
    "deep_link_value", "isretargeting",
})


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_id_produto(parsed) -> str:
    """
    Extrai o identificador de produto da Magalu a partir do caminho,
    ou cadeia vazia se nenhuma rota oficial de produto casar.

    Pura e determinística. A ordem de _P_PRODUTO é a ordem de
    precedência: a rota canônica /p/<id> é avaliada primeiro.
    """
    for padrao in _P_PRODUTO:
        m = padrao.search(parsed.path)
        if m:
            return m.group(1)
    return ""


def _eh_url_magalu(url: str) -> bool:
    """Verdadeiro se a URL pertence a um domínio da Magalu."""
    return _bate_dominio(_netloc(url), _DOMINIOS)


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence à Magalu (domínio próprio ou
    encurtador próprio). Pura, determinística, não falha.
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
    Extrai a identidade estruturada de uma URL da Magalu.

    Pura e determinística. Para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida.

    Classificação do tipo de link:
      - encurtador próprio    → ENCURTADO
      - identificador presente → PRODUTO
      - caminho de lista       → BUSCA
      - caminho de seleção     → CAMPANHA
      - demais casos           → CAMPANHA
    """
    netloc = _netloc(url)

    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)
    id_produto = _extrair_id_produto(parsed)
    if id_produto:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=id_produto,
            id_global=f"{_IDENTIFICADOR}:{id_produto}",
        )

    if "/l/" in parsed.path:
        tipo = TipoLink.BUSCA
    elif "/selecao/" in parsed.path:
        tipo = TipoLink.CAMPANHA
    else:
        tipo = TipoLink.CAMPANHA

    return IdentidadeProduto(tipo_link=tipo, id_produto=AUSENTE)


# ── Capacidade opcional: limpeza de URL ───────────────────────────
def limpa_url(url: str) -> str:
    """
    Remove os parâmetros de rastreamento e de atribuição da URL.
    Pura e determinística. Preserva a identidade canônica do
    produto.
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params_limpos = {
            k: v for k, v in params.items()
            if k.lower() not in _PARAMS_REMOVER
        }
        pares = [
            (k, valor)
            for k, valores in params_limpos.items()
            for valor in valores
        ]
        return urlunparse(parsed._replace(
            query=urlencode(pares), fragment="",
        ))
    except Exception:
        return url


# ── Capacidade obrigatória: afiliação ─────────────────────────────
def _construir_url_afiliada(url: str) -> str:
    """
    Aplica patch não-destrutivo dos parâmetros afiliados sobre a URL.
    Substitui valores dos parâmetros canônicos APENAS quando já
    presentes na URL. Não adiciona parâmetros ausentes. Não remove
    parâmetros não afiliados. Preserva todo o restante intacto.
    """
    canonicos = (
        ("partner_id",        _MGL_PARTNER),
        ("promoter_id",       _MGL_PROMOTER),
        ("utm_source",        "divulgador"),
        ("utm_medium",        "magalu"),
        ("utm_campaign",      _MGL_PROMOTER),
        ("pid",               _MGL_PID),
        ("c",                 _MGL_PROMOTER),
        ("af_force_deeplink", "true"),
    )

    for nome, valor in canonicos:
        padrao = re.compile(rf'(?<=[?&]){re.escape(nome)}=[^&#]*')
        novo = f"{nome}={quote(valor, safe='')}"
        url = padrao.sub(novo, url, count=1)

    return url


async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Magalu na sua forma afiliada longa.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e consulta o cache de links mediado. Não
    propaga exceções: qualquer falha legítima resulta em AUSENTE.

    Devolve a URL afiliada LONGA. O encurtamento é etapa posterior,
    conduzida pelo core a partir da declaração requer_encurtamento.

    Validação pós-expansão: se a expansão de um encurtador não
    resultar numa URL da Magalu, devolve AUSENTE.
    """
    url = _sanitizar_url(url)

    cache = consultar_link(url)
    if cache:
        return cache

    # Expansão de encurtador próprio, com validação pós-expansão.
    if _netloc(url) in _ENCURTADORES:
        try:
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
        except Exception as exc:
            log_nrm.warning(
                f"⚠️ MGL expansão falhou | erro={type(exc).__name__}"
            )
            return AUSENTE

        if not _eh_url_magalu(url_expandida):
            log_nrm.warning(
                "⚠️ MGL expansão não resultou em URL Magalu — descarta"
            )
            return AUSENTE
        url = url_expandida

    # Construção da URL afiliada longa.
    try:
        afiliada = _construir_url_afiliada(url)
    except Exception as exc:
        log_nrm.warning(
            f"⚠️ MGL afiliação falhou | erro={type(exc).__name__}"
        )
        return AUSENTE

    if "magazineluiza" not in _netloc(afiliada) \
            and "magazinevoce" not in _netloc(afiliada):
        log_nrm.warning(f"⚠️ MGL afiliação inválida: {afiliada[:60]}")
        return AUSENTE

    registrar_link(url, afiliada, _IDENTIFICADOR)
    log_nrm.info("✅ MGL afiliada (longa)")
    return afiliada


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
    hosts_campanha=_HOSTS_CAMPANHA,
    requer_encurtamento=True,
)
