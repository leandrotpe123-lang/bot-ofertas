"""
Plataforma Shopee — conhecimento de URLs.

Domínio único: tudo que a Shopee sabe sobre as PRÓPRIAS URLs —
reconhecimento de domínio, taxonomia de links (produto, campanha,
live, busca, perfil…), extração de identidade e canonização.

É a ÚNICA fonte do conhecimento de URLs da Shopee. Quando a Shopee
criar um formato de link novo, a mudança acontece AQUI e em nenhum
outro lugar do sistema. Puro e determinístico: não acessa rede,
cache nem o serviço de afiliados.

Fluxo de dependência: folha do pacote. Depende apenas do contrato e
do utilitário de URL do core; não importa `afiliacao` nem a
composição.
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

from plataformas.contrato import AUSENTE, IdentidadeProduto, TipoLink
from utils.urls import _netloc


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


# ── Padrões de extração do identificador de produto ───────────────
# Na Shopee, o produto é identificado pelo par (loja, item).
_P_PRODUTO = [
    re.compile(r'/product/(\d+)/(\d+)'),
    re.compile(r'/item/(\d+)/(\d+)'),
    re.compile(r'/opaanlp/(\d+)/(\d+)'),
    re.compile(r'[-/]i\.(\d+)\.(\d+)'),
]


# ── Página de campanha por CAMINHO (promo/institucional) ──────────
# Prefixos de caminho da Shopee que são páginas promocionais/
# institucionais (sem produto). Para ampliar quando surgir um formato
# novo, acrescente o prefixo aqui — é o ÚNICO lugar de decisão.
_PREFIXOS_CAMPANHA = ("/m/", "/oficial", "/shopeevip")


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


def _eh_pagina_campanha(parsed) -> bool:
    """
    Verdadeiro se a URL é página de campanha/promoção da Shopee (sem
    produto): domínios de premiação (premios/flapremios) OU caminhos
    /m/*, /oficial, /shopeevip. NÃO cobre live (identidade por
    `session`, tratada à parte) nem produto (precedência em
    extrai_identidade, verificada antes).
    """
    netloc = (parsed.netloc or "").lower()
    if netloc == "flapremios.com.br" or netloc.endswith("premios.shopee.com.br"):
        return True
    if netloc == "shopee.com.br" or netloc.endswith(".shopee.com.br"):
        caminho = (parsed.path or "").rstrip("/").lower()
        return any(caminho == pre.rstrip("/") or caminho.startswith(pre)
                   for pre in _PREFIXOS_CAMPANHA)
    return False


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
    Extrai a identidade estruturada de uma URL da Shopee. Pura e
    determinística. Para qualquer URL reconhecida, produz sempre uma
    IdentidadeProduto válida.

    Classificação por TIPO, em ordem de PRECEDÊNCIA:
      1. PRODUTO   — par (loja,item) em QUALQUER host/caminho (mesmo
                     numa página de campanha). Sinal mais forte.
      2. ENCURTADO — encurtador próprio; expandir antes de reclassificar.
      3. CAMPANHA  — promo/institucional: domínios premios/flapremios,
                     ou caminhos /m/*, /oficial, /shopeevip.
      4. BUSCA     — qualquer outro: perfil de loja, listagem, live, …
    """
    parsed = urlparse(url)
    netloc = (parsed.netloc or "").lower()

    # 1. PRODUTO tem precedência máxima (par loja,item em qualquer lugar).
    par = _extrair_par_produto(parsed)
    if par:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=par,
            id_global=f"{_IDENTIFICADOR}:{par}",
        )

    # 2. Encurtador: natureza final desconhecida até a expansão.
    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    # 3. Página de campanha/promoção (sem produto).
    if _eh_pagina_campanha(parsed):
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    # 4. Qualquer outro caso.
    return IdentidadeProduto(tipo_link=TipoLink.BUSCA, id_produto=AUSENTE)


# ── Campanha é derivada do classificador único (extrai_identidade) ─
def _eh_url_campanha(url: str) -> bool:
    """
    Consumido pelo core (normalização) p/ popular chaves_campanha.
    Deriva do classificador ÚNICO: é campanha sse extrai_identidade
    devolve TipoLink.CAMPANHA. Produto, encurtado, live e busca NÃO
    são campanha — não fundem famílias. (A precedência POR POST —
    produto presente suprime campanha — fica em normalizacao; aqui é
    só a classificação POR URL.)
    """
    return extrai_identidade(url).tipo_link == TipoLink.CAMPANHA


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
        # Live da Shopee: a transmissão é identificada por
        # session/share_user_id/from — não por shopid/itemid. Sem
        # esses, o link de live abre em erro.
        if (parsed.netloc or "").lower() == "live.shopee.com.br":
            manter = {"session", "share_user_id", "from"}
        else:
            manter = {"shopid", "itemid", "smtt"}
        params = {
            k: v[0] for k, v in parse_qs(parsed.query).items()
            if k in manter
        }
        return urlunparse(parsed._replace(
            query=urlencode(params) if params else "",
            fragment="",
        ))
    except Exception:
        return url


def _canonica_live(url: str) -> str:
    """
    Identidade canônica de uma live da Shopee. A live é identificada
    pelo `session` (a transmissão direta). O tipo de universal-link
    (aggregation/share) e os parâmetros de afiliação NÃO fazem parte
    da identidade. URLs de vitrine/aba (sem session) colapsam numa
    identidade única de vitrine — para não fragmentar entre si nem se
    fundir com uma live específica.
    """
    from urllib.parse import parse_qs
    try:
        parsed = urlparse(url)
        session = (parse_qs(parsed.query).get("session") or [""])[0].strip()
        if session:
            return f"https://live.shopee.com.br/live/{session}"
        return "https://live.shopee.com.br/aggregation"
    except Exception:
        return url


def _url_produto_canonica(url: str) -> Optional[str]:
    """
    Constrói a URL canônica de produto a partir de uma URL da Shopee,
    quando o par (loja, item) está presente. Usada como mecanismo de
    recuperação na afiliação.
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

