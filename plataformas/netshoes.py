"""
Plataforma — Netshoes (deeplink Rakuten / LinkSynergy).

Responsabilidade ÚNICA: representar a Netshoes perante o contrato.

A única transformação aplicada a uma URL é a troca do valor do parâmetro
`id` pelo ID de afiliado. Todo o resto — mid, murl, sellerId, UTMs,
encoding, ordem dos parâmetros e fragmento — permanece byte a byte.

Sem rede, sem banco, sem cache, sem normalização. O encurtamento é do
core, acionado por requer_encurtamento=True.
"""
from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit, urlunsplit

from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    Plataforma,
    TipoLink,
)
from utils.urls import _netloc


_IDENTIFICADOR = "netshoes"

# O único parâmetro que este módulo escreve.
_PARAM_AFILIADO = "id"
_ID_AFILIADO    = "ZYv0JCDsx8s"

# O host do deeplink é COMPARTILHADO entre anunciantes da Rakuten; o
# anunciante real é o destino (`murl`). Reconhecer o host pelado
# quebraria a exclusividade mútua que o contrato exige.
_DOMINIOS_DEEPLINK = frozenset({"linksynergy.com"})
_DOMINIOS_LOJA     = frozenset({"netshoes.com.br"})


def _bate_dominio(host: str, dominios: frozenset[str]) -> bool:
    return any(host == d or host.endswith("." + d) for d in dominios)


def _destino(url: str) -> str:
    """Valor de `murl` — o destino do deeplink. Leitura apenas."""
    for chave, valor in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if chave == "murl":
            return valor
    return ""


# ── Contrato: reconhecimento ──────────────────────────────────────
def reconhece(url: str) -> bool:
    """Deeplink LinkSynergy cujo destino é a Netshoes. Puro, sem I/O.

    Reconhece também a saída de afilia(): normalizacao._encurtar_mapa
    resolve a plataforma pela URL JÁ AFILIADA para acionar o encurtador.
    """
    if not url or not _bate_dominio(_netloc(url), _DOMINIOS_DEEPLINK):
        return False
    return _bate_dominio(_netloc(_destino(url)), _DOMINIOS_LOJA)


# ── Contrato: identidade ──────────────────────────────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """Identidade do destino (`murl`). Pura.

    Precondição garantida pelo core: só é chamada sobre URL já
    reconhecida (registry.resolver → normalizacao:252-253).

    O caminho do produto é o identificador — nenhum padrão de SKU é
    presumido. A query (sellerId, UTMs) não participa da identidade.
    """
    caminho = urlsplit(_destino(url)).path.rstrip("/")
    if caminho.startswith("/p/"):
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=caminho,
            id_global=f"{_IDENTIFICADOR}:{caminho}",
        )
    return IdentidadeProduto(tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE)


# ── A única transformação ─────────────────────────────────────────
def _trocar_id(url: str) -> str:
    """`id=<qualquer>` → `id=ZYv0JCDsx8s`. Nada mais muda.

    A query é fatiada no `&` e a chave comparada por igualdade EXATA
    (`mid` não é `id`). Pares que não são `id` são reemitidos como a
    string original — nunca decodificados, logo nunca corrompidos.
    Sem `id` na query, a URL volta intacta.
    """
    partes = urlsplit(url)

    pares = []
    for par in partes.query.split("&"):
        chave, sep, _valor = par.partition("=")
        if sep and chave == _PARAM_AFILIADO:
            pares.append(f"{_PARAM_AFILIADO}={_ID_AFILIADO}")
        else:
            pares.append(par)

    nova_query = "&".join(pares)
    if nova_query == partes.query:
        return url
    return urlunsplit(partes._replace(query=nova_query))


# ── Contrato: afiliação ───────────────────────────────────────────
async def afilia(url: str, sessao=None) -> object:
    """URL afiliada LONGA: o mesmo deeplink com o `id` trocado.

    Não propaga exceção — falha resulta em AUSENTE.
    """
    try:
        if not reconhece(url):
            return AUSENTE
        return _trocar_id(url)
    except Exception as exc:
        log_nrm.warning(f"⚠️ NSH afiliação falhou | erro={type(exc).__name__}")
        return AUSENTE


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    requer_encurtamento=True,
)
