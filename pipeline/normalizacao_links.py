"""
Camada 3 — Normalização / Ciclo de vida das URLs.

Responsabilidade ÚNICA: todo o ciclo de vida das URLs de uma
mensagem — particionar, expandir encurtador genérico, consultar
cache, resolver plataforma, afiliar e aplicar o encurtamento
terminal.

NÃO conhece identidade de oferta, cupom nem forma de texto. Não
importa pipeline.normalizacao, normalizacao_identidade nem
normalizacao_texto.

AVISO DE ORDEM — CROSS-MODULE:
  _encurtar_mapa mora neste módulo, mas a ordem que a governa é do
  ORQUESTRADOR. Ela é a ÚLTIMA transformação de URL do pipeline e
  roda SOMENTE depois da derivação de identidade, que acontece em
  pipeline.normalizacao_identidade. NENHUMA função deste módulo
  pode chamar _encurtar_mapa — em particular resolver_e_afiliar.
  Fazê-lo destrói silenciosamente a identidade da oferta. O
  invariante completo está no banner logo acima da função.

Extraído de pipeline.normalizacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, NamedTuple, Optional, Tuple

import aiohttp

from globals import _get_session
from logger import log_nrm
from utils.categorias_universais import classificar_universal, eh_encurtador_generico
from plataformas import registry
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link
from utils.encurtador import encurtar
from utils.url_resolver import desencurtar
from utils.urls import _netloc

__all__ = [
    "LinksParticionados",
    "LinksResolvidos",
    "particionar_links",
    "resolver_e_afiliar",
    "_encurtar_mapa",
]


# ─────────────────────────────────────────────────────────────────
# CONTRATOS DE SAÍDA DESTE MÓDULO
#
# NamedTuple e não dataclass: contrato tipado explícito que também é
# desempacotável, para que o orquestrador continue usando os mesmos
# nomes locais de hoje.
# ─────────────────────────────────────────────────────────────────
class LinksParticionados(NamedTuple):
    converter: List[str]
    preservar: List[str]


class LinksResolvidos(NamedTuple):
    mapa:      Dict[str, str]
    canonicas: Dict[str, str]
    plats:     List[str]


def particionar_links(links: List[str]) -> LinksParticionados:
    """
    Separa os links da mensagem em 'converter' e 'preservar',
    deduplicando por ordem de aparição. Único ponto de decisão sobre
    qual link entra em qual trilha.
    """
    converter:     List[str] = []
    preservar_lst: List[str] = []
    vistos: set = set()
    for url in links:
        if url in vistos:
            continue
        vistos.add(url)
        if classificar_universal(url) == "preservar":
            preservar_lst.append(url)
        else:
            converter.append(url)

    return LinksParticionados(converter, preservar_lst)


# ─────────────────────────────────────────────────────────────────
# RESOLUÇÃO E AFILIAÇÃO DE UM LINK
# ─────────────────────────────────────────────────────────────────
async def _normalizar_um(
    url_original: str,
    sessao: aiohttp.ClientSession,
    msg_id: int = 0,
) -> Tuple[str, Optional[str], str]:
    """
    Resolve e afilia um único link. Devolve a tupla
    (url_original, conv, plataforma), onde conv é um Afiliacao
    (publicada + canonica), uma str (quando as duas formas
    coincidem) ou None. O encurtamento NÃO ocorre aqui.

    conv é None quando o link não pôde ser afiliado ou não é
    aproveitável.
    """
    # 1. Categorias universais do core, decididas antes do registry.
    categoria = classificar_universal(url_original)
    if categoria == "bloqueado":
        return url_original, None, "none"
    if categoria in ("mundial", "preservar"):
        return url_original, url_original, categoria

    # 2. Expansão de encurtador genérico — responsabilidade universal.
    url = url_original
    if eh_encurtador_generico(url_original):
        try:
            expandida = await desencurtar(url_original, sessao)
            if expandida and expandida != url_original:
                url = expandida
                log_nrm.info(
                    f"🔗 expandido | {_netloc(url_original)} → {_netloc(url)}"
            )
                categoria = classificar_universal(url)
                if categoria == "bloqueado":
                    return url_original, None, "none"
                if categoria in ("mundial", "preservar"):
                    return url_original, url, categoria
        except Exception as e:
            log_nrm.warning(
                f"⚠️ expansão de encurtador genérico falhou: {e}"
            )
            return url_original, None, "none"

    # 3. Cache de link já afiliado. O cache armazena exclusivamente
    #    a URL afiliada LONGA; o encurtamento é terminal e não cacheado.
    cached = consultar_link(url_original)
    if not cached and url != url_original:
        cached = consultar_link(url)
    if cached:
        plataforma_cache = registry.resolver(url)
        ident = (
            plataforma_cache.identificador
            if plataforma_cache is not None else "none"
        )
        return url_original, cached, ident

    # 4. Resolução de plataforma via registry (fonte soberana).
    plataforma = registry.resolver(url)
    if plataforma is None:
        return url_original, None, "none"

    # 5. Afiliação pela capacidade do contrato. Devolve a URL
    #    afiliada LONGA.
    afiliada = await plataforma.afilia(url, sessao)
    if afiliada is AUSENTE:
        return url_original, None, plataforma.identificador

    return url_original, afiliada, plataforma.identificador


# ─────────────────────────────────────────────────────────────────
# ENCURTAMENTO TERMINAL DO MAPA
#
# INVARIANTE DE ORDEM — LEIA ANTES DE ALTERAR A FUNÇÃO normalizar:
#   O encurtamento é a ÚLTIMA transformação aplicada às URLs, e
#   ocorre SOMENTE depois que toda a identidade semântica
#   (ids_globais, sku, chave_campanha, tem_host_campanha, estado de
#   evento) já foi derivada e consolidada a partir das URLs
#   afiliadas LONGAS.
#
#   Nenhuma lógica nova pode ser inserida entre a derivação de
#   identidade e esta transformação sem revisão arquitetural
#   explícita. A URL curta é forma terminal de publicação e jamais
#   participa de identidade, deduplicação, persistência ou cache.
# ─────────────────────────────────────────────────────────────────
def _encurtar_mapa(mapa: Dict[str, str]) -> Tuple[Dict[str, str], int]:
    """
    Produz o mapa de publicação a partir do mapa de URLs afiliadas
    longas. Para cada URL convertida, resolve a plataforma e, quando
    esta declara requer_encurtamento, aplica o encurtador terminal.

    Devolve o novo mapa de publicação e a contagem de URLs
    encurtadas, esta última para observabilidade.
    """
    mapa_publicacao: Dict[str, str] = {}
    n_encurtadas = 0
    for original, afiliada_longa in mapa.items():
        plataforma = registry.resolver(afiliada_longa)
        if plataforma is not None and plataforma.requer_encurtamento:
            url_publicacao = encurtar(afiliada_longa)
            if url_publicacao != afiliada_longa:
                n_encurtadas += 1
            mapa_publicacao[original] = url_publicacao
        else:
            mapa_publicacao[original] = afiliada_longa
    return mapa_publicacao, n_encurtadas


async def resolver_e_afiliar(
    converter: List[str],
    msg_id: int = 0,
) -> LinksResolvidos:
    """
    Resolve e afilia em paralelo os links da trilha 'converter',
    limitados a 50, e coleta o resultado.

    Devolve o mapa de URLs afiliadas LONGAS (base de publicação
    antes do encurtamento), o mapa de canônicas (base semântica da
    identidade) e a lista de plataformas observadas.

    plat_dom NÃO é calculado aqui: é derivado pelo orquestrador
    depois do guard, exatamente onde sempre esteve.
    """
    sessao = await _get_session()
    resultados = await asyncio.gather(
        *[_normalizar_um(url, sessao, msg_id) for url in converter[:50]],
        return_exceptions=True,
    )

    # mapa: URLs afiliadas LONGAS. Base semântica até o encurtamento.
    mapa:      Dict[str, str] = {}
    canonicas: Dict[str, str] = {}
    plats:     List[str]      = []
    for res in resultados:
        if isinstance(res, Exception):
            log_nrm.error(f"❌ normalizar link: {res}")
            continue
        orig, conv, plat = res
        if conv and plat not in ("none", None):
            # conv é Afiliacao (publicada ≠ canonica) ou str (formas
            # iguais). A publicada vai ao mapa de publicação; a
            # canonica alimenta a derivação de identidade.
            if isinstance(conv, Afiliacao):
                publicada, canonica = conv.publicada, conv.canonica
            else:
                publicada = canonica = conv
            mapa[orig]      = publicada
            canonicas[orig] = canonica
            if plat not in ("mundial", "preservar"):
                plats.append(plat)

    return LinksResolvidos(mapa, canonicas, plats)
  
