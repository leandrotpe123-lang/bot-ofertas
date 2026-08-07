"""
Camada 3 — Normalização.

Responsabilidade única: transformar uma MensagemBruta em uma
MensagemNormalizada, orquestrando a resolução e a afiliação dos
links contidos na mensagem, e produzindo os metadados derivados
necessários às camadas seguintes.

AUTORIDADE DE DERIVAÇÃO DE IDENTIDADE:
  A normalização é a autoridade única de derivação de identidade
  semântica. Toda identidade — produto e campanha — é derivada das
  URLs afiliadas LONGAS, antes do encurtamento terminal, e
  transportada adiante como dado consolidado. As camadas seguintes
  consomem identidade já derivada; NENHUMA delas reextrai identidade
  a partir do mapa.

SEPARAÇÃO CONCEITUAL — FRONTEIRA RÍGIDA:
  - ids_globais, sku, chave_campanha, tem_host_campanha :
        identidade canônica, derivada das URLs afiliadas LONGAS.
  - mapa :
        tabela de tradução de URL para publicação. Após o bloco
        terminal de encurtamento, NÃO é fonte de identidade
        semântica. Exceção explícita: o nível 6 de identidade_canonica
        usa o mapa como fallback operacional NÃO semântico.
  - URL afiliada longa : base semântica.
  - URL curta          : forma terminal de publicação; nunca
                         participa de identidade ou deduplicação.

CONTRATO COM A DEDUPLICAÇÃO:
  Os campos chave_campanha e tem_host_campanha são parte do contrato
  consumido pela deduplicação. A sua derivação é responsabilidade
  exclusiva desta camada. A deduplicação os consome e não reextrai
  identidade de campanha a partir do mapa.

Operações que constituem a normalização:
  - limpeza estrutural do texto (limpar_texto)
  - resolução e afiliação de links (via registry e contrato)
  - derivação de identidade (produto e campanha)
  - encurtamento terminal das URLs de publicação

NÃO faz:
  - filtragem de conteúdo (responsabilidade de pipeline.filtros)
  - detecção/validação de cupom (responsabilidade de utils.cupom)
  - resolução de URL via rede (responsabilidade de utils.url_resolver)
  - registro de posts pendentes (responsabilidade do orquestrador)
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp

from database import db_get_link
from globals import _get_session, _get_final, _log_cache_stats
from logger import log_nrm
from utils.categorias_universais import classificar_universal, eh_encurtador_generico
from pipeline.ingestao import MensagemBruta
from pipeline.normalizacao_identidade import (
    derivar_campanha,
    derivar_produto,
    remover_cupons_da_entidade,
)
from pipeline.normalizacao_texto import _tem_emoji, limpar_texto
from plataformas import registry
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link
from utils.cupom import extrair_todos_cupons
from utils.encurtador import encurtar
from utils.url_resolver import desencurtar
from utils.urls import _netloc

# ── API pública do módulo ─────────────────────────────────────────
__all__ = [
    "MensagemNormalizada", "normalizar", "limpar_texto",
    "_tem_emoji",
]

# ─────────────────────────────────────────────────────────────────
# CONTRATO DE SAÍDA
# ─────────────────────────────────────────────────────────────────
@dataclass
class MensagemNormalizada:
    msg_id:            int
    chat:              str
    texto_limpo:       str
    mapa:              Dict[str, str]
    preservar:         List[str]
    plat:              str
    sku:               str
    tem_midia:         bool
    media_obj:         object
    ids_globais:       List[str]    = field(default_factory=list)
    # Vínculo POR LINK que a plataforma entrega via contrato e que a
    # travessia PRESERVA: (plataforma_do_link, id_produto, tipo_link).
    # ids_globais é projeção de idents — mesma passada de derivação.
    idents: List[Tuple[str, str, str]] = field(default_factory=list)
    # Lista completa de cupons — snapshot derivado UMA vez na normalização
    # (extrair_todos_cupons sobre texto_limpo). Consumida por identidade e
    # score. É o ÚNICO fato de cupom do contrato (Cupom-1).
    cupons:            List[str]    = field(default_factory=list)
    # ── Identidade de campanha derivada — CONTRATO COM A DEDUPLICAÇÃO.
    # Derivados das URLs afiliadas LONGAS, antes do encurtamento.
    # Derivação é responsabilidade EXCLUSIVA da normalização; a
    # deduplicação os consome e não reextrai identidade do mapa.
    chave_campanha:    str          = ""
    chaves_campanha:   List[str]    = field(default_factory=list)
    tem_host_campanha: bool         = False
    tem_sinal_cashback: bool        = False
    # Entidades de código (monospace) capturadas na ingestão — insumo que
    # normalizar() usa para derivar norm.cupons. Este campo é insumo
    # interno e sai do contrato no Cupom-2.
    code_entities:     List[str]    = field(default_factory=list)
    # ──────────────────────────────────────────────────────────────
    is_reply:          bool         = False
    reply_to:          int          = 0
    is_override:       bool         = False


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


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT — NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────
async def normalizar(
    bruta: MensagemBruta,
    is_override: bool = False,
) -> Optional[MensagemNormalizada]:
    """
    Transforma uma MensagemBruta em uma MensagemNormalizada.

    ORDEM DAS OPERAÇÕES (invariante de ordem):
      1. limpeza e viabilidade do texto
      2. resolução e afiliação dos links (URLs afiliadas LONGAS)
      3. derivação de identidade — produto E campanha — sobre as
         URLs afiliadas LONGAS
      4. determinação do estado de evento
      5. encurtamento terminal do mapa — ÚLTIMA transformação
      6. construção da MensagemNormalizada
    """
    if not bruta.texto.strip():
        return None

    from pipeline.institucional import deve_descartar, tem_contexto
    veto = deve_descartar(bruta.texto)
    if veto:
        log_nrm.info(f"🚫 Post vetado | motivo='{veto}' | @{bruta.chat}")
        return None

    if not tem_contexto(bruta.texto):
        return None

    from pipeline.filtros import filtrar
    texto_limpo = filtrar(limpar_texto(bruta.texto))

    converter:     List[str] = []
    preservar_lst: List[str] = []
    vistos: set = set()
    for url in bruta.links:
        if url in vistos:
            continue
        vistos.add(url)
        if classificar_universal(url) == "preservar":
            preservar_lst.append(url)
        else:
            converter.append(url)
    if not converter and not preservar_lst:
        return None

    sessao = await _get_session()
    resultados = await asyncio.gather(
        *[_normalizar_um(url, sessao, bruta.msg_id) for url in converter[:50]],
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

    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}")
        return None

    plat_dom = max(set(plats), key=plats.count) if plats else ""
    cupons   = extrair_todos_cupons(texto_limpo, getattr(bruta, "code_entities", None))

    # ── DERIVAÇÃO DE IDENTIDADE ───────────────────────────────────
    # Opera EXCLUSIVAMENTE sobre as URLs afiliadas LONGAS (conv),
    # antes de qualquer encurtamento. A URL original (orig) NÃO é
    # fonte de identidade: a fonte canônica é a URL afiliada longa.
    # A normalização é a autoridade única de derivação — produto
    # (ids_globais, sku) e campanha (chave_campanha, tem_host_campanha).
    urls_longas = list(canonicas.values())

    ids_globais, idents, sku = derivar_produto(urls_longas)

    cupons = remover_cupons_da_entidade(cupons, ids_globais)

    (tem_host_campanha, chave_campanha,
     chaves_campanha, tem_sinal_cashback) = derivar_campanha(
        urls_longas, texto_limpo)

    # ── ENCURTAMENTO TERMINAL ─────────────────────────────────────
    # ÚLTIMA transformação antes do retorno. Toda a identidade —
    # produto e campanha — já foi derivada acima sobre as URLs
    # longas. A partir daqui o mapa passa a conter a forma de
    # publicação. Nenhuma lógica pode ser inserida entre a derivação
    # de identidade e este ponto sem revisão arquitetural.
    mapa_publicacao, n_encurtadas = _encurtar_mapa(mapa)

    # Política de bloco: só permanece o bloco que gerou oferta
    # publicável. Depende do mapa de publicação, por isso roda
    # aqui e não junto da filtragem de linha.
    from pipeline.filtros import filtrar_blocos
    texto_limpo = filtrar_blocos(
        texto_limpo, mapa_publicacao, preservar_lst)

    log_nrm.info(
    f"✅ {len(mapa_publicacao)}/{len(converter)} | "
    f"plat={plat_dom or 'none'} cupons={cupons} sku={sku} "
    f"ids_globais={ids_globais} "
    f"chave_campanha='{chave_campanha}' "
    f"chaves_campanha={chaves_campanha} "
    f"encurtadas={n_encurtadas} " 
    f"override={is_override}"
  )
    _log_cache_stats()

    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat, texto_limpo=texto_limpo,
        mapa=mapa_publicacao, preservar=preservar_lst, plat=plat_dom,
        cupons=cupons, sku=sku, tem_midia=bruta.tem_midia,
        media_obj=bruta.media_obj,
        ids_globais=ids_globais, idents=idents,
        chave_campanha=chave_campanha,
        chaves_campanha=chaves_campanha,
        tem_host_campanha=tem_host_campanha,
        tem_sinal_cashback=tem_sinal_cashback,
        code_entities=bruta.code_entities,
        is_reply=bruta.is_reply,
        reply_to=bruta.reply_to, is_override=is_override,
  )
