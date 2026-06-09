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
  - verificação de viabilidade do texto (tem_contexto)
  - resolução e afiliação de links (via registry e contrato)
  - derivação de identidade (produto e campanha)
  - encurtamento terminal das URLs de publicação

NÃO faz:
  - filtragem de conteúdo (responsabilidade de pipeline.filtros)
  - detecção/validação de cupom (responsabilidade de utils.cupom)
  - classificação de estado de evento (responsabilidade de
    pipeline.estado_evento)
  - resolução de URL via rede (responsabilidade de utils.url_resolver)
  - registro de posts pendentes (responsabilidade do orquestrador)

REEXPORTAÇÕES TEMPORÁRIAS DE COMPATIBILIDADE:
  Os símbolos _KW_CUPOM, _FALSO_CUPOM, extrair_todos_cupons,
  EstadoEvento e _JANELA_C3 são reexportados a partir de suas novas
  origens para não quebrar montagem e deduplicação antes da revisão
  desses módulos. Estas reexportações devem ser removidas quando
  montagem e deduplicação tiverem seus imports corrigidos.
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp

from database import db_get_dedupe, db_get_link
from globals import _get_session, _get_final, _log_cache_stats
from logger import log_nrm
from utils.categorias_universais import classificar_universal
from pipeline.estado_evento import (
    EstadoEvento,
    detectar_estado_evento,
    _JANELA_C3,
)
from pipeline.ingestao import MensagemBruta
from plataformas import registry
from plataformas.contrato import AUSENTE
from utils.cupom import _FALSO_CUPOM, _KW_CUPOM, extrair_cupom, extrair_todos_cupons
from utils.encurtador import encurtar
from utils.hashes import _fp_c3
from utils.url_resolver import desencurtar
from utils.urls import _netloc, host_canonico_campanha

# ── Reexportações temporárias de compatibilidade ──────────────────
__all__ = [
    "MensagemNormalizada", "normalizar", "limpar_texto", "tem_contexto",
    "EstadoEvento", "detectar_estado_evento", "_JANELA_C3",
    "_KW_CUPOM", "_FALSO_CUPOM", "extrair_cupom", "extrair_todos_cupons",
    "_tem_emoji", "desencurtar",
]


# ─────────────────────────────────────────────────────────────────
# LIMPEZA DE TEXTO
# ─────────────────────────────────────────────────────────────────
_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_GRUPO_EXT  = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I,
)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+)\s*$',
    re.I,
)
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|'
    r'clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|'
    r'entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$',
    re.I,
)
_RE_REDES = re.compile(
    r'^\s*(?:redes\s+\w+|[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|'
    r'acesse\s+nossas\s+redes)',
    re.I,
)
_RE_ROTULO    = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')
_RE_EMOJI_CHK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]"
)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHK.search(s))


def _eh_header_canal(linha: str) -> bool:
    l = linha.strip()
    if not l or _tem_emoji(l[0]):
        return False
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}', l):
        return True
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$', l, re.UNICODE):
        return True
    return False


def limpar_texto(texto: str) -> str:
    """
    Remove ruído estrutural do texto: caracteres invisíveis, headers
    de canal, blocos de redes sociais, chamadas para ação vazias e
    links para grupos externos. Preserva o conteúdo promocional.
    """
    texto = (
        _RE_INVISIVEIS.sub(" ", texto)
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    linhas = texto.split("\n")
    saida: List[str] = []
    vazio = False
    em_redes = False
    primeira = True
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio:
                saida.append("")
            vazio = True
            em_redes = False
            continue
        vazio = False
        if primeira:
            primeira = False
            if _eh_header_canal(l):
                continue
        if _RE_REDES.match(l):
            em_redes = True
            continue
        if em_redes:
            if _RE_ROTULO.match(l) or not l:
                continue
            if not re.match(r'https?://', l):
                em_redes = False
            else:
                continue
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l):
            continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                continue
        saida.append(l)
    return "\n".join(saida).strip()


# ─────────────────────────────────────────────────────────────────
# VIABILIDADE DO TEXTO
# ─────────────────────────────────────────────────────────────────
def tem_contexto(texto: str) -> bool:
    """
    Verifica se o texto possui conteúdo promocional relevante o
    suficiente para prosseguir.
    """
    linhas = [
        l.strip() for l in texto.splitlines()
        if l.strip() and not re.match(r'https?://', l.strip())
    ]
    if not linhas:
        return False
    total = " ".join(linhas)
    indicadores = [
        r'off', r'%', r'r\$', r'cupom', r'desconto', r'promoção', r'oferta',
        r'grátis', r'evento', r'live', r'relâmpago', r'flash', r'volta',
        r'normalizou', r'a\s+partir', r'ativo', r'disponivel', r'pix',
        r'voltando', r'reativado', r'jogos?\s+gr[aá]tis',
    ]
    for ind in indicadores:
        if re.search(ind, total, re.I):
            return True
    return len(total) > 20


# ─────────────────────────────────────────────────────────────────
# ENCURTADORES GENÉRICOS
# ─────────────────────────────────────────────────────────────────
_ENCURTADORES_GENERICOS = frozenset({
    "bit.ly", "tinyurl.com", "cutt.ly", "t.co", "ow.ly", "goo.gl",
    "rb.gy", "is.gd", "tiny.cc", "buff.ly", "short.io", "bl.ink",
    "rebrand.ly", "shorturl.at", "tidd.ly",
})


def _eh_encurtador_generico(url: str) -> bool:
    """Verdadeiro se a URL é um encurtador genérico, não de plataforma."""
    return _netloc(url) in _ENCURTADORES_GENERICOS


# ─────────────────────────────────────────────────────────────────
# HOSTS DE CAMPANHA — COMPOSIÇÃO VIA REGISTRY
#
# O conhecimento de quais hosts caracterizam uma página de campanha
# pertence a cada plataforma e é declarado na capacidade
# hosts_campanha do contrato. Este módulo NÃO mantém lista concreta
# de hosts de marketplace: compõe a UNIÃO das contribuições das
# plataformas registradas, do mesmo modo que url_resolver compõe
# encurtadores_forca_get.
#
# DIVERGÊNCIA DELIBERADA EM RELAÇÃO A _compor_forca_get:
#   A composição de força-GET parte de um conjunto local de
#   encurtadores GENÉRICOS (universais, sem dono). Hosts de campanha
#   NÃO têm componente universal: todo host de campanha legítimo
#   pertence a uma plataforma. Semear um conjunto local de hosts de
#   campanha reintroduziria exatamente o acoplamento que esta
#   migração elimina. Por isso a semente é VAZIA — a união provém
#   integralmente das plataformas.
# ─────────────────────────────────────────────────────────────────
_HOSTS_CAMPANHA_COMPOSTO: Optional[frozenset] = None


def _compor_hosts_campanha() -> frozenset:
    """
    Compõe a união dos hosts de campanha declarados pelas plataformas
    registradas, lendo a capacidade hosts_campanha de cada uma via
    registry. Defensiva por fonte: falha ao ler uma plataforma não
    bloqueia as demais. NÃO faz cache; o cache é de _hosts_campanha.
    Sem semente local — hosts de campanha não têm componente universal.
    """
    try:
        # Composição com proveniência vive no registry (dono da camada
        # coletiva). Visão plana = chaves do mapa elemento -> donos. A
        # origem fica retida e consultável em registry.compor_capacidade,
        # tornando avaliável a disjunção deste espaço com dono.
        return frozenset(registry.compor_capacidade("hosts_campanha").keys())
    except Exception as e:
        log_nrm.warning(
            f"⚠ _compor_hosts_campanha: falha compondo via registry: {e}"
        )
        return frozenset()


def _logar_decomposicao_hosts_campanha() -> None:
    """
    Log observacional por plataforma da composição. Estritamente
    observacional: não altera composição nem cache, não propaga
    exceção. Espelha _logar_decomposicao_inicial de url_resolver e
    torna VISÍVEL, no log, exatamente quais hosts cada plataforma
    contribui — base empírica para verificar a preservação.
    """
    try:
        for ident in registry.plataformas_registradas():
            try:
                plataforma = registry.acessar(ident)
                if plataforma is None:
                    continue
                contrib = plataforma.hosts_campanha
                if contrib is None:
                    log_nrm.info(
                        f"⚙ _hosts_campanha: fonte=plataforma "
                        f"id={ident!r} contribuicao=nao_declarada"
                    )
                else:
                    log_nrm.info(
                        f"⚙ _hosts_campanha: fonte=plataforma "
                        f"id={ident!r} hosts={sorted(contrib)}"
                    )
            except Exception as e:
                log_nrm.warning(
                    f"⚠ _hosts_campanha: falha lendo plataforma "
                    f"{ident!r} p/ decomposição: {e}"
                )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _hosts_campanha: falha iterando registry "
            f"p/ decomposição: {e}"
        )


def _hosts_campanha() -> frozenset:
    """
    Devolve o conjunto efetivo de hosts de campanha, compondo-o lazy
    na primeira chamada e cacheando em variável de módulo. Na
    composição inicial, emite log observacional por fonte. Espelha
    _hosts_forca_get.
    """
    global _HOSTS_CAMPANHA_COMPOSTO
    if _HOSTS_CAMPANHA_COMPOSTO is None:
        _HOSTS_CAMPANHA_COMPOSTO = _compor_hosts_campanha()
        log_nrm.info(
            f"⚙ _hosts_campanha: composição inicial — "
            f"{len(_HOSTS_CAMPANHA_COMPOSTO)} hosts"
        )
        _logar_decomposicao_hosts_campanha()
    return _HOSTS_CAMPANHA_COMPOSTO


def _resetar_hosts_campanha() -> None:
    """
    Invalida o cache da composição. Costura única de invalidação,
    preparada para registro tardio de plataforma e ciclos de teste
    isolados. Espelha _resetar_forca_get.
    """
    global _HOSTS_CAMPANHA_COMPOSTO
    _HOSTS_CAMPANHA_COMPOSTO = None


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
    cupom:             str
    sku:               str
    tem_midia:         bool
    media_obj:         object
    estado_evento:     EstadoEvento = EstadoEvento.NEW
    ids_globais:       List[str]    = field(default_factory=list)
    # ── Identidade de campanha derivada — CONTRATO COM A DEDUPLICAÇÃO.
    # Derivados das URLs afiliadas LONGAS, antes do encurtamento.
    # Derivação é responsabilidade EXCLUSIVA da normalização; a
    # deduplicação os consome e não reextrai identidade do mapa.
    chave_campanha:    str          = ""
    tem_host_campanha: bool         = False
    # ──────────────────────────────────────────────────────────────
    is_reply:          bool         = False
    reply_to:          int          = 0
    is_override:       bool         = False


# ─────────────────────────────────────────────────────────────────
# DERIVAÇÃO DE IDENTIDADE
#
# INVARIANTE FORMAL DO PIPELINE:
#   A derivação de identidade opera EXCLUSIVAMENTE sobre a URL
#   afiliada LONGA (conv), nunca sobre a URL original do texto
#   (orig) nem sobre URL encurtada. A URL afiliada longa é a única
#   fonte canônica de identidade.
# ─────────────────────────────────────────────────────────────────
def _identificador_de(url: str) -> str:
    """
    Devolve o identificador puro do produto de uma URL afiliada
    longa, ou string vazia quando a URL não pertence a uma
    plataforma ou não corresponde a um produto individual.
    """
    plataforma = registry.resolver(url)
    if plataforma is None:
        return ""
    ident = plataforma.extrai_identidade(url)
    if ident.id_produto is AUSENTE:
        return ""
    return str(ident.id_produto)


def _eh_host_de_campanha(url: str) -> bool:
    """
    Verdadeiro se o host de uma única URL pertence à união de hosts
    de campanha composta a partir das plataformas registradas.
    Predicado unitário, sobre a URL afiliada LONGA, usando _netloc.
    A semântica de casamento (igualdade ou sufixo) é idêntica à do
    conjunto hardcoded anterior, preservando o comportamento.
    """
    host = _netloc(url)
    hosts = _hosts_campanha()
    for h in hosts:
        if host == h or host.endswith("." + h):
            return True
    return False


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
    (url_original, url_convertida, plataforma). A url_convertida é
    a URL afiliada LONGA — o encurtamento NÃO ocorre aqui.

    A url_convertida é None quando o link não pôde ser afiliado ou
    não é aproveitável.
    """
    # 1. Categorias universais do core, decididas antes do registry.
    categoria = classificar_universal(url_original)
    if categoria == "bloqueado":
        return url_original, None, "none"
    if categoria in ("mundial", "preservar"):
        return url_original, url_original, categoria

    # 2. Expansão de encurtador genérico — responsabilidade universal.
    url = url_original
    if _eh_encurtador_generico(url_original):
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
    cached = _get_final(url_original) or db_get_link(url_original)
    if not cached and url != url_original:
        cached = _get_final(url) or db_get_link(url)
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

    texto_limpo = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo):
        return None

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
    mapa:  Dict[str, str] = {}
    plats: List[str]      = []
    for res in resultados:
        if isinstance(res, Exception):
            log_nrm.error(f"❌ normalizar link: {res}")
            continue
        orig, conv, plat = res
        if conv and plat not in ("none", None):
            mapa[orig] = conv
            if plat not in ("mundial", "preservar"):
                plats.append(plat)

    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}")
        return None

    plat_dom = max(set(plats), key=plats.count) if plats else ""
    cupom    = extrair_cupom(texto_limpo, getattr(bruta, "code_entities", None))

    # ── DERIVAÇÃO DE IDENTIDADE ───────────────────────────────────
    # Opera EXCLUSIVAMENTE sobre as URLs afiliadas LONGAS (conv),
    # antes de qualquer encurtamento. A URL original (orig) NÃO é
    # fonte de identidade: a fonte canônica é a URL afiliada longa.
    # A normalização é a autoridade única de derivação — produto
    # (ids_globais, sku) e campanha (chave_campanha, tem_host_campanha).
    urls_longas = list(mapa.values())

    ids_globais: List[str] = []
    for conv in urls_longas:
        ident = _identificador_de(conv)
        if ident and ident not in ids_globais:
            ids_globais.append(ident)

    sku = ids_globais[0] if ids_globais else ""

    # Identidade de campanha: chave_campanha e tem_host_campanha
    # DEVEM derivar da mesma população de URLs — as URLs de campanha
    # — para que sejam coerentes entre si. A chave_campanha jamais
    # pode ser derivada de uma URL de produto ou de landing.
    urls_campanha     = [u for u in urls_longas if _eh_host_de_campanha(u)]
    tem_host_campanha = bool(urls_campanha)
    chave_campanha    = host_canonico_campanha(urls_campanha)

    # ── ESTADO DE EVENTO ──────────────────────────────────────────
    estado = EstadoEvento.NEW
    if ids_globais:
        estado = detectar_estado_evento(texto_limpo, ids_globais[0], plat_dom)
    elif cupom:
        fp_cup  = _fp_c3(f"cup_{cupom}", plat_dom)
        entrada = db_get_dedupe(fp_cup)
        if entrada:
            delta  = time.time() - entrada.get("ts", 0)
            janela = _JANELA_C3.get(plat_dom, 120.0)
            estado = EstadoEvento.SEEN if delta < janela else EstadoEvento.EXPIRED

    # ── ENCURTAMENTO TERMINAL ─────────────────────────────────────
    # ÚLTIMA transformação antes do retorno. Toda a identidade —
    # produto e campanha — já foi derivada acima sobre as URLs
    # longas. A partir daqui o mapa passa a conter a forma de
    # publicação. Nenhuma lógica pode ser inserida entre a derivação
    # de identidade e este ponto sem revisão arquitetural.
    mapa_publicacao, n_encurtadas = _encurtar_mapa(mapa)

    log_nrm.info(
        f"✅ {len(mapa_publicacao)}/{len(converter)} | "
        f"plat={plat_dom or 'none'} cupom='{cupom}' sku={sku} "
        f"ids={ids_globais} camp='{chave_campanha}' "
        f"estado={estado.value} encurtadas={n_encurtadas} "
        f"override={is_override}"
    )
    _log_cache_stats()

    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat, texto_limpo=texto_limpo,
        mapa=mapa_publicacao, preservar=preservar_lst, plat=plat_dom,
        cupom=cupom, sku=sku, tem_midia=bruta.tem_midia,
        media_obj=bruta.media_obj, estado_evento=estado,
        ids_globais=ids_globais, chave_campanha=chave_campanha,
        tem_host_campanha=tem_host_campanha, is_reply=bruta.is_reply,
        reply_to=bruta.reply_to, is_override=is_override,
  )
