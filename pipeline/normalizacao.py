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
  - resolução de URL via rede (responsabilidade de utils.url_resolver)
  - registro de posts pendentes (responsabilidade do orquestrador)
"""
from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp

from database import db_get_link
from globals import _get_session, _get_final, _log_cache_stats
from logger import log_nrm
from utils.categorias_universais import classificar_universal, eh_encurtador_generico
from pipeline.ingestao import MensagemBruta
from plataformas import registry
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link
from utils.cupom import extrair_cupom, extrair_todos_cupons
from utils.encurtador import encurtar
from utils.url_resolver import desencurtar
from utils.urls import _netloc, host_canonico_campanha, chaves_canonicas_campanha

# ── API pública do módulo ─────────────────────────────────────────
__all__ = [
    "MensagemNormalizada", "normalizar", "limpar_texto", "tem_contexto",
    "_tem_emoji",
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
    ids_globais:       List[str]    = field(default_factory=list)
    # Vínculo POR LINK que a plataforma entrega via contrato e que a
    # travessia PRESERVA: (plataforma_do_link, id_produto, tipo_link).
    # ids_globais é projeção de idents — mesma passada de derivação.
    idents: List[Tuple[str, str, str]] = field(default_factory=list)
    # Lista completa de cupons — snapshot derivado UMA vez na normalização
    # (extrair_todos_cupons sobre texto_limpo). Consumida por identidade e
    # score; o representante norm.cupom (extrair_cupom) é derivação distinta.
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
    # normalizar() usa para derivar norm.cupom E norm.cupons. Após Cupom-1
    # o pipeline consome norm.cupons; este campo é insumo interno e sai do
    # contrato no Cupom-2.
    code_entities:     List[str]    = field(default_factory=list)
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
def _identidade_de(url: str) -> Optional[Tuple[str, str, str]]:
    """
    Devolve (plataforma, id_produto, tipo_link) de uma URL afiliada
    longa — a estrutura que a plataforma entrega via contrato — ou
    None quando a URL não pertence a uma plataforma ou não
    corresponde a um produto individual.
    """
    plataforma = registry.resolver(url)
    if plataforma is None:
        return None
    ident = plataforma.extrai_identidade(url)
    if ident.id_produto is AUSENTE:
        return None
    return (plataforma.identificador, str(ident.id_produto),
            ident.tipo_link.value)


def _eh_host_de_campanha(url: str) -> bool:
    """
    Verdadeiro se o host de uma única URL pertence à união de hosts
    de campanha composta a partir das plataformas registradas.
    Predicado unitário, sobre a URL afiliada LONGA, usando _netloc.
    A semântica de casamento (igualdade ou sufixo) é idêntica à do
    conjunto hardcoded anterior, preservando o comportamento.
    """
    host = _netloc(url)
    hosts = registry.compor_capacidade("hosts_campanha").keys()
    for h in hosts:
        if host == h or host.endswith("." + h):
            return True
    return False

def _tem_sinal_cashback(texto: str) -> bool:
    """
    Verdadeiro se a PRIMEIRA linha não-vazia do texto casa algum
    padrão de cashback composto a partir das plataformas registradas
    (união de sinais_cashback). Opera sobre a MESMA linha-título que a
    deduplicação usa em _eh_post_cashback, preservando o escopo e,
    portanto, o comportamento. Cada padrão é regex, casado com
    re.IGNORECASE (equivalente ao re.I do regex anterior).
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    for padrao in registry.compor_capacidade("sinais_cashback").keys():
        if re.search(padrao, titulo, re.IGNORECASE):
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
    cupom    = extrair_cupom(texto_limpo, getattr(bruta, "code_entities", None))
    cupons   = extrair_todos_cupons(texto_limpo, getattr(bruta, "code_entities", None))

    # ── DERIVAÇÃO DE IDENTIDADE ───────────────────────────────────
    # Opera EXCLUSIVAMENTE sobre as URLs afiliadas LONGAS (conv),
    # antes de qualquer encurtamento. A URL original (orig) NÃO é
    # fonte de identidade: a fonte canônica é a URL afiliada longa.
    # A normalização é a autoridade única de derivação — produto
    # (ids_globais, sku) e campanha (chave_campanha, tem_host_campanha).
    urls_longas = list(canonicas.values())

    ids_globais: List[str] = []
    idents: List[Tuple[str, str, str]] = []
    for conv in urls_longas:
        trio = _identidade_de(conv)
        if trio and trio[1] not in ids_globais:
            ids_globais.append(trio[1])
            idents.append(trio)

    sku = ids_globais[0] if ids_globais else ""

    # Identidade de campanha: chave_campanha e tem_host_campanha
    # DEVEM derivar da mesma população de URLs — as URLs de campanha
    # — para que sejam coerentes entre si. A chave_campanha jamais
    # pode ser derivada de uma URL de produto ou de landing.
    urls_campanha     = [u for u in urls_longas if _eh_host_de_campanha(u)]
    tem_host_campanha = bool(urls_campanha)
    chave_campanha    = host_canonico_campanha(urls_campanha)
    chaves_campanha   = chaves_canonicas_campanha(urls_campanha)
    tem_sinal_cashback = _tem_sinal_cashback(texto_limpo)

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
        cupom=cupom, cupons=cupons, sku=sku, tem_midia=bruta.tem_midia,
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
