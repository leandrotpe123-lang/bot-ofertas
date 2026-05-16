"""
Camada 3 — Normalização.

Responsabilidade única: transformar uma MensagemBruta em uma
MensagemNormalizada, orquestrando a resolução e a afiliação dos
links contidos na mensagem, e produzindo os metadados derivados
necessários às camadas seguintes.

Operações que constituem a normalização:
  - limpeza estrutural do texto (limpar_texto)
  - verificação de viabilidade do texto (tem_contexto)
  - resolução e afiliação de links (via url_resolver e affiliate_router)
  - derivação de plataforma dominante, cupom, SKU e identificadores

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
from pipeline.classificacao import (
    LinkClassificado,
    _classificar_cached,
    classificar_links,
)
from pipeline.estado_evento import (
    EstadoEvento,
    detectar_estado_evento,
    _JANELA_C3,
)
from pipeline.ingestao import MensagemBruta
from utils.cupom import _FALSO_CUPOM, _KW_CUPOM, extrair_cupom, extrair_todos_cupons
from utils.hashes import _fp_c3
from utils.url_resolver import desencurtar

# ── Reexportações temporárias de compatibilidade ──────────────────
# Consumidos hoje por montagem e deduplicação a partir deste módulo.
# Remover quando esses módulos forem revisados em suas fases.
__all__ = [
    "MensagemNormalizada", "normalizar", "limpar_texto", "tem_contexto",
    "EstadoEvento", "detectar_estado_evento", "_JANELA_C3",
    "_KW_CUPOM", "_FALSO_CUPOM", "extrair_cupom", "extrair_todos_cupons",
    "_tem_emoji", "desencurtar",
]


# ─────────────────────────────────────────────────────────────────
# LIMPEZA DE TEXTO
# Transformação estrutural do texto bruto. Permanece na normalização
# por ser operação intrínseca à transformação, com consumidor único.
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
# Verificação intrínseca: o texto possui substância promocional
# mínima para justificar o processamento? Não é política de filtro.
# ─────────────────────────────────────────────────────────────────
def tem_contexto(texto: str) -> bool:
    """
    Verifica se o texto possui conteúdo promocional relevante o
    suficiente para prosseguir. Avalia indicadores promocionais
    (percentuais, preços, palavras-chave) ou comprimento mínimo.
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
    msg_id:        int
    chat:          str
    texto_limpo:   str
    mapa:          Dict[str, str]
    preservar:     List[str]
    plat:          str
    cupom:         str
    sku:           str
    tem_midia:     bool
    media_obj:     object
    estado_evento: EstadoEvento = EstadoEvento.NEW
    ids_globais:   List[str]    = field(default_factory=list)
    is_reply:      bool         = False
    reply_to:      int          = 0
    is_override:   bool         = False


# ─────────────────────────────────────────────────────────────────
# RESOLUÇÃO E AFILIAÇÃO DE UM LINK
# ─────────────────────────────────────────────────────────────────
async def _normalizar_um(
    lc: LinkClassificado,
    sessao: aiohttp.ClientSession,
    msg_id: int = 0,
) -> Tuple[str, Optional[str], str]:
    """
    Resolve e afilia um único link classificado. Devolve a tupla
    (url_original, url_convertida, plataforma). A url_convertida é
    None quando o link não pôde ser afiliado ou não é aproveitável.
    """
    from plataformas.affiliate_router import rotear_afiliacao

    plat = lc.plat
    url_original = lc.url_original

    if plat == "mundial":
        return url_original, url_original, "mundial"
    if plat == "preservar":
        return url_original, url_original, "preservar"
    if plat is None or lc.tipo in (
        "invalido", "bloqueado", "grupo_externo", "desconhecido",
    ):
        return url_original, None, plat or "none"
    if lc.tipo == "claims":
        return url_original, url_original, plat

    cached = _get_final(url_original) or db_get_link(url_original)
    if cached:
        return url_original, cached, plat

    url = url_original
    precisa_expandir = (plat == "expandir") or lc.eh_encurtador

    if precisa_expandir:
        try:
            url_expandida = await desencurtar(url, sessao)
            if url_expandida and url_expandida != url:
                url = url_expandida
                lc = _classificar_cached(url)
                if lc.plat:
                    plat = lc.plat
                if plat is None or plat in ("expandir", "none"):
                    log_nrm.warning(
                        f"🚫 expansão não revelou plataforma final: {url[:80]}"
                    )
                    return url_original, None, "none"
                if plat == "mundial":
                    return url_original, url, "mundial"
                if lc.tipo == "claims":
                    return url_original, url, plat
                cached = _get_final(url) or db_get_link(url)
                if cached:
                    return url_original, cached, plat
        except Exception as e:
            log_nrm.warning(f"  ⚠️ expansão antecipada falhou: {e}")
            if plat == "expandir":
                return url_original, None, "none"

    if plat == "expandir":
        return url_original, None, "none"

    convertido = await rotear_afiliacao(plat, url, sessao, msg_id)
    return url_original, convertido, plat


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT — NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────
async def normalizar(
    bruta: MensagemBruta,
    is_override: bool = False,
) -> Optional[MensagemNormalizada]:
    """
    Transforma uma MensagemBruta em uma MensagemNormalizada.

    O parâmetro is_override é fornecido pelo orquestrador a partir do
    resultado da avaliação de filtros; a normalização apenas o propaga
    para a estrutura de saída.

    Retorna None quando a mensagem não é normalizável: texto vazio,
    ausência de contexto promocional, ou ausência de links aproveitáveis.
    """
    if not bruta.texto.strip():
        return None

    texto_limpo = limpar_texto(bruta.texto)
    if not tem_contexto(texto_limpo):
        return None

    classificados = classificar_links(bruta.links)
    converter     = [
        lc for lc in classificados if lc.plat not in ("preservar", None)
    ]
    preservar_lst = [
        lc.url_original for lc in classificados if lc.plat == "preservar"
    ]
    if not converter and not preservar_lst:
        return None

    sessao = await _get_session()
    resultados = await asyncio.gather(
        *[_normalizar_um(lc, sessao, bruta.msg_id) for lc in converter[:50]],
        return_exceptions=True,
    )

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

    sku = next(
        (f"{lc.plat[:3]}_{lc.sku}" for lc in classificados if lc.sku and lc.plat),
        "",
    )
    if not sku:
        for conv in mapa.values():
            lc_conv = _classificar_cached(conv)
            if lc_conv.sku and lc_conv.plat:
                sku = f"{lc_conv.plat[:3]}_{lc_conv.sku}"
                break

    ids_globais: List[str] = []
    for orig, conv in mapa.items():
        lc_orig = _classificar_cached(orig)
        if lc_orig.sku and lc_orig.sku not in ids_globais:
            ids_globais.append(lc_orig.sku)
        if conv:
            lc_conv = _classificar_cached(conv)
            if lc_conv.sku and lc_conv.sku not in ids_globais:
                ids_globais.append(lc_conv.sku)
    if sku and sku not in ids_globais:
        ids_globais.append(sku)

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

    log_nrm.info(
        f"✅ {len(mapa)}/{len(converter)} | plat={plat_dom or 'none'} "
        f"cupom='{cupom}' sku={sku} ids={ids_globais} "
        f"estado={estado.value} override={is_override}"
    )
    _log_cache_stats()

    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat, texto_limpo=texto_limpo,
        mapa=mapa, preservar=preservar_lst, plat=plat_dom, cupom=cupom,
        sku=sku, tem_midia=bruta.tem_midia, media_obj=bruta.media_obj,
        estado_evento=estado, ids_globais=ids_globais,
        is_reply=bruta.is_reply, reply_to=bruta.reply_to,
        is_override=is_override,
)
