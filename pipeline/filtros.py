"""
Camada — Filtros de Conteúdo.

Responsabilidade única: decidir se uma mensagem deve ser bloqueada
antes de prosseguir no processamento, aplicando critérios de política
de conteúdo.

Aplica dois conjuntos de políticas:
  - filtro de produtos: bloqueia termos de produtos indesejados,
    com exceção para multi-produto e override por sinal social forte
  - filtro de posts indesejados: bloqueia posts que pedem link,
    encaminham para canais externos, contêm vídeo Shopee, ou apontam
    para domínios concorrentes sem link de plataforma legítima

NÃO faz:
  - normalização de mensagem (responsabilidade da normalização)
  - registro de posts pendentes (responsabilidade do orquestrador)
  - classificação de plataforma (responsabilidade da classificação)
  - verificação de viabilidade de texto (permanece na normalização)
  - persistência

A decisão sobre o que fazer com uma mensagem bloqueada — descartá-la
ou registrá-la como pendente — pertence ao orquestrador. Este módulo
apenas avalia e comunica o resultado.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Tuple

from logger import log_cls
from pipeline.classificacao import _ENCURTADORES_POR_PLAT, _classificar_cached
from utils.urls import _netloc


# ── Conjunto de plataformas comerciais ────────────────────────────
_PLAT_COMERCIAL = frozenset(_ENCURTADORES_POR_PLAT.keys()) | {"expandir"}


# ── Filtro de produtos: termos bloqueados ─────────────────────────
_FILTRO_TEXTO = [
    "Monitor Samsung", "Fonte Mancer", "Placa de video", "Monitor LG",
    "PC home Essential", "Suporte articulado", "VHAGAR", "Superframe",
    "AM5", "AM4", "GTX", "Placa de Vídeo", "DDR5", "DDR4", "Dram",
    "Monitor Safe", "Monitor Redragon", "CL18", "CL16", "CL32",
    "MT/s", "MHz", "RX 580", "Ryzen", "Placa Mãe", "Gabinete Gamer",
    "Water Cooler", "Monitor Dell", "Monitor Gamer", "Air Cooler",
]


# ── Filtro de produtos: detecção de multi-produto (exceção) ───────
_RE_MULTI_OFERTA = re.compile(
    r'\b(?:ofertas?|promoções?)\s+(?:na\s+|no\s+|da\s+)?'
    r'(?:shopee|amazon|magalu|magazine\s*luiza)\b', re.I,
)
_RE_PRECO_LINHA = re.compile(r'R\$\s?[\d.,]+')
_RE_URL_COUNT   = re.compile(r'https?://')


# ── Filtro de produtos: sinal social forte (override) ─────────────
_RE_SINAL_FORTE = re.compile(
    r'\b(?:'
    r'esgota\s+r[aá]pido|corre|voa|pega\s+logo|'
    r'quem\s+pegou\s+pegou|acaba\s+r[aá]pido|poucas?\s+unidades?|'
    r'bug|erro\s+de\s+pre[cç]o|j[aá]\s+era|insano'
    r')\b',
    re.I,
)


# ── Filtro de posts indesejados ───────────────────────────────────
_RE_EXCLUSIVO_CANAL = re.compile(
    r'\b(?:exclusivo|exclusiva|s[oó]|somente|apenas)\s+'
    r'(?:do|da|de|para|pra|p/|pro|pros)\s+'
    r'(?:canal|grupo|membros?|seguidores?|@\w+)',
    re.I,
)
_RE_PEDE_LINK = re.compile(
    r'\b(?:envie?|mande|manda|envia|coloque|cole|cola|passe|passa)\s+'
    r'(?:o\s+|seu\s+|os\s+|aí\s+(?:o\s+)?)?link',
    re.I,
)
_RE_VIA_CHAT_SITE = re.compile(
    r'\b(?:no\s+chat|pelo\s+(?:nosso\s+)?site|aqui\s+no\s+grupo|'
    r'aqui\s+no\s+canal|aqui\s+embaixo|abaixo|aba\s+de)\b',
    re.I,
)
_RE_SHOPEE_VIDEO = re.compile(
    r'https?://(?:[a-z0-9-]+\.)?shopee\.com\.br/'
    r'(?:v|vt|live|video)/',
    re.I,
)
_RE_TELEGRAM_LINK = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org)/',
    re.I,
)
_DOMINIOS_CONCORRENTES = frozenset({
    "fadadoscupons.com", "fadadoscupons.com.br",
    "pelando.com.br", "pelando.com",
    "promobit.com.br", "promobit.com",
    "savvii.com.br",
    "tecmundo.com.br/promobit",
    "buscape.com.br/cupom",
    "meliuz.com.br",
    "cuponomia.com.br",
    "cuponeria.com.br",
    "picodi.com",
    "cupomvalido.com.br",
})


# ── Contrato de saída ─────────────────────────────────────────────
@dataclass
class ResultadoFiltro:
    """
    Resultado da avaliação de filtros sobre uma mensagem.

      - bloqueado : True se a mensagem deve ser barrada
      - override  : True se seria bloqueada mas foi liberada por
                    sinal social forte
      - motivo    : identificador textual da razão (rastreabilidade)
    """
    bloqueado: bool
    override:  bool
    motivo:    str


# ── Filtro de produtos ────────────────────────────────────────────
def _tem_sinal_social_forte(texto: str) -> bool:
    if not texto:
        return False
    return bool(_RE_SINAL_FORTE.search(texto))


def _eh_multi_produto(texto: str) -> bool:
    if _RE_MULTI_OFERTA.search(texto):
        return True
    linhas_preco = sum(
        1 for l in texto.splitlines() if _RE_PRECO_LINHA.search(l)
    )
    return linhas_preco >= 2 or len(_RE_URL_COUNT.findall(texto)) >= 3


def _avaliar_filtro_produto(
    texto: str,
    contexto_extra: str = "",
) -> Tuple[bool, bool, str]:
    """
    Avalia o filtro de produtos. Retorna (bloqueado, override, motivo).

    Posts multi-produto são isentos do filtro. Um termo bloqueado pode
    ser liberado por override quando o contexto traz sinal social forte.
    """
    if _eh_multi_produto(texto):
        return False, False, ""

    tl = texto.casefold()
    for termo in _FILTRO_TEXTO:
        if termo.casefold() in tl:
            if _tem_sinal_social_forte(contexto_extra):
                log_cls.debug(f"⚡ Override social '{termo}'")
                return False, True, ""
            log_cls.debug(f"🚫 Filtro produto: '{termo}'")
            return True, False, f"produto_bloqueado:{termo}"
    return False, False, ""


# ── Filtro de posts indesejados ───────────────────────────────────
def _tem_link_concorrente(texto: str) -> bool:
    urls = re.findall(r'https?://[^\s\)>\]\}",;]+', texto)
    for url in urls:
        nl = _netloc(url)
        if nl in _DOMINIOS_CONCORRENTES:
            return True
        for d in _DOMINIOS_CONCORRENTES:
            if nl.endswith("." + d):
                return True
    return False


def _tem_link_plataforma_real(texto: str) -> bool:
    urls = re.findall(r'https?://[^\s\)>\]\}",;]+', texto)
    if not urls:
        return False
    for url in urls:
        lc = _classificar_cached(url)
        if lc.plat in _PLAT_COMERCIAL:
            return True
    return False


def _avaliar_post_indesejado(texto: str) -> Tuple[bool, str]:
    """
    Avalia o filtro de posts indesejados. Retorna (bloqueado, motivo).
    """
    if _RE_EXCLUSIVO_CANAL.search(texto):
        return True, "exclusivo_canal"
    if _RE_SHOPEE_VIDEO.search(texto):
        return True, "shopee_video"
    if _RE_PEDE_LINK.search(texto) and _RE_VIA_CHAT_SITE.search(texto):
        return True, "pede_link_servico"
    if _tem_link_concorrente(texto) and not _tem_link_plataforma_real(texto):
        return True, "link_concorrente_sem_plataforma"
    if (_RE_TELEGRAM_LINK.search(texto)
            and not _tem_link_plataforma_real(texto)):
        return True, "encaminha_telegram"
    return False, ""


# ── Entrypoint público ────────────────────────────────────────────
def avaliar(texto: str, contexto_extra: str = "") -> ResultadoFiltro:
    """
    Avalia todos os filtros de conteúdo sobre uma mensagem.

    Aplica primeiro o filtro de produtos, depois o filtro de posts
    indesejados. Devolve um ResultadoFiltro estruturado, permitindo
    ao orquestrador decidir o tratamento da mensagem bloqueada.
    """
    bloq_prod, override, motivo_prod = _avaliar_filtro_produto(
        texto, contexto_extra=contexto_extra,
    )
    if bloq_prod:
        return ResultadoFiltro(
            bloqueado=True, override=False, motivo=motivo_prod,
        )

    bloq_post, motivo_post = _avaliar_post_indesejado(texto)
    if bloq_post:
        return ResultadoFiltro(
            bloqueado=True, override=False, motivo=motivo_post,
        )

    return ResultadoFiltro(
        bloqueado=False, override=override, motivo="",
    )
