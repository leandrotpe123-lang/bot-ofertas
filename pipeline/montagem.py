"""Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada."""
from __future__ import annotations

import asyncio
import io
import re

from dataclasses import dataclass
from typing import Dict, List, Optional

from logger import log_enr
from pipeline.normalizacao import (
    MensagemNormalizada,
    _KW_CUPOM,
    _FALSO_CUPOM,
    _tem_emoji,
)
from pipeline.estado_evento import _KW_EVENTO

# ─────────────────────────────────────────────────────────────────
# Dataclass de saída
# ─────────────────────────────────────────────────────────────────

@dataclass
class MensagemMontada:
    msg_id:        int
    chat:          str
    plat:          str
    sku:           str
    texto:         str
    imagem:        object
    mapa:          Dict[str, str]
    msg_id_origem: int


# ═══════════════════════════════════════════════════════════════════
# SISTEMA SEMÂNTICO PROFISSIONAL DE EMOJIS
#
# Filosofia:
#   • emoji FIXO por semântica
#   • consistência visual absoluta
#   • prioridade contextual
#   • baixo risco arquitetural
#   • compatibilidade retroativa
#   • contexto de bloco
#   • zero dependência de IA
#
# Resultado:
#   • aparência humana/profissional
#   • leitura ultra rápida
#   • padrão visual forte
#   • baixo ruído visual
# ═══════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────
# EMOJIS FIXOS
# ─────────────────────────────────────────────────────────────────

_EMOJI_TITULO_OFERTA = "🔥"
_EMOJI_TITULO_CUPOM  = "🚨"
_EMOJI_TITULO_EVENTO = "⚠️"

_EMOJI_DESCONTO = "🎟"
_EMOJI_PRECO    = "💵"

_EMOJI_RESGATE  = "⭐️"
_EMOJI_LINK     = "✅"

_EMOJI_MULTI    = "🔹"
_EMOJI_CARRINHO = "🛒"

_EMOJI_FRETE    = "🚚"
_EMOJI_PARCELA  = "💳"


# Compatibilidade retroativa
_EMJ: Dict[str, List[str]] = {
    "titulo_oferta": [_EMOJI_TITULO_OFERTA],
    "titulo_cupom":  [_EMOJI_TITULO_CUPOM],
    "titulo_evento": [_EMOJI_TITULO_EVENTO],

    "desconto":      [_EMOJI_DESCONTO],
    "preco_produto": [_EMOJI_PRECO],

    "resgate":       [_EMOJI_RESGATE],
    "carrinho":      [_EMOJI_CARRINHO],

    "frete":         [_EMOJI_FRETE],
    "multi_item":    [_EMOJI_MULTI],

    "link_prod":     [_EMOJI_LINK],

    "parcelamento":  [_EMOJI_PARCELA],
}

_EMJ_IDX: Dict[str, int] = {k: 0 for k in _EMJ}


def _prox_emoji(cat: str) -> str:
    """
    Compatibilidade retroativa.
    Mantido para não quebrar chamadas antigas.
    """
    try:
        return _EMJ[cat][0]
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────
# REGEX
# PRIORIDADE IMPORTA
# ─────────────────────────────────────────────────────────────────

_KW_PRECO = re.compile(
    r'^\s*r\$\s*[\d.,]+',
    re.I,
)

_KW_DESCONTO = re.compile(
    r'\b('
    r'cupom|'
    r'cupons|'
    r'cashback|'
    r'c[oó]digo|'
    r'desconto|'
    r'resgate\s+todos\s+os\s+cupons|'
    r'\d+\s*%\s*off|'
    r'r\$\s*[\d.,]+\s*off|'
    r'off\s+em\s+r\$|'
    r'limite\s*r\$'
    r')\b',
    re.I,
)

_KW_FRETE = re.compile(
    r'\b('
    r'frete\s+gr[aá]tis|'
    r'frete\s+gr[aá]t|'
    r'entrega\s+gr[aá]tis|'
    r'sem\s+frete|'
    r'frete\s+0'
    r')\b',
    re.I,
)

_KW_RESGATE = re.compile(
    r'\b('
    r'resgate|'
    r'resgatar|'
    r'acesse|'
    r'ative|'
    r'ativar|'
    r'use\s+o\s+cupom|'
    r'pegue\s+aqui'
    r')\b',
    re.I,
)

_KW_CARRINHO = re.compile(
    r'\b(?:carrinho|cart)\b',
    re.I,
)

_KW_LINK_PROD = re.compile(
    r'\b('
    r'link\s+produto|'
    r'link\s+oferta|'
    r'link\s+lista|'
    r'clique|'
    r'mostrar\s+mais|'
    r'teste\s+aqui|'
    r'veja\s+aqui|'
    r'acessar'
    r')\b',
    re.I,
)

_KW_PARCELA = re.compile(
    r'\b\d+\s*x\s*sem\s+juros\b',
    re.I,
)

# Ex:
# 8/256GB - R$ 1599
# Branco - R$ 99
# Azul - R$ 120
_KW_MULTI_ITEM = re.compile(
    r'^('
    r'\d+\/\d+\s*gb|'
    r'\d+\/\d+|'
    r'[a-zà-ÿ0-9\s\-\+]{2,50}\s*[-–]\s*r\$'
    r')',
    re.I,
)

_KW_URGENCIA = re.compile(
    r'\b('
    r'esgotando|'
    r'últimas?\s+unidades|'
    r'acabando|'
    r'corre|'
    r'corra|'
    r'voando|'
    r'pre[cç][aã]o|'
    r'por\s+tempo\s+limitado'
    r')\b',
    re.I,
)

_RE_LIXO_PREF = re.compile(
    r'^\s*[-:•|]\s*(?:MG|AMZ)\s*[-:•]?\s*',
    re.I,
)

_RE_ANUNCIO = re.compile(
    r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$',
    re.I,
)

_RE_URL_RENDER = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c]+'
)

_RE_JA_EMOJI = re.compile(
    r'^[🎟💵⭐✅🔹🛒🚚💳🔥🚨⚠️]'
)


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))


def _eh_linha_cupom(linha: str) -> bool:
    return bool(
        _KW_DESCONTO.search(linha)
        or _KW_CUPOM.search(linha)
    )


def _eh_preco_puro(linha: str) -> bool:
    """
    Detecta linha monetária REAL.

    Ex:
      R$ 599
      R$ 1299 no pix

    NÃO:
      R$ 100 OFF
    """

    l = linha.strip()

    if _eh_linha_cupom(l):
        return False

    return bool(_KW_PRECO.search(l))


def _eh_multi_item_real(linha: str) -> bool:
    return bool(_KW_MULTI_ITEM.search(linha))


def _eh_titulo_evento(linha: str) -> bool:
    return bool(_KW_EVENTO.search(linha))


def _eh_titulo_cupom(linha: str) -> bool:
    return bool(_KW_CUPOM.search(linha))


def _normalizar_linha_contexto(linha: str) -> str:
    """
    Normalização leve para análise semântica.

    NÃO altera render final.
    """

    return re.sub(r'\s+', ' ', linha.strip().lower())


def _proteger_url_md(url: str) -> str:
    """
    Protege URLs contra interpretação de Markdown do Telegram.
    """

    return (
        url.replace('\\', '\\\\')
           .replace('*', '\\*')
           .replace('`', '\\`')
           .replace('[', '\\[')
    )


# ─────────────────────────────────────────────────────────────────
# MOTOR SEMÂNTICO PROFISSIONAL
# ─────────────────────────────────────────────────────────────────

def _emoji_linha(
    linha: str,
    eh_titulo: bool,
    is_multi: bool = False,
    idx_bloco: int = 0,
    total_bloco: int = 0,
    linha_anterior: str = "",
    proxima_linha: str = "",
) -> Optional[str]:

    l = (linha or "").strip()

    if not l:
        return None

    # Já possui emoji manual
    if _tem_emoji(l):
        return None

    # ═══════════════════════════════════════════════════════════
    # TÍTULOS
    # ═══════════════════════════════════════════════════════════
    if eh_titulo:

        if _eh_titulo_evento(l):
            return _EMOJI_TITULO_EVENTO

        if _eh_titulo_cupom(l):
            return _EMOJI_TITULO_CUPOM

        return _EMOJI_TITULO_OFERTA

    # ═══════════════════════════════════════════════════════════
    # CARRINHO
    # ═══════════════════════════════════════════════════════════
    if _KW_CARRINHO.search(l):
        return _EMOJI_CARRINHO

    # ═══════════════════════════════════════════════════════════
    # PARCELAMENTO
    # ═══════════════════════════════════════════════════════════
    if _KW_PARCELA.search(l):
        return _EMOJI_PARCELA

    # ═══════════════════════════════════════════════════════════
    # FRETE
    # ═══════════════════════════════════════════════════════════
    if _KW_FRETE.search(l):
        return _EMOJI_FRETE

    # ═══════════════════════════════════════════════════════════
    # RESGATE
    # PRIORIDADE ALTA
    # ═══════════════════════════════════════════════════════════
    if _KW_RESGATE.search(l):
        return _EMOJI_RESGATE

    # ═══════════════════════════════════════════════════════════
    # LINKS
    # ═══════════════════════════════════════════════════════════
    if _KW_LINK_PROD.search(l):
        return _EMOJI_LINK

    # ═══════════════════════════════════════════════════════════
    # DESCONTO / CUPOM / CASHBACK
    # ═══════════════════════════════════════════════════════════
    if _eh_linha_cupom(l):
        return _EMOJI_DESCONTO

    # ═══════════════════════════════════════════════════════════
    # MULTI-ITEM
    #
    # Contexto:
    #   bloco multi-produto
    #   OU linha variante/preço
    # ═══════════════════════════════════════════════════════════
    if is_multi and _eh_multi_item_real(l):
        return _EMOJI_MULTI

    # Contexto adicional:
    # Se linha anterior foi desconto/cupom
    # e próxima linha é URL,
    # provavelmente é bloco variante.
    if (
        _eh_multi_item_real(l)
        and _RE_URL_RENDER.search(proxima_linha or "")
    ):
        return _EMOJI_MULTI

    # ═══════════════════════════════════════════════════════════
    # PREÇO PURO
    # ═══════════════════════════════════════════════════════════
    if _eh_preco_puro(l):
        return _EMOJI_PRECO

    return None


def _crases(
    linha: str,
    eh_titulo: bool = False,
) -> str:

    if "http" in linha or eh_titulo or "`" in linha:
        return linha

    if not _KW_CUPOM.search(linha):
        return linha

    def _sub(m: re.Match) -> str:
        c = m.group(0)

        if c in _FALSO_CUPOM or len(c) < 4:
            return c

        return f"`{c}`"

    return re.sub(
        r'\b([A-Z][A-Z0-9_-]{4,20})\b',
        _sub,
        linha,
    )


# ─────────────────────────────────────────────────────────────────
# MONTAGEM DE TEXTO
# ─────────────────────────────────────────────────────────────────

def montar_texto(norm: MensagemNormalizada) -> str:

    mapa = {
        **norm.mapa,
        **{u: u for u in norm.preservar},
    }

    linhas = norm.texto_limpo.split("\n")

    is_multi = _contar_produtos(norm.texto_limpo) >= 2

    saida: List[str] = []

    primeiro = True

    cupons_vistos: set = set()

    total_linhas = len(linhas)

    for idx, linha in enumerate(linhas):

        l = linha.strip()

        linha_anterior = linhas[idx - 1] if idx > 0 else ""
        proxima_linha  = linhas[idx + 1] if idx + 1 < total_linhas else ""

        if not l:
            saida.append("")
            continue

        if _RE_ANUNCIO.match(l):
            saida.append(l)
            continue

        l = _RE_LIXO_PREF.sub("", l).strip()

        if not l:
            continue

        urls_na_linha = _RE_URL_RENDER.findall(l)

        sem_urls = _RE_URL_RENDER.sub("", l).strip()

        # ─────────────────────────────────────────────
        # Linha só URL
        # ─────────────────────────────────────────────
        if urls_na_linha and not sem_urls:

            for u in urls_na_linha:

                uc = u.rstrip('.,;)>')

                url_final = mapa.get(uc, uc)

                saida.append(_proteger_url_md(url_final))

            continue

        # ─────────────────────────────────────────────
        # URL inline
        # ─────────────────────────────────────────────
        def _sub_url(m: re.Match) -> str:

            u = m.group(0).rstrip('.,;)>')

            url_final = mapa.get(u, m.group(0))

            return _proteger_url_md(url_final)

        l = _RE_URL_RENDER.sub(_sub_url, l).strip()

        if not l:
            continue

        # ─────────────────────────────────────────────
        # Deduplicação de cupom
        # ─────────────────────────────────────────────
        if _KW_CUPOM.search(l):

            cupons_linha = re.findall(
                r'\b([A-Z][A-Z0-9_-]{3,19})\b',
                l,
            )

            cupons_novos = [
                c for c in cupons_linha
                if c not in _FALSO_CUPOM
                and c not in cupons_vistos
            ]

            if cupons_linha and not cupons_novos:

                log_enr.debug(
                    f"🔁 Cupom duplicado suprimido: {l[:60]}"
                )

                continue

            cupons_vistos.update(cupons_novos)

        eh_titulo = primeiro

        l = _crases(
            l,
            eh_titulo=eh_titulo,
        )

        if not _tem_emoji(l):

            emoji = _emoji_linha(
                l,
                eh_titulo=eh_titulo,
                is_multi=is_multi,
                idx_bloco=idx,
                total_bloco=total_linhas,
                linha_anterior=linha_anterior,
                proxima_linha=proxima_linha,
            )

            if emoji:
                l = f"{emoji} {l}"

        primeiro = False

        saida.append(l)

    return "\n".join(saida).strip()


# ─────────────────────────────────────────────────────────────────
# IMAGENS
# ─────────────────────────────────────────────────────────────────

async def preparar_imagem_tg(media_obj) -> Optional[object]:

    from client import client
    import config

    try:

        buf = io.BytesIO()

        try:

            res = await asyncio.wait_for(
                client.download_media(
                    media_obj,
                    file=buf,
                ),
                timeout=config._TIMEOUT_DOWNLOAD_MIDIA,
            )

        except asyncio.TimeoutError:

            log_enr.warning(
                f"⏱ download_media timeout após "
                f"{config._TIMEOUT_DOWNLOAD_MIDIA}s"
            )

            return None

        if res is None:
            return None

        buf.seek(0)

        if buf.getbuffer().nbytes < 500:
            return None

        buf.name = "imagem.jpg"

        return buf

    except Exception as e:

        log_enr.warning(f"⚠️ download_media: {e}")

        return None


async def _resolver_imagem(
    norm: MensagemNormalizada,
) -> object:

    if norm.tem_midia:

        img = await preparar_imagem_tg(norm.media_obj)

        if img:
            return img

    return None


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────

async def montar(
    norm: MensagemNormalizada,
) -> MensagemMontada:

    texto = montar_texto(norm)

    imagem = await _resolver_imagem(norm)

    return MensagemMontada(
        msg_id=norm.msg_id,
        chat=norm.chat,
        plat=norm.plat,
        sku=norm.sku,
        texto=texto,
        imagem=imagem,
        mapa=norm.mapa,
        msg_id_origem=norm.msg_id,
  )
