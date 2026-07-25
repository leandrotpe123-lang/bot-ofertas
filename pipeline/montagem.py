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
    _tem_emoji,
)
from utils.cupom import _KW_CUPOM, extrair_todos_cupons
from utils import marcacao
from pipeline import papel

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

# ─────────────────────────────────────────────────────────────────
# PALETA — PAPEL → EMOJI
#
# Ratificada contra o corpus do canal de referência. Trocar um emoji
# é editar esta tabela: a classificação vive em pipeline.papel e não
# é tocada. Papel sem entrada aqui não recebe emoji nem negrito.
# ─────────────────────────────────────────────────────────────────
_EMOJI_POR_PAPEL: Dict[str, str] = {
    papel.TITULO:   "🔥",
    papel.PRECO:    "💵",
    papel.CUPOM:    "🎟",
    papel.RESGATE:  "⭐",
    papel.PARCELA:  "⭐",
    papel.LINK:     "✅",
    papel.VARIANTE: "🔹",
    papel.CARRINHO: "🛒",
    papel.FRETE:    "🚚",
    papel.AVISO:    "⚠️",
    papel.GATILHO:  "",
}


# ─────────────────────────────────────────────────────────────────
# REGEX
# PRIORIDADE IMPORTA
# ─────────────────────────────────────────────────────────────────

_KW_PRECO = re.compile(
    r'^\s*r\$\s*[\d.,]+',
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

# Bullet de origem (traço/ponto) que alguns canais colam antes de
# linhas estruturais ("-Resgate", "-Link produto"). Removido APENAS
# quando um emoji é prefixado — assim gatilho ("-PREÇÃO, ainda tem!")
# e specs ("-i5 1334U...") preservam o traço.
_RE_BULLET_PREFIXO = re.compile(r'^\s*[-–—•·]\s*')

_RE_URL_RENDER = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c]+'
)

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def _contar_produtos(texto: str) -> int:
    return sum(1 for l in texto.splitlines() if _KW_PRECO.search(l))


def _proteger_url_md(url: str) -> str:
    """
    Protege URLs contra interpretação de Markdown do Telegram.
    """

    return marcacao.proteger_url(url)

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
    proxima_conteudo: str = "",
) -> Optional[str]:
    """
    Adaptador puro: consulta o papel e devolve o emoji da paleta.

    NÃO classifica. A autoridade sobre o que a linha é pertence a
    pipeline.papel. Aqui só existe a tradução papel → desenho.
    """

    l = (linha or "").strip()

    if not l:
        return None

    # Emoji posto pelo divulgador na origem é preservado.
    if _tem_emoji(l):
        return None

    p = papel.classificar(
        l,
        eh_titulo=eh_titulo,
        is_multi=is_multi,
        proxima_linha=proxima_linha,
        proxima_conteudo=proxima_conteudo,
    )

    return _EMOJI_POR_PAPEL.get(p) or None


def _crases(
    linha: str,
    eh_titulo: bool = False,
) -> str:

    if eh_titulo:
        return linha

    # A AUTORIDADE do que é cupom é utils.cupom (MB: soberania do
    # módulo). A montagem NÃO reconhece cupom e NÃO conhece o dialeto
    # do Telegram: consulta a autoridade e entrega os literais a
    # utils.marcacao, que aplica a apresentação.
    if not _KW_CUPOM.search(linha):
        return linha

    return marcacao.codigo(linha, extrair_todos_cupons(linha))

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

        proxima_conteudo = next(
            (x.strip() for x in linhas[idx + 1:] if x.strip()),
            "",
        )

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

            # A AUTORIDADE do que é cupom é utils.cupom (MB: soberania
            # do módulo). A montagem NÃO reconhece cupom — ela apenas
            # evita repetir visualmente o mesmo código no mesmo post.
            codigos = extrair_todos_cupons(l)

            novos = [c for c in codigos if c not in cupons_vistos]

            # Só suprime quando há cupom REAL e TODOS já foram
            # renderizados antes. Sem cupom reconhecido, a linha é
            # preservada: em dúvida, apresentação nunca apaga.
            if codigos and not novos:

                log_enr.debug(
                    f"🔁 [MONTAGEM] Cupom repetido suprimido: {l[:60]}"
                )

                continue

            cupons_vistos.update(novos)
        eh_titulo = primeiro

        l = _crases(
            l,
            eh_titulo=eh_titulo,
        )

        # Item semântico: a linha carrega classificação — emoji posto
        # pelo divulgador na origem, ou atribuído pelo motor semântico.
        # É este fato, e não a forma do texto, que habilita o negrito.
        eh_item = _tem_emoji(l)

        if not eh_item:

            emoji = _emoji_linha(
                l,
                eh_titulo=eh_titulo,
                is_multi=is_multi,
                idx_bloco=idx,
                total_bloco=total_linhas,
                linha_anterior=linha_anterior,
                proxima_linha=proxima_linha,
                proxima_conteudo=proxima_conteudo,
            )

            if emoji:
                l = _RE_BULLET_PREFIXO.sub("", l)
                l = f"{emoji} {l}"
                eh_item = True

        primeiro = False

        # Marcação terminal: nenhuma camada semântica lê o texto depois
        # daqui (I-M1). A elegibilidade é o veredito acima (I-M7).
        l = marcacao.negrito(l, eh_item)

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
