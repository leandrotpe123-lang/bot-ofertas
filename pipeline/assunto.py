# pipeline/assunto.py — CLASSIFICAÇÃO DO ASSUNTO DO POST
#
# Responsabilidade ÚNICA: dado o texto (e fatos derivados pela
# normalização), dizer QUAL É A ESPÉCIE DO ASSUNTO do post.
#
# NÃO faz:
#   - derivação de identidade / âncoras     (identidade_oferta)
#   - memória de códigos / cupom_idx        (identidade_oferta, por ora)
#   - decisão de duplicidade / evolução     (deduplicacao / decisao)
#   - I/O de qualquer natureza              (camada PURA)
#
# CONTRATO PÚBLICO (o resto é interno — regex NUNCA saem daqui):
#   eh_post_cupom(texto)                      -> bool
#   eh_post_cashback(texto, tem_sinal)        -> bool
#   eh_post_evento(texto, tem_host)           -> bool
#   eh_lista_cupons(texto)                    -> bool
#   extrair_pct_cashback(texto)               -> str
#   buscar_calendario_comercial(texto)        -> re.Match | None
#
# C1 (refactor puro): nova casa dos detectores, extraídos byte-a-byte de
# identidade_oferta.py. O classificador AINDA NÃO tem autoridade sobre a
# família — ancoras() segue decidindo como antes. A autoridade só será
# concedida em C3, após o endurecimento (C2).
from __future__ import annotations

import re
from typing import Optional

from pipeline.estado_evento import _KW_EVENTO
from utils.cupom import _KW_CUPOM


# ─────────────────────────────────────────────────────────────────
# Detecção do TIPO do post (define qual identidade usar)
# ─────────────────────────────────────────────────────────────────
_RE_TITULO_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|coupon|novo\s+cupom|cupom\s+novo)\b',
    re.I,
)
_RE_TITULO_CASHBACK = re.compile(
    r'\b(?:cashback|cash\s*back)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')

# Resíduo NOMEADO de calendário comercial — fronteira explícita.
# NÃO pertence à família interativa canônica (_KW_EVENTO, dono:
# pipeline.estado_evento) e NÃO deve subir para lá: no canônico,
# este vocabulário furaria a saturação e mudaria títulos no pico
# comercial. Serve apenas ao gate/chave de identidade de campanha.
_RE_CALENDARIO_COMERCIAL = re.compile(
    r'\b(?:black\s*friday|esquenta)\b',
    re.I,
)

# Padrão "lista de cupons" — formato típico:
#   "🔥 R$ 100 OFF em R$ 900: INFLU100"
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Padrão "OFF: CODIGO" no título
_RE_TITULO_OFF_COD = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Linha de cashback (sem precisar de "OFF" literal). Usado pra
# detectar posts Shopee de cashback como post-cupom.
_RE_CASHBACK_LINHA = re.compile(
    r'\d+\s*%\s+cash\s*back',
    re.I,
)


def _eh_lista_cupons(texto: str) -> bool:
    """
    Detecta se o post é uma LISTA DE CUPONS (não um cupom único).
    Critério: 2+ linhas no formato "R$ X OFF em R$ Y: CODIGO".
    """
    linhas = texto.strip().split("\n")
    linhas_lista = sum(
        1 for l in linhas
        if _RE_LINHA_CUPOM_LISTA.search(l)
    )
    return linhas_lista >= 2


def _eh_post_cupom(texto: str) -> bool:
    """
    Detecta se o post é 'tipo cupom' — onde o cupom é o ASSUNTO PRINCIPAL.

    Casos cobertos:
      a) palavra "cupom"/"cupons"/"código" no título
      b) título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
      c) 2+ linhas formato lista de cupons (delegada a _eh_lista_cupons)
      d) cashback nas primeiras linhas + código presente
      e) linha "X% Cashback ... : CODIGO" (mesmo sem palavra cupom)

    Casos (d) e (e) cobrem cards Shopee com título genérico ("Leo Indica
    / Ofertas Insanas") + linha "🎟️ 50% Cashback ... : BRUIANHEZ10".
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]

    # Caso (a): palavra "cupom" no título
    if _RE_TITULO_CUPOM.search(titulo):
        return True

    # Caso (b): título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
    if _RE_TITULO_OFF_COD.search(titulo):
        return True

    # Caso (c): 2+ linhas formato lista (reusa _eh_lista_cupons)
    if _eh_lista_cupons(texto):
        return True

    # Caso (d): cashback presente nas primeiras 5 linhas + cupom
    # mencionado no texto inteiro
    primeiras = "\n".join(linhas[:5])
    if (_RE_CASHBACK_LINHA.search(primeiras)
            and _KW_CUPOM.search(texto)):
        return True

    # Caso (e): linha "X% Cashback ... : CODIGO" no formato KV
    for linha in linhas[:6]:
        if (_RE_CASHBACK_LINHA.search(linha)
                and re.search(r':\s*[A-Z0-9][A-Z0-9_-]{3,19}\b', linha)):
            return True

    return False


def _eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """
    Detecta se o post é especificamente sobre cashback (sem cupom code).
    Combina o vocabulário genérico de cashback no título (universal)
    com o sinal derivado tem_sinal_cashback — vocabulário específico
    de plataforma, declarado pela própria plataforma e composto pelo
    registry. A dedup não conhece mais termos de marca diretamente.
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    if _RE_TITULO_CASHBACK.search(titulo):
        return True
    return tem_sinal_cashback


def _eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """
    Detecta evento/campanha/roleta (sem ASIN, sem cupom claro).

    CONSUMO DE IDENTIDADE DERIVADA:
      Esta função NÃO reinterpreta o mapa de URLs. A identidade de
      campanha é derivada pela normalização — autoridade única dessa
      derivação — sobre as URLs afiliadas LONGAS, antes do
      encurtamento terminal. A deduplicação apenas consome o campo
      derivado tem_host_campanha.

    A detecção combina dois sinais independentes:
      - vocabulário de evento no texto, em duas famílias nomeadas:
        a interativa canônica (_KW_EVENTO, dono: estado_evento) e o
        resíduo local de calendário comercial — classificação de
        conteúdo, legítima nesta camada por operar sobre o texto;
      - presença de host de campanha: consumida do campo derivado.
    """
    if _KW_EVENTO.search(texto[:200]):
        return True
    if _RE_CALENDARIO_COMERCIAL.search(texto[:200]):
        return True
    return tem_host_campanha


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""

# ─────────────────────────────────────────────────────────────────
# API PÚBLICA — o único contrato exposto. As regex e os detectores
# internos (_RE_*, _eh_*) permanecem privados deste módulo.
# ─────────────────────────────────────────────────────────────────
def eh_lista_cupons(texto: str) -> bool:
    """O post é uma LISTA de cupons (2+ linhas 'R$X OFF em R$Y: COD')?"""
    return _eh_lista_cupons(texto)


def eh_post_cupom(texto: str) -> bool:
    """O assunto do post é um CUPOM (código promocional)?"""
    return _eh_post_cupom(texto)


def eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """O assunto do post é CASHBACK (sem cupom code)?"""
    return _eh_post_cashback(texto, tem_sinal_cashback)


def eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """O assunto do post é uma CAMPANHA/EVENTO?"""
    return _eh_post_evento(texto, tem_host_campanha)


def extrair_pct_cashback(texto: str) -> str:
    """Percentual de cashback nas 5 primeiras linhas ('30' de '30%')."""
    return _extrair_pct_cashback(texto)


def buscar_calendario_comercial(texto: str) -> Optional[re.Match]:
    """Match do vocabulário de calendário comercial (black friday /
    esquenta) nos 200 primeiros chars. Devolve o MATCH — o consumidor
    precisa da posição (.start()) para ordenar candidatos —, nunca a
    regex, que permanece interna a este módulo."""
    return _RE_CALENDARIO_COMERCIAL.search(texto[:200])
 
