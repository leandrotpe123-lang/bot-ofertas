"""Autoridade de admissão — decide se a mensagem entra no pipeline.

Responsabilidade ÚNICA: responder se uma mensagem deve existir no
pipeline. NÃO limpa texto, NÃO remove linhas, NÃO monta mensagem, NÃO
deriva identidade.

normalizacao.py consulta esta autoridade; a política de bloqueio vive
aqui e em nenhum outro lugar.

Reorganização arquitetural: as regras abaixo foram MOVIDAS sem
alteração de lógica — `deve_descartar`/`_RE_VETO_POST` vinham de
pipeline.filtros (módulo de limpeza, que não pode barrar post inteiro)
e `tem_contexto` vinha de pipeline.normalizacao (que orquestra e não
define política de bloqueio).
"""
from __future__ import annotations

import re
import unicodedata


def _sem_acento(t: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )


# ══════════════════════════════════════════════════════════════════
# VETO DE POST — mecânica de canal, não oferta
# ══════════════════════════════════════════════════════════════════
# Alguns posts não são ofertas: são mecânica do canal de origem
# (gerar link por bot, mandar produto no chat) ou benefício restrito
# a quem é membro daquele grupo. Republicá-los polui o destino e
# entrega algo que o leitor não consegue usar.
#
# As expressões são DELIBERADAMENTE estreitas: exigem a locução
# inteira, não palavras soltas. "exclusivo" sozinho é comum em
# oferta legítima ("Cupons exclusivos Shopee VIP") e NÃO veta;
# apenas "exclusivo do grupo" veta. Mesmo critério para "gere o
# link" e "mande no chat", que descrevem a mecânica do canal.
#
# Na dúvida, publica: só veta com a locução completa presente.

_RE_VETO_POST = re.compile(
    r'exclusiv[oa]s?\s+d[oe]\s+(?:grupo|canal)'
    r'|d[oe]\s+(?:grupo|canal)\s+exclusiv'
    r'|ger(?:e|ar|a)\s+(?:o\s+|seu\s+|um\s+)?link'
    r'|man(?:de|da|dar)\s+(?:o\s+)?(?:produto\s+)?no\s+chat'
    r'|envi(?:e|ar)\s+(?:o\s+)?(?:produto\s+)?no\s+chat',
    re.I,
)


def deve_descartar(texto: str) -> str:
    """Motivo do veto do post inteiro, ou cadeia vazia se publicável."""
    m = _RE_VETO_POST.search(_sem_acento(texto or ""))
    return m.group(0).strip() if m else ""


# ─────────────────────────────────────────────────────────────────
# VIABILIDADE DO TEXTO
# ─────────────────────────────────────────────────────────────────
def tem_contexto(texto: str, midia_visual: bool = False) -> bool:
    """
    Verifica se a mensagem possui conteúdo promocional relevante o
    suficiente para prosseguir.

    Com MÍDIA VISUAL (foto, vídeo ou imagem — ingestao.tipo_midia_visual;
    PDF, áudio, voz, figurinha e a prévia de link NÃO contam) o conteúdo
    está na imagem, que não é lida (sem OCR): a legenda curta não tira o
    contexto. Medido nas prévias públicas (8.024 mensagens com link de
    loja, 6 fontes, 26/10→03/11): dos 7 posts que este filtro barrava, os
    6 com foto eram todos promoção — "Mais cupons Amazon"
    (Promotom 113100), "🔥 Nova Roleta Shopee" (Fada dos Cupons 17839, o
    mesmo post que passava como "🔥 Nova Chance Roleta Shopee"), "👀" (18222,
    a mesma Roleta Shopee do post seguinte), "Libera 17h"/"Libera 18h"
    (Promotom 112082/112017, cupom ML), foto + link (Promotom 112322, o link
    do "Cupons Shopee — Resgate aqui"). O sétimo, só um link sem foto,
    continua barrado. Legenda vazia, ou sem link de loja — inclusive só
    link do Telegram ("Pocando" → t.me/fumotom, Promotom 112021) —, é
    descartada adiante (normalizacao), com ou sem mídia.
    """
    if midia_visual:
        return True
    linhas = [
        l.strip() for l in texto.splitlines()
        if l.strip() and not re.match(r'https?://', l.strip())
    ]
    if not linhas:
        return False
    total = " ".join(linhas)
    # Singular E plural: "Mais cupons Amazon" (Promotom 113100, 03/11) era
    # barrado — `cupom` não casa com "cupons", nem `promoção` com
    # "promoções". `roleta`: campanha promocional (Roleta Shopee) — sem ela,
    # "Nova Roleta Shopee" (18 caracteres) caía e "Nova Chance Roleta Shopee"
    # (25) passava.
    indicadores = [
        r'off', r'%', r'r\$', r'cupo(?:m|ns)', r'desconto', r'promo(?:ção|ções)', r'oferta',
        r'roleta',
        r'grátis', r'evento', r'live', r'relâmpago', r'flash', r'volta',
        r'normalizou', r'a\s+partir', r'ativo', r'disponivel', r'pix',
        r'voltando', r'reativado', r'jogos?\s+gr[aá]tis',
    ]
    for ind in indicadores:
        if re.search(ind, total, re.I):
            return True
    return len(total) > 20

