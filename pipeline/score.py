"""
Camada — Score de qualidade da oferta.

Responsabilidade ÚNICA: QUANTIFICAR a qualidade de um post — devolver um
inteiro que mede quão boa/rica é a oferta. É a autoridade de CÁLCULO de
score; não decide o que fazer com ele.

Superfície pública (estável):
  - calcular_score(norm) -> int

O QUE CONSOME:
  Campos já derivados pela normalização (texto_limpo, mapa, cupom, sku,
  tem_midia, chat, code_entities) e a política de mídia por grupo de
  origem (config._GRUPOS_IMG_RUIM), hoje só para a política de mídia e
  para a projeção de escala v1. Não conhece detalhes
  internos de nenhuma plataforma.

O QUE NÃO FAZ:
  - decisão de evolução (evoluir/ignorar/publicar)  → pipeline.decisao
  - deduplicação / janela / claim                   → pipeline.deduplicacao
  - publicação / edição / substituição              → pipeline.publicacao
  - derivação/classificação de identidade           → pipeline.identidade_oferta

NOTAS (histórico de extração):
  Comportamento BYTE-IDÊNTICO ao que existia em deduplicacao.calcular_score.
  A quantificação foi extraída em 2.1; o predicado de mídia foi unificado
  em midia_ruim (2.2); a contagem de cupons passou a consumir norm.cupons
  em vez de re-derivar (Cupom-1). Aqui o score é consumidor puro de norm.
"""
from __future__ import annotations

import re

import config
from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade import username_de


# ── Bônus de QUANTIDADE (riqueza) no score — D3 ──────────────────
# Aditivos sobre a presença; recompensam ofertas ALÉM da primeira,
# com teto para a quantidade não dominar a qualidade. Tunáveis.
_SCORE_POR_LINK_EXTRA  = 2
_SCORE_POR_CUPOM_EXTRA = 2
_MAX_EXTRAS_CONTADOS   = 3


# ── RÉGUA DE RIQUEZA POR MECÂNICA ─────────────────────────────────
# Riqueza não é a mesma coisa para todas as mecânicas.
#
# PRODUTO — links extras são riqueza REAL: variações do mesmo item,
#   outra loja, outro preço, opção esgotada. O post com mais links dá
#   mais caminhos ao usuário. A régua de produto é a HISTÓRICA e
#   permanece CONGELADA (_SCORE_POR_LINK_EXTRA, _SCORE_POR_CUPOM_EXTRA,
#   _MAX_EXTRAS_CONTADOS). Medido: score idêntico em 94 posts.
#
# CUPOM — links extras NÃO são riqueza. Um post que cola 4 links de
#   resgate não descreve mais oferta que um que diz "R$30 OFF em R$299
#   e R$90 OFF em R$899". A riqueza é QUANTAS OFERTAS o post descreve.
#
# UMA oferta, UMA contagem. Código e campanha são duas FORMAS de
# enxergar a mesma oferta, não duas ofertas:
#     "10% OFF acima de R$79, limite R$100: QUERODESCONTO"
#      └────────── campanha ──────────┘  └── código ──┘
# Somar as duas premiava a mesma oferta duas vezes — medido em 70 dos
# 92 posts de cupom do corpus, onde nº códigos == nº campanhas. Por
# isso `max`, nunca soma: cobre o post que só traz código
# ("🎟 MERCADOSALDAO303"), o que só traz campanha ("R$30 OFF em R$299")
# e o que traz as duas, sem contar nada em dobro.
#
# Calibrado em 92 posts reais, 1.664 pares (rico, pobre) ordenados por
# evidência estrutural. Inversões — post pobre vencendo o rico:
#     régua única de hoje ......................... 19
#     peso 2 teto 5 ................................ 3
#     peso 3 teto 4 ................................ 1
#     peso 3 teto 5 ................................ 0   ← escolhido
#     peso 4 teto 5 ................................ 0   (mesmo resultado,
#                                                        peso menor é
#                                                        mais conservador)
_SCORE_POR_OFERTA    = 3
_MAX_OFERTAS_CONTADAS = 5

# Campanha COMPLETA: benefício + condição explícitos na MESMA linha.
#   "R$30 OFF em R$299"  ·  "20% OFF acima de R$79"
#   "40% limitado a R$15"  ·  "30% Cashback (limite R$30)"
# Linha a linha, de propósito: o benefício de uma linha nunca casa com
# a condição de outra.
_RE_CAMPANHA = re.compile(
    r'(?:R\$\s?\d[\d.]*|\d{1,3}\s?%)'
    r'(?:\s*(?:de\s+)?(?:OFF|desconto|cashback)?[\s,(]*'
    r'(?:em|acima\s+de|a\s+partir\s+de|nas?\s+compras?\s+de|'
    r'em\s+compras\s+de|para\s+compras\s+de|'
    r'limitad[oa]\s+a|limite(?:\s+de)?|min(?:imo)?\.?\s*(?:de)?)\s*)'
    r'R\$\s?\d[\d.]*',
    re.I,
)
# Escopo do cabeçalho da oferta — mesmo limite usado pelo assunto.
_ESCOPO_CAMPANHA = 600


def _n_ofertas(norm: MensagemNormalizada) -> int:
    """Quantas ofertas distintas o post descreve.

    `max` e não soma: código e campanha são duas formas de enxergar a
    MESMA oferta. Ver a doutrina acima.
    """
    n_campanhas = sum(
        len(_RE_CAMPANHA.findall(l))
        for l in norm.texto_limpo[:_ESCOPO_CAMPANHA].split("\n"))
    return max(len(norm.cupons), n_campanhas)


def midia_ruim(chat: str) -> bool:
    """True se o grupo de origem tem mídia de baixa qualidade
    (config._GRUPOS_IMG_RUIM). Ponto único da regra — consumido por
    calcular_score (peso de mídia) e por decisao.decidir (troca de imagem)."""
    return username_de(chat) in config._GRUPOS_IMG_RUIM


def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula o score de CONTEÚDO do post — escala v2.

    Mede exclusivamente riqueza da oferta: link, preço, cupom, %off,
    R$off, piso, frete grátis e SKU. A PRESENÇA de links/cupom mantém o
    peso histórico; a QUANTIDADE (ofertas além da primeira) soma por
    cima, com teto — é o que faz o post mais rico vencer (D3).

    NÃO participam do score: origem/grupo, qualidade da mídia, emoji,
    formatação ou aparência. O vencedor do TEXTO é decidido só por
    conteúdo; a IMAGEM é decidida em paralelo por
    pipeline.midia_politica. As duas fontes podem ser grupos distintos.
    """
    texto = norm.texto_limpo
    score = 0

    if norm.mapa:
        score += 3
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):
        score += 2
    if norm.cupons:
        score += 2
    if re.search(r'\d+\s*%\s*off', texto, re.I):
        score += 2
    if re.search(r'r\$\s*[\d.,]+\s*off', texto, re.I):
        score += 2
    if re.search(r'(acima|mínimo|min)\s+de\s+r\$', texto, re.I):
        score += 1
    if re.search(r'frete\s+gr[aá]t', texto, re.I):
        score += 1
    if norm.sku:
        score += 1

    # ── Quantidade (riqueza): ofertas ALÉM da primeira somam, com teto.
    #    A presença acima fica intacta → posts de 1 link/1 cupom NÃO mudam.
    #
    # A régua da QUANTIDADE é POR MECÂNICA. `ids_globais` é a evidência
    # de produto JÁ DERIVADA por normalizacao_identidade.derivar_produto
    # — a mesma que identidade_oferta consome como `tem_produto`. O
    # score apenas pergunta; não classifica produto por conta própria.
    if norm.ids_globais:
        n_links_extra = max(0, len(norm.mapa) - 1)
        if n_links_extra:
            score += min(n_links_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_LINK_EXTRA
        n_cupons_extra = max(0, len(norm.cupons) - 1)
        if n_cupons_extra:
            score += min(n_cupons_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_CUPOM_EXTRA
    else:
        n_ofertas_extra = max(0, _n_ofertas(norm) - 1)
        if n_ofertas_extra:
            score += (min(n_ofertas_extra, _MAX_OFERTAS_CONTADAS)
                      * _SCORE_POR_OFERTA)

    # [FASE 4] O peso de mídia FOI REMOVIDO daqui. Ele fazia a
    # qualidade/origem da imagem decidir o vencedor do TEXTO: um grupo
    # de midia boa vencia com ate 2 pontos de conteudo A MENOS
    # (_SCORE_MIDIA_NORMAL 3 - _SCORE_MIDIA_RUIM 1). A imagem e decidida
    # por pipeline.midia_politica; o score representa SOMENTE conteudo.

    return score


# ── Escala de score — transicao v1 → v2 ───────────────────────────
# v1 (legado): score = conteudo + peso de midia (comportamento antigo)
# v2 (atual):  score = conteudo puro
#
# Posts anteriores ao deploy tem score na escala v1 e NAO sao
# decomponiveis: post_estado nao registra se o post tem midia, e `lider`
# diverge do dono da imagem no caminho "evolucao textual sem imagem".
# Por isso NAO reconstruimos o legado — projetamos o CANDIDATO na escala
# do post. O candidato e o unico lado de que sabemos tudo (chat,
# tem_midia), entao a projecao e exata, sem inferencia.
V_LEGADO = 1
V_CONTEUDO = 2


def _peso_midia(chat: str, tem_midia: bool) -> int:
    """Peso que a escala v1 dava a midia. Codigo de TRANSICAO: some
    quando nao houver mais post v1 vivo (<=_VIDA_OFERTA_S apos o deploy)."""
    if not tem_midia:
        return 0
    return (config._SCORE_MIDIA_RUIM if midia_ruim(chat)
            else config._SCORE_MIDIA_NORMAL)


def score_na_escala(score_conteudo: int, chat: str, tem_midia: bool,
                    versao: int) -> int:
    """Projeta o score do CANDIDATO na escala do POST comparado.

    Nunca decompoe o score do post. Garante que toda comparacao
    acontece dentro de uma unica escala."""
    if versao == V_LEGADO:
        return score_conteudo + _peso_midia(chat, tem_midia)
    return score_conteudo
      
