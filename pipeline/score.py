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
  origem (config._GRUPOS_IMG_RUIM / _SCORE_MIDIA_*). Não conhece detalhes
  internos de nenhuma plataforma.

O QUE NÃO FAZ:
  - decisão de evolução (evoluir/ignorar/publicar)  → pipeline.decisao
  - deduplicação / janela / claim                   → pipeline.deduplicacao
  - publicação / edição / substituição              → pipeline.publicacao
  - derivação/classificação de identidade           → pipeline.identidade_oferta

NOTAS (extração — Front 2, passo 2.1):
  Comportamento BYTE-IDÊNTICO ao que existia em deduplicacao.calcular_score.
  Dois pontos são reconhecidos como herança, a tratar em passos próprios,
  NÃO aqui:
    - o predicado de mídia (username_de(...) in _GRUPOS_IMG_RUIM) segue
      inline; a unificação com decisao._midia_ruim é o passo 2.2;
    - a contagem de cupons re-deriva via extrair_todos_cupons; consumir
      n_cupons já produzido pela identidade é evolução posterior.
"""
from __future__ import annotations

import re

import config
from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade import username_de
from utils.cupom import extrair_todos_cupons


# ── Bônus de QUANTIDADE (riqueza) no score — D3 ──────────────────
# Aditivos sobre a presença; recompensam ofertas ALÉM da primeira,
# com teto para a quantidade não dominar a qualidade. Tunáveis.
_SCORE_POR_LINK_EXTRA  = 2
_SCORE_POR_CUPOM_EXTRA = 2
_MAX_EXTRAS_CONTADOS   = 3


def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula score de qualidade do post.
    Mídia tem peso configurável: grupos em config._GRUPOS_IMG_RUIM
    recebem peso reduzido. A PRESENÇA de links/cupom mantém o peso
    histórico; a QUANTIDADE (ofertas além da primeira) soma por cima,
    com teto — é o que faz o post mais rico vencer pelo score (D3).
    """
    texto = norm.texto_limpo
    score = 0

    if norm.mapa:
        score += 3
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):
        score += 2
    if norm.cupom:
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
    n_links_extra = max(0, len(norm.mapa) - 1)
    if n_links_extra:
        score += min(n_links_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_LINK_EXTRA
    n_cupons = len(extrair_todos_cupons(texto, getattr(norm, "code_entities", None)))
    n_cupons_extra = max(0, n_cupons - 1)
    if n_cupons_extra:
        score += min(n_cupons_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_CUPOM_EXTRA

    # Mídia: peso varia conforme grupo de origem. A identidade é numérica;
    # resolvemos o @username via identidade p/ consultar a lista legível.
    if norm.tem_midia:
        if username_de(norm.chat) in config._GRUPOS_IMG_RUIM:
            score += config._SCORE_MIDIA_RUIM
        else:
            score += config._SCORE_MIDIA_NORMAL

    return score
      
