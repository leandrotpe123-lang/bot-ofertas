# pipeline/memoria_cupom.py — MEMÓRIA DE CÓDIGOS DE CUPOM
#
# Responsabilidade ÚNICA: a memória operacional de códigos reais.
# Responde a UMA pergunta: "este código já foi visto (plataforma+código)
# dentro da janela?" — e, em caso afirmativo, devolve a identidade da
# corrente à qual ele já pertence (grudação transitiva).
#
# ESTA CAMADA É IMPURA POR NATUREZA — e é justamente por isso que ela é
# isolada aqui. Ela lê e escreve no banco (cupom_idx) e produz a contagem
# de códigos INÉDITOS, que alimenta a decisão de evolução por cupom.
#
# NÃO faz:
#   - classificação do assunto do post          (pipeline.assunto)
#   - derivação de âncoras / família            (identidade_oferta)
#   - decisão de duplicidade / evolução         (deduplicacao / decisao)
#
# FRONTEIRA IMPORTANTE (C3.1): o índice guarda apenas CÓDIGOS REAIS.
# Cupom sem código é identificado por benefício (plat|cupb|<desc>) e NUNCA
# entra aqui — garantido por construção, pois esta camada só opera sobre
# norm.cupons, que é vazio nesse caso.
#
# C4.2 (refactor puro): extraído byte-a-byte de identidade_oferta, que com
# isso volta a ser uma camada PURA de derivação. O comportamento é
# idêntico; o alinhamento da janela ao ciclo de 25 min é o C4.3.
from __future__ import annotations

import config
from database import (
    db_cupom_idx_buscar,
    db_cupom_idx_registrar,
)

# Kill-switch da memória de códigos. Desligar faz a identidade cair no
# fallback puro (sem grudação) — útil para isolar o efeito em diagnóstico.
CUPOM_IDX_ON = True


def resolver_identidade(norm, plat: str, fallback: str) -> str:
    """Resolve a identidade de cupom pelo ÍNDICE POR CÓDIGO.

    Se QUALQUER código do post já foi visto (plat + código) dentro da
    janela de cupom, reusa a identidade daquele post. Senão usa
    `fallback` (identidade nova). Em ambos os casos registra TODOS os
    códigos sob a identidade resolvida — preserva todos, não só um. O
    link nunca participa: a chave é código + plataforma.

    EFEITO COLATERAL (deliberado, e a razão de este módulo existir):
    muta `norm._cupom_novos` com a contagem de códigos INÉDITOS — insumo
    do ramo CUPOM_ENRIQUECIDO da evolução. O valor é congelado em
    MensagemEnriquecida.cupons_novos logo após, pelo enriquecimento.
    """
    if not CUPOM_IDX_ON:
        return fallback

    codes = sorted(set(
        c.upper() for c in norm.cupons if c
    ))
    if not codes:
        return fallback

    janela = float(config._JANELA_CUPOM_S)
    existente = db_cupom_idx_buscar(plat, codes, janela)
    identity = existente or fallback
    # max() é guarda DEFENSIVA. Desde o D1 o efeito roda 1x por mensagem
    # nova (identidade_canonica, via enriquecimento). Se algum caminho
    # reexecutar, registrar devolve 0 para códigos já indexados — o max
    # preserva a contagem real. (Fase 2: o valor é congelado em
    # MensagemEnriquecida.cupons_novos APÓS o efeito.)
    norm._cupom_novos = max(
        getattr(norm, "_cupom_novos", 0),
        db_cupom_idx_registrar(plat, codes, identity))
    return identity
  
