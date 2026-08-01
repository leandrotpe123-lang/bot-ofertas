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
# isso volta a ser uma camada PURA de derivação.
#
# C4.3 (janela): a memória vive EXATAMENTE o ciclo. A janela deriva de
# vida_oferta.VIDA_OFERTA_S — a autoridade única do tempo (Frente 0) —
# em vez de ter relógio próprio. Antes eram 30 min contra 25 do ciclo:
# um código de ciclo JÁ MORTO ainda sequestrava a identidade de um post
# novo (histórico governando o presente). Agora a memória expira junto
# com o ciclo que a criou.
#
# São perguntas de negócio diferentes — "este código já foi visto?" versus
# "esta identidade já foi reivindicada?" — e não se unificam só porque
# ambas usam tempo.
from __future__ import annotations

from database import (
    db_cupom_idx_buscar,
    db_cupom_idx_registrar,
)
from pipeline.vida_oferta import VIDA_OFERTA_S
# Kill-switch da memória de códigos. Desligar faz a identidade cair no
# fallback puro (sem grudação) — útil para isolar o efeito em diagnóstico.
CUPOM_IDX_ON = True

def buscar_identidade(norm, plat: str, fallback: str) -> str:
    """CONSULTA PURA do índice por código — sem escrita, sem mutação.

    Se qualquer código do post já foi visto (plat + código) dentro da
    janela, devolve a identidade da corrente à qual ele pertence.
    Senão devolve `fallback`. Pode ser chamada N vezes com o mesmo
    resultado: é o que permite a derivação ser pura (F1e).
    """
    if not CUPOM_IDX_ON:
        return fallback
    codes = sorted(set(c.upper() for c in norm.cupons if c))
    if not codes:
        return fallback
    return db_cupom_idx_buscar(plat, codes, float(VIDA_OFERTA_S)) or fallback


def registrar_uso(norm, plat: str, identity: str) -> int:
    """EFEITO — registra TODOS os códigos do post sob `identity` e
    devolve quantos eram INÉDITOS na janela.

    Roda 1x por mensagem, só no caminho NOVO (P9), chamada
    explicitamente por enriquecimento.enriquecer DEPOIS da derivação.
    Muta `norm._cupom_novos` como ponte legada para decisao/publicacao.
    """
    if not CUPOM_IDX_ON:
        return 0
    codes = sorted(set(c.upper() for c in norm.cupons if c))
    if not codes:
        return 0
    novos = db_cupom_idx_registrar(plat, codes, identity, float(VIDA_OFERTA_S))
    norm._cupom_novos = max(getattr(norm, "_cupom_novos", 0), novos)
    return novos
