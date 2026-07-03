"""
Camada — Vocabulários canônicos de evento e retorno.

Responsabilidade única: ser a FONTE ÚNICA de dois vocabulários do
domínio, expostos como expressões regulares compiladas:
  - _KW_EVENTO  : família INTERATIVA de evento (quiz, roleta...);
  - _RE_RETORNO : linguagem de RETORNO de oferta ("voltou"...).

Módulo PURO: sem I/O, sem estado, sem decisão. As DECISÕES que
consomem estes vocabulários moram nos consumidores:
  - _KW_EVENTO  → identidade_oferta (classificação e chave de
                  campanha), montagem (título de evento),
                  publicacao (evento interativo fura saturação);
  - _RE_RETORNO → deduplicacao (_eh_reativacao — gate anti-flood).

NÃO faz:
  - decisão temporal sobre ofertas (responsabilidade da
    deduplicação: janela por tipo, reativação e TTL do banco)
  - deduplicação, score, normalização ou persistência
"""
import re

# ── Vocabulário de evento e de reativação ─────────────────────────
# _KW_EVENTO é a definição canônica da família INTERATIVA de evento:
# dinâmicas de participação (quiz, roleta, missão, sorteio...).
# Vocabulário de CALENDÁRIO COMERCIAL (black friday, esquenta) NÃO
# pertence a esta família e não deve entrar aqui: no canônico, ele
# alteraria a saturação (publicacao) e os títulos (montagem) no pico
# comercial. Esse vocabulário vive como resíduo nomeado na
# identidade de oferta (_RE_CALENDARIO_COMERCIAL).
_KW_EVENTO = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|'
    r'jogue|desafio|sorteio)\b',
    re.I,
)

# _RE_RETORNO é a definição canônica da linguagem de RETORNO de
# oferta ("voltou", "reativado", "restock"...). Fonte única do
# VOCABULÁRIO — a DECISÃO que o consome permanece separada e mora
# no consumidor:
#   - gate anti-flood (deduplicacao._eh_reativacao): janela curta
#     sobre o texto, com ou sem histórico.
# Compostos com "voltou" (ex.: "voltou ao estoque") são subsumidos
# por \bvoltou\b e não se re-declaram.
_RE_RETORNO = re.compile(
    r'\b(?:voltou|voltando|reativad[oa]|reativa[çc][aã]o|'
    r'ativ[oa]\s+novamente|dispon[ií]vel\s+novamente|de\s+volta|'
    r'normalizou|relan[çc]amento|restock)\b',
    re.I,
)
