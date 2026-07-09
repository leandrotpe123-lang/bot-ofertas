"""
Autoridade de REATIVAÇÃO de oferta (R1 — mensagem nova de retorno).

Responsabilidade ÚNICA e pergunta de negócio:
    "Este texto sinaliza que a oferta VOLTOU (reativação)?"

Puro: sem estado, sem I/O, sem banco, sem Telegram. CONSOME o vocabulário
canônico de retorno (dono: pipeline.estado_evento._RE_RETORNO) — não
declara vocabulário próprio. NÃO decide se o ciclo está vivo (dono:
vida_oferta), NÃO executa o renascimento (dono: publicação), NÃO detecta
reply social (isso é R2 — responsabilidade do shadow_reply, fora daqui).

Consumidor: pipeline.decisao (ramo RENASCER).
"""
from __future__ import annotations

from pipeline.estado_evento import _RE_RETORNO

# Escopo de busca do sinal: início da mensagem, onde o anúncio de retorno
# aparece. Espelha a convenção já usada pela dedup (texto[:300]).
_ESCOPO_SINAL = 300


def eh_reativacao(texto: str) -> bool:
    """True se o texto sinaliza retorno de oferta ('voltou', 'reativado',
    'restock'...). Consulta o vocabulário canônico; decisão de frequência
    e de ciclo pertence aos consumidores."""
    if not texto:
        return False
    return bool(_RE_RETORNO.search(texto[:_ESCOPO_SINAL]))
